"""
dev_screener.py — 链上 Dev 画像，A/B/C 分级
"""

import asyncio
import logging
import time
from collections import OrderedDict

from web3 import Web3
from rpc_fleet import RpcFleet

logger = logging.getLogger("DevScreener")

CREATE_TOPIC0 = "0x396d5e902b675b032348d3d2e9517ee8f0c4a926603fbc075d3d282ff00cad20"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

DEFAULT_DEV_SCREEN_CONFIG = {
    "DEV_MIN_NONCE": 3,
    "DEV_MIN_BALANCE_BNB": 0.05,
    "DEV_SCREEN_TIMEOUT": 30.0,
    "DEV_SCREEN_ENABLED": True,

    # 时间窗（BSC ~ 3s/block）
    "DEV_PROFILE_1H_BLOCKS": 1200,
    "DEV_PROFILE_6H_BLOCKS": 7200,
    "DEV_PROFILE_12H_BLOCKS": 14400,

    # 分段预算（不会硬等满）
    "DEV_FAST_TIMEOUT": 2.0,
    "DEV_HEAD_TIMEOUT": 2.5,
    "DEV_QUERY_1H_TIMEOUT": 6.0,
    "DEV_QUERY_6H_TIMEOUT": 9.0,
    "DEV_QUERY_12H_TIMEOUT": 12.0,

    # A级高门槛
    "DEV_A_MIN_NONCE": 50,
    "DEV_A_MIN_BALANCE_BNB": 0.5,
    "DEV_A_MAX_DEPLOYS_1H": 0,
    "DEV_A_MAX_DEPLOYS_6H": 1,
    "DEV_A_MAX_DEPLOYS_12H": 2,

    # C级直接踹
    "DEV_C_MAX_DEPLOYS_1H": 2,
    "DEV_C_MAX_DEPLOYS_6H": 4,
    "DEV_C_MAX_DEPLOYS_12H": 8,
}


class DevScreener:
    def __init__(self, fleet: RpcFleet, risk_guard, config: dict = None):
        self.fleet = fleet
        self.risk = risk_guard
        self.cfg = dict(DEFAULT_DEV_SCREEN_CONFIG)
        if config:
            self.cfg.update(config)

        self._cache = OrderedDict()  # addr -> (ts, assessment, ttl)
        self._cache_ttl = 300
        self._degraded_cache_ttl = 90
        self._head_cache = None  # (ts, block)

        self.total_screened = 0
        self.total_cache_hits = 0
        self.total_grade_a = 0
        self.total_grade_b = 0
        self.total_grade_c = 0
        self.total_degraded = 0
        self.total_blacklist = 0
        self.total_glove = 0
        self.total_factory = 0

        logger.info("🔍 [DevScreener] 初始化完成 (A/B/C 分级, v2.1)")
        logger.info(f"🔍 [DevScreener] 预算: {self.cfg['DEV_SCREEN_TIMEOUT']}s | 窗口: 1h/{self.cfg['DEV_PROFILE_1H_BLOCKS']} 6h/{self.cfg['DEV_PROFILE_6H_BLOCKS']} 12h/{self.cfg['DEV_PROFILE_12H_BLOCKS']}")
        logger.info(f"🔍 [DevScreener] A级门槛: nonce≥{self.cfg['DEV_A_MIN_NONCE']} bal≥{self.cfg['DEV_A_MIN_BALANCE_BNB']}BNB")

    def _bump_stats(self, grade: str):
        if grade == "A":
            self.total_grade_a += 1
        elif grade == "B":
            self.total_grade_b += 1
        elif grade == "C":
            self.total_grade_c += 1

    def _cache_get(self, dev_addr: str):
        item = self._cache.get(dev_addr)
        if not item:
            return None
        ts, assessment, ttl = item
        if time.time() - ts < ttl:
            self.total_cache_hits += 1
            self._bump_stats(assessment["grade"])
            out = dict(assessment)
            out["reason"] = "[缓存] " + out["reason"]
            return out
        self._cache.pop(dev_addr, None)
        return None

    def _cache_put(self, dev_addr: str, assessment: dict, ttl: float = None):
        self._cache[dev_addr] = (time.time(), assessment, ttl or self._cache_ttl)
        if len(self._cache) > 5000:
            self._cache.popitem(last=False)

    def _make_assessment(self, grade: str, reason: str, summary: str, details: dict, degraded: bool = False):
        return {
            "grade": grade,
            "decision": {"A": "direct_buy", "B": "monitor", "C": "block"}[grade],
            "reason": reason,
            "summary": summary,
            "details": details,
            "degraded": degraded,
        }

    async def assess(self, creator_addr: str) -> dict:
        if not self.cfg.get("DEV_SCREEN_ENABLED", True):
            assessment = self._make_assessment("B", "审查已关闭", "Dev审查关闭 → 进入动量", {}, degraded=True)
            self._bump_stats("B")
            return assessment

        if not creator_addr or creator_addr == ZERO_ADDRESS:
            assessment = self._make_assessment("B", "无 creator 地址", "无 Dev 地址 → 进入动量", {}, degraded=True)
            self._bump_stats("B")
            return assessment

        self.total_screened += 1
        dev_addr = Web3.to_checksum_address(creator_addr)

        cached = self._cache_get(dev_addr)
        if cached:
            return cached

        if self.risk and self._is_dev_blacklisted(dev_addr):
            self.total_blacklist += 1
            assessment = self._make_assessment(
                "C",
                "Dev 地址在黑名单中",
                "黑名单命中 → 直接拉黑拦截",
                {"dev": dev_addr},
            )
            self._bump_stats("C")
            self._cache_put(dev_addr, assessment)
            return assessment

        total_budget = max(8.0, float(self.cfg.get("DEV_SCREEN_TIMEOUT", 30.0)))
        started = time.time()
        fast = await self._fast_profile(dev_addr, timeout=min(self.cfg["DEV_FAST_TIMEOUT"], total_budget * 0.2))

        # 手套号可以直接判 C，不必继续等长查询
        if fast.get("glove"):
            details = {
                "dev": dev_addr,
                "nonce": fast.get("nonce"),
                "balance_bnb": fast.get("balance_bnb"),
                "deploy_1h": None,
                "deploy_6h": None,
                "deploy_12h": None,
                "degrade_reason": fast.get("degrade_reason"),
            }
            assessment = self._make_assessment("C", "一次性手套号", "nonce 低 + 余额低 → 直接踹", details)
            self.total_glove += 1
            self._bump_stats("C")
            self._cache_put(dev_addr, assessment)
            return assessment

        elapsed = time.time() - started
        remain = max(2.0, total_budget - elapsed)
        deploy = await self._deploy_profile_staged(dev_addr, remain)

        assessment = self._grade(dev_addr, fast, deploy)
        self._bump_stats(assessment["grade"])
        ttl = self._degraded_cache_ttl if assessment.get("degraded") else self._cache_ttl
        self._cache_put(dev_addr, assessment, ttl=ttl)

        if assessment["grade"] == "C" and self.risk and "黑名单" not in assessment["reason"]:
            self.risk.add_dev_to_blacklist(dev_addr, assessment["reason"])
            self.total_factory += 1

        logger.info(f"🔍 [DevScreener] {dev_addr[:12]}... => {assessment['grade']} | {assessment['reason']}")
        return assessment

    async def check(self, creator_addr: str) -> tuple:
        assessment = await self.assess(creator_addr)
        return assessment["grade"] != "C", assessment["reason"]

    async def _fast_profile(self, dev_addr: str, timeout: float):
        nonce_task = asyncio.create_task(self.fleet.safe_call('get_transaction_count', dev_addr, 'latest', tiers=[1, 0]))
        balance_task = asyncio.create_task(self.fleet.safe_call('get_balance', dev_addr, tiers=[1, 0]))
        try:
            nonce, balance = await asyncio.wait_for(asyncio.gather(nonce_task, balance_task, return_exceptions=True), timeout=timeout)
        except asyncio.TimeoutError:
            for t in (nonce_task, balance_task):
                t.cancel()
            return {"degraded": True, "degrade_reason": f"快审超时({timeout:.1f}s)", "nonce": None, "balance_bnb": None, "glove": False}

        if isinstance(nonce, Exception):
            nonce = None
        if isinstance(balance, Exception):
            balance = None
        balance_bnb = float(Web3.from_wei(balance, 'ether')) if balance is not None else None

        glove = False
        if nonce is not None and balance_bnb is not None:
            glove = nonce <= self.cfg["DEV_MIN_NONCE"] and balance_bnb < self.cfg["DEV_MIN_BALANCE_BNB"]

        return {
            "degraded": nonce is None or balance_bnb is None,
            "degrade_reason": None if (nonce is not None and balance_bnb is not None) else "快审数据不完整",
            "nonce": nonce,
            "balance_bnb": balance_bnb,
            "glove": glove,
        }

    async def _deploy_profile_staged(self, dev_addr: str, timeout: float):
        out = {
            "degraded": False,
            "degrade_reason": None,
            "d1h": None,
            "d6h": None,
            "d12h": None,
            "current_block": None,
            "window_done": "none",
        }

        head_budget = min(self.cfg["DEV_HEAD_TIMEOUT"], max(1.5, timeout * 0.12))
        current_block = await self._get_current_head(head_budget)
        if current_block is None:
            out["degraded"] = True
            out["degrade_reason"] = f"block_number 超时/失败({head_budget:.1f}s)"
            return out

        out["current_block"] = current_block
        started = time.time()

        # 1h
        d1h, ok, why = await self._window_deploy_count(dev_addr, current_block, int(self.cfg["DEV_PROFILE_1H_BLOCKS"]), min(self.cfg["DEV_QUERY_1H_TIMEOUT"], max(2.0, timeout * 0.2)))
        out["d1h"] = d1h if ok else None
        if not ok:
            out["degraded"] = True
            out["degrade_reason"] = why
            return out
        out["window_done"] = "1h"
        if d1h >= self.cfg["DEV_C_MAX_DEPLOYS_1H"]:
            return out

        # 6h
        elapsed = time.time() - started
        remain = max(2.0, timeout - elapsed)
        d6h, ok, why = await self._window_deploy_count(dev_addr, current_block, int(self.cfg["DEV_PROFILE_6H_BLOCKS"]), min(self.cfg["DEV_QUERY_6H_TIMEOUT"], remain))
        out["d6h"] = d6h if ok else None
        if not ok:
            out["degraded"] = True
            out["degrade_reason"] = why
            return out
        out["window_done"] = "6h"
        if d6h >= self.cfg["DEV_C_MAX_DEPLOYS_6H"]:
            return out

        # 12h：只有可能给 A 或命中 12h 级别 C 时才有价值
        elapsed = time.time() - started
        remain = max(2.0, timeout - elapsed)
        likely_a_candidate = (
            d1h <= self.cfg["DEV_A_MAX_DEPLOYS_1H"] and
            d6h <= self.cfg["DEV_A_MAX_DEPLOYS_6H"]
        )
        need_12h_for_c = self.cfg["DEV_C_MAX_DEPLOYS_12H"] > self.cfg["DEV_C_MAX_DEPLOYS_6H"]
        if not likely_a_candidate and not need_12h_for_c:
            return out

        d12h, ok, why = await self._window_deploy_count(dev_addr, current_block, int(self.cfg["DEV_PROFILE_12H_BLOCKS"]), min(self.cfg["DEV_QUERY_12H_TIMEOUT"], remain))
        out["d12h"] = d12h if ok else None
        out["window_done"] = "12h" if ok else out["window_done"]
        if not ok:
            out["degraded"] = True
            out["degrade_reason"] = why
            return out
        return out

    async def _window_deploy_count(self, dev_addr: str, current_block: int, lookback_blocks: int, timeout: float):
        from_block = max(1, current_block - lookback_blocks)
        # Dev 画像专用：比默认 30 blocks 更大的 chunk，减少碎片 RPC 调度开销
        if lookback_blocks <= self.cfg["DEV_PROFILE_1H_BLOCKS"]:
            chunk_size = 300
            tiers = [0, 1]
            priority = "high"
        elif lookback_blocks <= self.cfg["DEV_PROFILE_6H_BLOCKS"]:
            chunk_size = 600
            tiers = [0, 1]
            priority = "high"
        else:
            chunk_size = 1200
            tiers = [0, 1]
            priority = "normal"

        logs_task = asyncio.create_task(self.fleet.safe_call_chunked_logs({
            "fromBlock": from_block,
            "toBlock": current_block,
            "topics": [CREATE_TOPIC0],
        }, chunk_size=chunk_size, tiers=tiers, priority=priority))
        try:
            logs = await asyncio.wait_for(logs_task, timeout=max(3.0, timeout))
        except asyncio.TimeoutError:
            logs_task.cancel()
            return None, False, f"{lookback_blocks}块窗口 get_logs 超时({max(3.0, timeout):.1f}s)"

        if logs is None:
            return None, False, f"{lookback_blocks}块窗口 get_logs 失败"

        count = 0
        for log in logs:
            if self._log_mentions_dev(dev_addr, log):
                count += 1
        return count, True, None

    async def _get_current_head(self, timeout: float):
        if self._head_cache:
            ts, bn = self._head_cache
            if time.time() - ts < 2.0:
                return bn
        head_task = asyncio.create_task(self.fleet.safe_call('block_number', tiers=[0, 1, 2]))
        try:
            current_block = await asyncio.wait_for(head_task, timeout=timeout)
        except asyncio.TimeoutError:
            head_task.cancel()
            return self._head_cache[1] if self._head_cache else None
        if current_block is not None:
            self._head_cache = (time.time(), current_block)
        return current_block

    def _log_mentions_dev(self, dev_addr: str, log: dict) -> bool:
        dev_lower = dev_addr.lower()
        topics = log.get('topics', [])
        data = log.get('data', b'')

        for t in topics[1:]:
            try:
                t_hex = t.hex() if hasattr(t, 'hex') else str(t)
                addr = "0x" + t_hex[-40:]
                if addr.lower() == dev_lower:
                    return True
            except Exception:
                pass

        try:
            d_hex = data.hex() if isinstance(data, (bytes, bytearray)) else str(data)
            if d_hex.startswith('0x'):
                d_hex = d_hex[2:]
            for i in range(0, len(d_hex), 64):
                chunk = d_hex[i:i + 64]
                if len(chunk) >= 40 and chunk[-40:].lower() == dev_lower[2:].lower():
                    return True
        except Exception:
            pass
        return False

    def _grade(self, dev_addr: str, fast: dict, deploy: dict) -> dict:
        d1h = deploy.get("d1h")
        d6h = deploy.get("d6h")
        d12h = deploy.get("d12h")
        degraded = fast.get("degraded") or deploy.get("degraded")

        details = {
            "dev": dev_addr,
            "nonce": fast.get("nonce"),
            "balance_bnb": fast.get("balance_bnb"),
            "deploy_1h": d1h,
            "deploy_6h": d6h,
            "deploy_12h": d12h,
            "window_done": deploy.get("window_done"),
            "degrade_reason": deploy.get("degrade_reason") or fast.get("degrade_reason"),
        }

        # 能明确判 C 就尽早判 C
        if d1h is not None and d1h >= self.cfg["DEV_C_MAX_DEPLOYS_1H"]:
            return self._make_assessment("C", f"疑似流水线 Dev: 1h={d1h}", "1h 高频发币 → 直接踹并加黑名单", details)
        if d6h is not None and d6h >= self.cfg["DEV_C_MAX_DEPLOYS_6H"]:
            return self._make_assessment("C", f"疑似流水线 Dev: 6h={d6h}", "6h 高频发币 → 直接踹并加黑名单", details)
        if d12h is not None and d12h >= self.cfg["DEV_C_MAX_DEPLOYS_12H"]:
            return self._make_assessment("C", f"疑似流水线 Dev: 12h={d12h}", "12h 高频发币 → 直接踹并加黑名单", details)

        nonce = fast.get("nonce")
        bal = fast.get("balance_bnb")
        a_ok = (
            not degraded and
            nonce is not None and nonce >= self.cfg["DEV_A_MIN_NONCE"] and
            bal is not None and bal >= self.cfg["DEV_A_MIN_BALANCE_BNB"] and
            d1h is not None and d1h <= self.cfg["DEV_A_MAX_DEPLOYS_1H"] and
            d6h is not None and d6h <= self.cfg["DEV_A_MAX_DEPLOYS_6H"] and
            d12h is not None and d12h <= self.cfg["DEV_A_MAX_DEPLOYS_12H"]
        )
        if a_ok:
            return self._make_assessment(
                "A",
                f"高置信优质 Dev: nonce={nonce} bal={bal:.3f} 1h/6h/12h={d1h}/{d6h}/{d12h}",
                "成熟地址 + 资金正常 + 非流水线 → 可跳过动量直买",
                details,
            )

        if degraded:
            self.total_degraded += 1
            return self._make_assessment(
                "B",
                f"画像部分降级: {details['degrade_reason']}",
                "第一关未查全，但未发现明确坏证据 → 进动量",
                details,
                degraded=True,
            )

        return self._make_assessment(
            "B",
            f"中性 Dev: nonce={nonce} bal={bal if bal is not None else 'N/A'} 1h/6h/12h={d1h}/{d6h}/{d12h}",
            "无明显风险，但证据不足以直接证明是优质 Dev → 进入动量",
            details,
        )

    def _is_dev_blacklisted(self, dev_addr: str) -> bool:
        if not self.risk:
            return False
        try:
            return self.risk.is_dev_blacklisted(dev_addr)
        except Exception:
            return False

    def update_config(self, **kwargs):
        updated = []
        for k, v in kwargs.items():
            if k in self.cfg and v is not None:
                old = self.cfg[k]
                self.cfg[k] = v
                updated.append(f"{k}: {old}→{v}")
        if updated:
            logger.info(f"⚙️ [DevScreener] 配置更新: {', '.join(updated)}")
        return updated

    def get_status_text(self) -> str:
        total = max(1, self.total_screened)
        return (
            f"🔍 <b>Dev 画像器 v2.1</b>\n"
            f"状态: {'🟢 启用' if self.cfg.get('DEV_SCREEN_ENABLED') else '🔴 关闭'}\n"
            f"审查: {self.total_screened} | A={self.total_grade_a} | B={self.total_grade_b} | C={self.total_grade_c}\n"
            f"黑名单={self.total_blacklist} | 手套={self.total_glove} | 流水线={self.total_factory}\n"
            f"降级={self.total_degraded} | 缓存命中={self.total_cache_hits} ({self.total_cache_hits/total*100:.0f}%)\n"
            f"━━━ 关键参数 ━━━\n"
            f"预算: {self.cfg['DEV_SCREEN_TIMEOUT']}s\n"
            f"分段: 快审{self.cfg['DEV_FAST_TIMEOUT']}s / 1h{self.cfg['DEV_QUERY_1H_TIMEOUT']}s / 6h{self.cfg['DEV_QUERY_6H_TIMEOUT']}s / 12h{self.cfg['DEV_QUERY_12H_TIMEOUT']}s\n"
            f"A级: nonce≥{self.cfg['DEV_A_MIN_NONCE']} bal≥{self.cfg['DEV_A_MIN_BALANCE_BNB']}BNB 1h≤{self.cfg['DEV_A_MAX_DEPLOYS_1H']} 6h≤{self.cfg['DEV_A_MAX_DEPLOYS_6H']} 12h≤{self.cfg['DEV_A_MAX_DEPLOYS_12H']}\n"
            f"C级: 1h≥{self.cfg['DEV_C_MAX_DEPLOYS_1H']} 或 6h≥{self.cfg['DEV_C_MAX_DEPLOYS_6H']} 或 12h≥{self.cfg['DEV_C_MAX_DEPLOYS_12H']}"
        )
