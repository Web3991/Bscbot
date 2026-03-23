"""
buyer_profiler.py — 多维买家评分，Cabal 检测
"""

import asyncio
import logging
import sqlite3
import time
import os
import contextlib
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from web3 import Web3
from rpc_fleet import RpcFleet

logger = logging.getLogger("BuyerProfiler")

# ================= 默认参数 =================
DEFAULT_PROFILER_CONFIG = {
    "PROFILER_ENABLED": True,           # 总开关
    "PROFILER_SAMPLE_SIZE": 5,          # 抽样买家数
    "PROFILER_TIMEOUT": 8.0,            # 总超时 (秒)
    "PROFILER_DANGER_THRESHOLD": 60,    # 单人危险阈值 (分)
    "PROFILER_CABAL_RATIO": 3,          # N/SAMPLE_SIZE 人危险 → 拉黑
    # 打分权重
    "SCORE_DISPOSABLE_WALLET": 30,      # 一次性钱包
    "SCORE_SNIPER_BOT": 25,             # 狙击机器人
    "SCORE_CROSS_LAUNCH": 35,           # 跨盘串联
    "SCORE_SAME_BLOCK": 20,             # 同块聚集
    "SCORE_NONCE_UNIFORMITY": 20,       # 群体 nonce 一致性
    # 阈值细节
    "DISPOSABLE_MAX_NONCE": 5,          # nonce ≤ 此值
    "DISPOSABLE_MAX_BALANCE_BNB": 0.05, # 余额 < 此值
    "SNIPER_BLOCK_WINDOW": 2,           # 创建后几个区块内算狙击
    "CROSS_LAUNCH_LOOKBACK_HOURS": 24,  # 跨盘回溯时长
    "CROSS_LAUNCH_MIN_TOKENS": 2,       # 出现在几个币以上算串联
    "SAME_BLOCK_MIN_COUNT": 3,          # 几人同块算聚集
    "NONCE_UNIFORMITY_MIN_COUNT": 4,    # 几人 nonce 一致算 Sybil
    "NONCE_UNIFORMITY_THRESHOLD": 5,    # nonce < 此值算"一致低"
}


class BuyerProfiler:
    """
    多维侧写雷达 v1.0

    用法:
        profiler = BuyerProfiler(fleet, risk_guard, db_path)
        passed, report = await profiler.profile(token_addr, buyers, buyer_blocks, creation_block)
        if not passed:
            # Cabal detected, blacklist!
    """

    def __init__(self, fleet: RpcFleet, risk_guard,
                 db_path: str = "buyer_history.db", config: dict = None,
                 legacy_db_path: str = None):
        self.fleet = fleet
        self.risk = risk_guard
        self.cfg = dict(DEFAULT_PROFILER_CONFIG)
        if config:
            self.cfg.update(config)

        self._db_path = db_path
        self._legacy_db_path = legacy_db_path
        self._db_pool = ThreadPoolExecutor(1, thread_name_prefix="profiler_db")
        self._init_db()
        self._migrate_legacy_history_once()

        # 统计
        self.total_profiled = 0
        self.total_cabal_blocked = 0
        self.total_passed = 0
        self.total_timeout = 0
        self.total_buyers_scanned = 0

        logger.info(f"🕵️ [Profiler] 初始化完成")
        logger.info(
            f"🕵️ [Profiler] 抽样: {self.cfg['PROFILER_SAMPLE_SIZE']}人 | "
            f"危险线: {self.cfg['PROFILER_DANGER_THRESHOLD']}分 | "
            f"Cabal: ≥{self.cfg['PROFILER_CABAL_RATIO']}/{self.cfg['PROFILER_SAMPLE_SIZE']}人"
        )

    def _get_conn(self):
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        """初始化 buyer_history 表"""
        with self._get_conn() as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS buyer_history (
                address TEXT NOT NULL,
                token_addr TEXT NOT NULL,
                block_number INTEGER DEFAULT 0,
                first_seen REAL NOT NULL,
                PRIMARY KEY (address, token_addr)
            )""")
            conn.execute("""CREATE INDEX IF NOT EXISTS idx_buyer_history_addr
                ON buyer_history (address)""")
            conn.execute("""CREATE INDEX IF NOT EXISTS idx_buyer_history_time
                ON buyer_history (first_seen)""")
            conn.commit()
        logger.info(f"🕵️ [Profiler] buyer_history 表就绪: {self._db_path}")

    def _migrate_legacy_history_once(self):
        """首次启动时把 risk_stats 中旧 buyer_history 迁移到独立库。"""
        legacy = (self._legacy_db_path or "").strip()
        if not legacy or legacy == self._db_path:
            return
        if not os.path.exists(legacy):
            return

        try:
            with self._get_conn() as conn:
                # 当前库有数据就不再迁移
                cur_total = conn.execute("SELECT COUNT(*) FROM buyer_history").fetchone()[0]
                if cur_total > 0:
                    return

                conn.execute("ATTACH DATABASE ? AS legacy", (legacy,))
                try:
                    row = conn.execute(
                        "SELECT name FROM legacy.sqlite_master WHERE type='table' AND name='buyer_history'"
                    ).fetchone()
                    if not row:
                        return

                    conn.execute(
                        """
                        INSERT OR IGNORE INTO buyer_history (address, token_addr, block_number, first_seen)
                        SELECT address, token_addr, COALESCE(block_number, 0), first_seen
                        FROM legacy.buyer_history
                        """
                    )
                    moved = conn.execute("SELECT changes()").fetchone()[0]
                    conn.commit()
                    if moved > 0:
                        logger.info(f"🕵️ [Profiler] 旧历史迁移完成: +{moved} 条 ({legacy} -> {self._db_path})")
                finally:
                    with contextlib.suppress(Exception):
                        conn.execute("DETACH DATABASE legacy")
        except Exception as e:
            logger.warning(f"⚠️ [Profiler] 旧历史迁移失败: {e}")

    # ================= 核心侧写 =================

    async def profile(self, token_addr: str, buyers: set,
                      buyer_blocks: dict, creation_block: int = 0) -> tuple:
        """
        对前 N 个最早买入的地址做多维侧写

        Args:
            token_addr: 代币地址
            buyers: 全部独立买家地址集合
            buyer_blocks: {addr: first_seen_block}
            creation_block: 代币创建时的区块号

        Returns:
            (passed: bool, report: str)
        """
        if not self.cfg.get("PROFILER_ENABLED", True):
            return True, "侧写已关闭"

        self.total_profiled += 1
        token_chk = Web3.to_checksum_address(token_addr)

        # 先把所有买家写入历史库 (异步, 不阻塞)
        asyncio.create_task(self._record_buyers(token_chk, buyers, buyer_blocks))

        if not buyers:
            self.total_passed += 1
            return True, "无买家数据"

        # 选取前 N 个最早买入的地址
        sample_size = min(self.cfg["PROFILER_SAMPLE_SIZE"], len(buyers))
        sorted_buyers = sorted(
            [(addr, buyer_blocks.get(addr, float('inf'))) for addr in buyers],
            key=lambda x: x[1]
        )
        sample_addrs = [addr for addr, _ in sorted_buyers[:sample_size]]
        self.total_buyers_scanned += len(sample_addrs)

        # 带超时的侧写
        try:
            result = await asyncio.wait_for(
                self._do_profile(token_chk, sample_addrs, buyer_blocks, creation_block),
                timeout=self.cfg["PROFILER_TIMEOUT"]
            )
            return result
        except asyncio.TimeoutError:
            self.total_timeout += 1
            logger.warning(f"⏰ [Profiler] {token_chk[:12]}... 侧写超时，当前单拦截")
            return False, f"侧写超时 ({self.cfg['PROFILER_TIMEOUT']}s)，当前单拦截"

    async def _do_profile(self, token_addr: str, sample_addrs: list,
                          buyer_blocks: dict, creation_block: int) -> tuple:
        """执行多维侧写"""

        # ===== Step 1: 并行 RPC 查询 nonce + balance =====
        rpc_tasks = []
        for addr in sample_addrs:
            rpc_tasks.append(self._fetch_addr_info(addr))

        addr_infos = await asyncio.gather(*rpc_tasks, return_exceptions=True)

        # 整理数据
        profiles = []
        for i, addr in enumerate(sample_addrs):
            info = addr_infos[i]
            if isinstance(info, Exception):
                info = {"nonce": None, "balance_bnb": None}
            profile = {
                "addr": addr,
                "nonce": info.get("nonce"),
                "balance_bnb": info.get("balance_bnb"),
                "first_block": buyer_blocks.get(addr, 0),
                "score": 0,
                "flags": [],
            }
            profiles.append(profile)

        # ===== Step 2: 查询跨盘历史 (批量) =====
        cross_launch_counts = await self._batch_cross_launch_check(
            [p["addr"] for p in profiles], token_addr
        )

        # ===== Step 3: 4 维打分 =====

        # 统计同块聚集
        block_counter = Counter(
            p["first_block"] for p in profiles if p["first_block"] > 0
        )
        crowded_blocks = {
            block for block, count in block_counter.items()
            if count >= self.cfg["SAME_BLOCK_MIN_COUNT"]
        }

        # 统计 nonce 一致性
        low_nonce_count = sum(
            1 for p in profiles
            if p["nonce"] is not None and p["nonce"] < self.cfg["NONCE_UNIFORMITY_THRESHOLD"]
        )
        nonce_uniform = low_nonce_count >= self.cfg["NONCE_UNIFORMITY_MIN_COUNT"]

        for p in profiles:
            score = 0
            flags = []

            # 维度 1: 一次性钱包
            if (p["nonce"] is not None and p["balance_bnb"] is not None
                    and p["nonce"] <= self.cfg["DISPOSABLE_MAX_NONCE"]
                    and p["balance_bnb"] < self.cfg["DISPOSABLE_MAX_BALANCE_BNB"]):
                score += self.cfg["SCORE_DISPOSABLE_WALLET"]
                flags.append(f"手套(n={p['nonce']},b={p['balance_bnb']:.3f})")

            # 维度 2: 狙击机器人
            if (creation_block > 0 and p["first_block"] > 0
                    and p["first_block"] - creation_block <= self.cfg["SNIPER_BLOCK_WINDOW"]):
                score += self.cfg["SCORE_SNIPER_BOT"]
                flags.append(f"狙击(+{p['first_block'] - creation_block}块)")

            # 维度 3: 跨盘串联
            cross_count = cross_launch_counts.get(p["addr"], 0)
            if cross_count >= self.cfg["CROSS_LAUNCH_MIN_TOKENS"]:
                score += self.cfg["SCORE_CROSS_LAUNCH"]
                flags.append(f"串联({cross_count}盘)")

            # 维度 4: 同块聚集
            if p["first_block"] in crowded_blocks:
                score += self.cfg["SCORE_SAME_BLOCK"]
                flags.append("同块")

            # 附加: 群体 nonce 一致性
            if nonce_uniform and p["nonce"] is not None and p["nonce"] < self.cfg["NONCE_UNIFORMITY_THRESHOLD"]:
                score += self.cfg["SCORE_NONCE_UNIFORMITY"]
                flags.append("齐刷刷")

            p["score"] = score
            p["flags"] = flags

        # ===== Step 4: 汇总决策 =====
        danger_threshold = self.cfg["PROFILER_DANGER_THRESHOLD"]
        cabal_min = self.cfg["PROFILER_CABAL_RATIO"]

        dangerous_count = sum(1 for p in profiles if p["score"] >= danger_threshold)

        # 生成报告
        report_lines = []
        for p in profiles:
            is_danger = p["score"] >= danger_threshold
            emoji = "🔴" if is_danger else "🟢"
            flags_text = ", ".join(p["flags"]) if p["flags"] else "无标记"
            nonce_text = str(p["nonce"]) if p["nonce"] is not None else "N/A"
            bal_text = f"{p['balance_bnb']:.3f}" if p["balance_bnb"] is not None else "N/A"
            report_lines.append(
                f"  {emoji} {p['addr'][:10]}... | "
                f"分:{p['score']} | n={nonce_text} bal={bal_text} | "
                f"{flags_text}"
            )

        report = "\n".join(report_lines)
        verdict = f"危险: {dangerous_count}/{len(profiles)} (阈值 ≥{cabal_min})"

        if dangerous_count >= cabal_min:
            self.total_cabal_blocked += 1
            logger.warning(
                f"🚨 [Profiler] Cabal 检测! {token_addr[:12]}... | "
                f"{dangerous_count}/{len(profiles)} 人危险\n{report}"
            )
            return False, f"Cabal 控盘! {verdict}\n{report}"
        else:
            self.total_passed += 1
            logger.info(
                f"✅ [Profiler] {token_addr[:12]}... 通过 | {verdict}"
            )
            return True, f"通过 | {verdict}\n{report}"

    async def _fetch_addr_info(self, addr: str) -> dict:
        """并行获取单个地址的 nonce 和 balance"""
        addr_chk = Web3.to_checksum_address(addr)

        nonce_task = self.fleet.safe_call(
            'get_transaction_count', addr_chk, 'latest', tiers=[1, 2]
        )
        balance_task = self.fleet.safe_call(
            'get_balance', addr_chk, tiers=[1, 2]
        )

        nonce, balance = await asyncio.gather(nonce_task, balance_task, return_exceptions=True)

        result = {"nonce": None, "balance_bnb": None}

        if not isinstance(nonce, Exception) and nonce is not None:
            result["nonce"] = nonce

        if not isinstance(balance, Exception) and balance is not None:
            try:
                result["balance_bnb"] = float(Web3.from_wei(balance, 'ether'))
            except Exception:
                pass

        return result

    # ================= 跨盘历史查询 =================

    async def _batch_cross_launch_check(self, addrs: list,
                                         exclude_token: str) -> dict:
        """
        批量查询: 这些地址在过去 24h 内出现在几个其他代币中

        返回: {addr: cross_launch_count}
        """
        lookback_hours = self.cfg["CROSS_LAUNCH_LOOKBACK_HOURS"]
        cutoff = time.time() - (lookback_hours * 3600)

        try:
            def _query():
                results = {}
                with self._get_conn() as conn:
                    for addr in addrs:
                        addr_lower = addr.lower()
                        cursor = conn.execute(
                            "SELECT COUNT(DISTINCT token_addr) FROM buyer_history "
                            "WHERE address = ? AND token_addr != ? AND first_seen > ?",
                            (addr_lower, exclude_token.lower(), cutoff)
                        )
                        row = cursor.fetchone()
                        results[addr] = row[0] if row else 0
                return results

            return await asyncio.get_running_loop().run_in_executor(
                self._db_pool, _query
            )
        except Exception as e:
            logger.debug(f"⚠️ [Profiler] 跨盘查询异常: {e}")
            return {}

    async def _record_buyers(self, token_addr: str, buyers: set,
                              buyer_blocks: dict):
        """将买家记录写入历史数据库 (异步)"""
        if not buyers:
            return
        try:
            now = time.time()
            records = [
                (addr.lower(), token_addr.lower(),
                 buyer_blocks.get(addr, 0), now)
                for addr in buyers
            ]

            def _write():
                with self._get_conn() as conn:
                    conn.executemany(
                        "INSERT OR IGNORE INTO buyer_history "
                        "(address, token_addr, block_number, first_seen) "
                        "VALUES (?, ?, ?, ?)",
                        records
                    )
                    conn.commit()

            await asyncio.get_running_loop().run_in_executor(
                self._db_pool, _write
            )
        except Exception as e:
            logger.debug(f"⚠️ [Profiler] 买家记录写入异常: {e}")

    # ================= 历史数据维护 =================

    async def cleanup_old_history(self, days: int = 7) -> int:
        """清理过期的 buyer_history 记录，返回删除条数。"""
        cutoff = time.time() - (days * 86400)
        try:
            def _cleanup():
                with self._get_conn() as conn:
                    cursor = conn.execute(
                        "DELETE FROM buyer_history WHERE first_seen < ?", (cutoff,)
                    )
                    deleted = cursor.rowcount
                    conn.commit()
                    return int(deleted or 0)

            deleted = await asyncio.get_running_loop().run_in_executor(
                self._db_pool, _cleanup
            )
            if deleted > 0:
                logger.info(f"🧹 [Profiler] 清理 {deleted} 条过期买家记录 (>{days}天)")
            return deleted
        except Exception as e:
            logger.debug(f"⚠️ [Profiler] 历史清理异常: {e}")
            return 0

    def get_history_stats(self) -> dict:
        """获取 buyer_history 表统计"""
        try:
            with self._get_conn() as conn:
                total = conn.execute("SELECT COUNT(*) FROM buyer_history").fetchone()[0]
                unique_addr = conn.execute(
                    "SELECT COUNT(DISTINCT address) FROM buyer_history"
                ).fetchone()[0]
                unique_tokens = conn.execute(
                    "SELECT COUNT(DISTINCT token_addr) FROM buyer_history"
                ).fetchone()[0]
                return {"total_records": total, "unique_addresses": unique_addr,
                        "unique_tokens": unique_tokens}
        except Exception:
            return {"total_records": 0, "unique_addresses": 0, "unique_tokens": 0}

    # ================= 配置热更新 =================

    def update_config(self, **kwargs):
        updated = []
        for k, v in kwargs.items():
            if k in self.cfg and v is not None:
                old = self.cfg[k]
                self.cfg[k] = v
                updated.append(f"{k}: {old}→{v}")
        if updated:
            logger.info(f"⚙️ [Profiler] 配置更新: {', '.join(updated)}")
        return updated

    # ================= TG 状态 =================

    def get_status_text(self) -> str:
        cabal_rate = (self.total_cabal_blocked / self.total_profiled * 100) if self.total_profiled > 0 else 0
        history = self.get_history_stats()

        return (
            f"🕵️ <b>买家侧写雷达 v1.0</b> (纯RPC)\n"
            f"状态: {'🟢 启用' if self.cfg.get('PROFILER_ENABLED') else '🔴 关闭'}\n"
            f"侧写: {self.total_profiled} | Cabal拦截: {self.total_cabal_blocked} ({cabal_rate:.0f}%) | 通过: {self.total_passed}\n"
            f"扫描买家: {self.total_buyers_scanned} | 超时放行: {self.total_timeout}\n"
            f"━━━ 历史库 ━━━\n"
            f"记录: {history['total_records']} | 地址: {history['unique_addresses']} | 代币: {history['unique_tokens']}\n"
            f"━━━ 参数 ━━━\n"
            f"抽样: {self.cfg['PROFILER_SAMPLE_SIZE']}人 | "
            f"危险线: {self.cfg['PROFILER_DANGER_THRESHOLD']}分 | "
            f"Cabal: ≥{self.cfg['PROFILER_CABAL_RATIO']}人\n"
            f"打分: 手套={self.cfg['SCORE_DISPOSABLE_WALLET']} "
            f"狙击={self.cfg['SCORE_SNIPER_BOT']} "
            f"串联={self.cfg['SCORE_CROSS_LAUNCH']} "
            f"同块={self.cfg['SCORE_SAME_BLOCK']} "
            f"齐刷刷={self.cfg['SCORE_NONCE_UNIFORMITY']}"
        )

    def shutdown(self):
        """关闭 DB 线程池"""
        self._db_pool.shutdown(wait=True)
        logger.info("🔌 [Profiler] DB 线程池已关闭")
