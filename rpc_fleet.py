"""
rpc_fleet.py — 三层 RPC 舰队管理（T0 付费 / T1 免费 / T2 公益兜底）
"""

import asyncio
import logging
import random
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from web3 import Web3

logger = logging.getLogger("RPC_Fleet")

# ================= v12.1: get_logs 调用追踪 =================
class GetLogsTracer:
    """追踪 get_logs 调用的来源和性能"""
    def __init__(self):
        self.stats = {}  # source -> {"count": 0, "wait_time": 0, "exec_time": 0}
        self.last_reset = time.time()
        self.lock = asyncio.Lock()
    
    async def record(self, source: str, wait_time: float, exec_time: float):
        async with self.lock:
            if source not in self.stats:
                self.stats[source] = {"count": 0, "wait_time": 0, "exec_time": 0}
            self.stats[source]["count"] += 1
            self.stats[source]["wait_time"] += wait_time
            self.stats[source]["exec_time"] += exec_time
    
    def get_report(self) -> str:
        now = time.time()
        duration = now - self.last_reset
        lines = [f"📊 get_logs 调用统计 (过去 {duration:.0f} 秒):"]
        
        total_calls = sum(s["count"] for s in self.stats.values())
        lines.append(f"总调用: {total_calls}")
        
        # 按调用次数排序
        sorted_sources = sorted(self.stats.items(), key=lambda x: x[1]["count"], reverse=True)
        for source, data in sorted_sources[:10]:  # 前10个来源
            avg_wait = data["wait_time"] / data["count"] if data["count"] > 0 else 0
            avg_exec = data["exec_time"] / data["count"] if data["count"] > 0 else 0
            lines.append(f"  {source[:25]:<25} {data['count']:>4}次  等待{avg_wait*1000:>5.0f}ms  执行{avg_exec*1000:>5.0f}ms")
        
        return "\n".join(lines)
    
    def reset(self):
        self.stats.clear()
        self.last_reset = time.time()

get_logs_tracer = GetLogsTracer()

# ================= 默认节点配置 =================
# 填入你自己的付费节点地址（从 bsc_sys_config.json 读取，或直接在此处填写）
PAID_DRPC_URL = ""  # 例如: https://lb.drpc.live/bsc/YOUR_KEY

# T1 半付费节点：填入你自己申请的免费额度节点（可留空）
PREMIUM_FREE_RPCS = [
    # "https://bsc-mainnet.nodereal.io/v1/YOUR_KEY",
    # "https://bsc.blockpi.network/v1/rpc/YOUR_KEY",
    # "https://bsc-mainnet.core.chainstack.com/YOUR_KEY",
    # "https://go.getblock.us/YOUR_KEY",
]

PUBLIC_FREE_RPCS = [
    "https://bsc.rpc.blxrbdn.com",
    "https://1rpc.io/bnb",
    "https://bsc.publicnode.com",
    "https://bsc-rpc.publicnode.com",
    "https://bsc.meowrpc.com",
    "https://bsc.drpc.org",
    "https://bsc-mainnet.public.blastapi.io",
    "https://bsc.blockrazor.xyz",
    "https://rpc.ankr.com/bsc",
    "https://api.zan.top/bsc-mainnet",
    "https://endpoints.omniatech.io/v1/bsc/mainnet/public",
    "https://bsc.api.onfinality.io/public",
    "https://bnb.api.onfinality.io/public",
    "https://binance.llamarpc.com",
    "https://public-bsc-mainnet.fastnode.io",
    "https://rpc.sentio.xyz/bsc",
    "https://binance-smart-chain-public.nodies.app",
    "https://bsc-mainnet-rpc.allthatnode.com",
    "https://rpc.polkachu.com/bsc",
    "https://bsc-mainnet.rpcfast.com",
]

# ================= 配置常量 =================
CHUNK_MAX_BLOCKS = 30
CHUNK_MAX_CONCURRENT = 3
RACE_TIMEOUT = 5.0

HEAVY_SEM_SLOTS = 8
HEAVY_HIGH_TIMEOUT = 5.0

CHUNKED_LOGS_MAX_TOTAL_BLOCKS = 5000

T2_HEALTH_INTERVAL = 300
T2_MAX_CONSECUTIVE_FAIL = 3

CACHE_MAX_DECIMALS = 2000
CACHE_MAX_NAME_SYM = 2000

TIER0_POOL_SIZE = 10
TIER1_POOL_SIZE = 30
TIER2_POOL_SIZE = 50

BLOCK_NUMBER_CACHE_TTL = 1.0

# v12.0: 舰队级熔断参数
FLEET_BREAKER_THRESHOLD = 8          # 连续全失败次数触发熔断
FLEET_BREAKER_SLEEP_SEC = 30         # 熔断休眠时间
FLEET_BREAKER_PROBE_INTERVAL = 10    # 探活间隔
FLEET_BREAKER_RECOVERY_COUNT = 2     # 连续成功 N 次后解除熔断

# v12.0: RPC 调用最终超时兜底 (防协程永久挂起)
RPC_EXECUTOR_TIMEOUT = 20.0          # 单次 run_in_executor 最大等待
RPC_EXECUTOR_TIMEOUT_HEAVY = 30.0    # get_logs 等重操作最大等待


class LRUCache:
    """简单 LRU 缓存"""

    def __init__(self, capacity: int):
        self._cache = OrderedDict()
        self._capacity = capacity

    def get(self, key, default=None):
        if key in self._cache:
            self._cache.move_to_end(key)
            return self._cache[key]
        return default

    def __contains__(self, key):
        return key in self._cache

    def __setitem__(self, key, value):
        if key in self._cache:
            self._cache.move_to_end(key)
        self._cache[key] = value
        if len(self._cache) > self._capacity:
            self._cache.popitem(last=False)

    def __getitem__(self, key):
        self._cache.move_to_end(key)
        return self._cache[key]


class FleetCircuitBreaker:
    """
    v12.0: 舰队级熔断器
    当全部节点连续失败时，系统自动休眠避免雪崩。
    """

    def __init__(self):
        self._consecutive_all_fail = 0
        self._is_open = False                    # 熔断是否开启
        self._open_since = 0                     # 熔断开始时间
        self._recovery_successes = 0             # 恢复期连续成功次数
        self._total_trips = 0                    # 累计熔断次数
        self._last_trip_time = 0
        self._lock = asyncio.Lock()

    @property
    def is_tripped(self) -> bool:
        return self._is_open

    def record_success(self):
        """记录一次成功调用"""
        self._consecutive_all_fail = 0
        if self._is_open:
            self._recovery_successes += 1
            if self._recovery_successes >= FLEET_BREAKER_RECOVERY_COUNT:
                self._is_open = False
                self._recovery_successes = 0
                logger.info(
                    f"🟢 [Fleet Breaker] 熔断解除 (连续 {FLEET_BREAKER_RECOVERY_COUNT} 次成功)"
                )

    def record_all_fail(self):
        """记录一次全层失败"""
        self._consecutive_all_fail += 1
        self._recovery_successes = 0

        if not self._is_open and self._consecutive_all_fail >= FLEET_BREAKER_THRESHOLD:
            self._is_open = True
            self._open_since = time.time()
            self._total_trips += 1
            self._last_trip_time = time.time()
            logger.error(
                f"🔴 [Fleet Breaker] 舰队级熔断触发! "
                f"连续 {self._consecutive_all_fail} 次全失败 "
                f"(#{self._total_trips}) → 休眠 {FLEET_BREAKER_SLEEP_SEC}s"
            )

    async def wait_if_tripped(self):
        """如果熔断已触发，等待休眠期结束"""
        if not self._is_open:
            return

        elapsed = time.time() - self._open_since
        if elapsed < FLEET_BREAKER_SLEEP_SEC:
            sleep_remaining = FLEET_BREAKER_SLEEP_SEC - elapsed
            logger.warning(
                f"💤 [Fleet Breaker] 熔断休眠中... 剩余 {sleep_remaining:.0f}s"
            )
            await asyncio.sleep(min(sleep_remaining, FLEET_BREAKER_PROBE_INTERVAL))

    def get_status_text(self) -> str:
        if self._is_open:
            elapsed = int(time.time() - self._open_since)
            return f"🔴 熔断中 ({elapsed}s / {FLEET_BREAKER_SLEEP_SEC}s) 累计: {self._total_trips}次"
        return f"🟢 正常 (连续失败: {self._consecutive_all_fail}/{FLEET_BREAKER_THRESHOLD}) 累计: {self._total_trips}次"


class RpcFleet:
    """
    三层分队 RPC 舰队 v12.0
    T0: 付费精锐 — 买卖/nonce/gas
    T1: 高质量免费 — Detector/验资/get_logs
    T2: 公益节点池 — 只读查询/Monitor采样

    v12.0: + 舰队级熔断 + RPC 超时兜底
    """

    def __init__(self, extra_rpcs: list = None, extra_t2_rpcs: list = None,
                 heavy_slots: int = None):
        self.nodes = {}
        self.paid_url = PAID_DRPC_URL

        self._executor_t0 = ThreadPoolExecutor(
            max_workers=TIER0_POOL_SIZE, thread_name_prefix="rpc_t0"
        )
        self._executor_t1 = ThreadPoolExecutor(
            max_workers=TIER1_POOL_SIZE, thread_name_prefix="rpc_t1"
        )
        self._executor_t2 = ThreadPoolExecutor(
            max_workers=TIER2_POOL_SIZE, thread_name_prefix="rpc_t2"
        )
        self._tier_executors = {
            0: self._executor_t0,
            1: self._executor_t1,
            2: self._executor_t2,
        }

        self._block_number_cache = None
        self._block_number_lock = None
        self._block_number_inflight = None

        # v12.0: 舰队级熔断器
        self.circuit_breaker = FleetCircuitBreaker()

        # === T0 ===
        self._add_node(PAID_DRPC_URL, tier=0, timeout=15)
        self.t0_urls = [PAID_DRPC_URL]
        self.t0_robin_idx = 0

        # === T1 ===
        self.t1_urls = []
        all_t1 = list(PREMIUM_FREE_RPCS)
        if extra_rpcs:
            all_t1.extend(extra_rpcs)
        for url in all_t1:
            if url not in self.nodes:
                self._add_node(url, tier=1, timeout=12)
                self.t1_urls.append(url)

        # === T2 ===
        self.t2_urls = []
        all_t2 = list(PUBLIC_FREE_RPCS)
        if extra_t2_rpcs:
            all_t2.extend(extra_t2_rpcs)
        for url in all_t2:
            if url not in self.nodes:
                self._add_node(url, tier=2, timeout=8)
                self.t2_urls.append(url)

        self.t2_kicked = set()

        self.heavy_slots = max(1, int(heavy_slots or HEAVY_SEM_SLOTS))
        self.heavy_sem = asyncio.Semaphore(self.heavy_slots)
        self.heavy_drops = 0
        self.heavy_waits = 0

        self._health_task = None

        self.block_number_dedup_hits = 0
        self.block_number_total_calls = 0

        logger.info(
            f"🛡️ [RPC舰队] T0:{len(self.t0_urls)} | "
            f"T1:{len(self.t1_urls)} | T2:{len(self.t2_urls)} | "
            f"get_logs槽: {self.heavy_slots} | "
            f"线程池: T0={TIER0_POOL_SIZE} T1={TIER1_POOL_SIZE} T2={TIER2_POOL_SIZE} | "
            f"熔断: {FLEET_BREAKER_THRESHOLD}次/{FLEET_BREAKER_SLEEP_SEC}s"
        )

    def _add_node(self, url: str, tier: int, timeout: int = 12):
        try:
            w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": timeout}))
            self.nodes[url] = {
                "w3": w3, "tier": tier,
                "success": 10.0, "fail": 0.0, "latency": 0.2,
                "cooldown_until": 0, "req_count": 0, "err_count": 0,
                "consecutive_fail": 0, "kicked": False,
            }
        except Exception as e:
            logger.warning(f"⚠️ [RPC舰队] 节点初始化失败: {url[:30]}... | {e}")

    def _get_executor_for_url(self, url: str) -> ThreadPoolExecutor:
        node = self.nodes.get(url)
        if node:
            return self._tier_executors.get(node["tier"], self._executor_t2)
        return self._executor_t2

    @property
    def default_w3(self):
        return self.nodes[self.paid_url]["w3"]

    @property
    def providers(self):
        return [self.default_w3]

    # ================= block_number 去重 =================

    async def _get_block_number_dedup(self, tiers: list = None) -> Optional[int]:
        self.block_number_total_calls += 1
        if self._block_number_cache:
            ts, bn = self._block_number_cache
            if time.time() - ts < BLOCK_NUMBER_CACHE_TTL:
                self.block_number_dedup_hits += 1
                return bn

        if self._block_number_lock is None:
            self._block_number_lock = asyncio.Lock()

        try:
            async with asyncio.timeout(10.0):
                async with self._block_number_lock:
                    if self._block_number_cache:
                        ts, bn = self._block_number_cache
                        if time.time() - ts < BLOCK_NUMBER_CACHE_TTL:
                            self.block_number_dedup_hits += 1
                            return bn

                    result = await self._safe_call_internal(
                        'block_number', tiers=tiers or [2, 1]
                    )
                    if result is not None:
                        self._block_number_cache = (time.time(), result)
                    return result
        except asyncio.TimeoutError:
            logger.warning("⏰ [RPC] block_number 去重查询超时")
            return None

    # ================= 启动健康检查 =================

    def start_health_check(self):
        if self._health_task is None:
            self._health_task = asyncio.create_task(self._t2_health_loop())
            logger.info("🏥 [RPC舰队] T2 健康检查已启动")

    async def _t2_health_loop(self):
        while True:
            await asyncio.sleep(T2_HEALTH_INTERVAL)
            alive = 0
            kicked_this_round = 0
            for url in list(self.t2_urls):
                if url in self.t2_kicked:
                    continue
                node = self.nodes.get(url)
                if not node:
                    continue
                try:
                    executor = self._get_executor_for_url(url)

                    def _check():
                        return node["w3"].eth.block_number

                    await asyncio.wait_for(
                        asyncio.get_running_loop().run_in_executor(executor, _check),
                        timeout=5.0
                    )
                    node["consecutive_fail"] = 0
                    alive += 1
                except Exception:
                    node["consecutive_fail"] += 1
                    if node["consecutive_fail"] >= T2_MAX_CONSECUTIVE_FAIL:
                        node["kicked"] = True
                        self.t2_kicked.add(url)
                        kicked_this_round += 1
                        logger.warning(
                            f"🏥 [RPC舰队] T2 踢出: {url.split('//')[-1].split('/')[0][:25]}"
                        )

            active_t2 = len(self.t2_urls) - len(self.t2_kicked)
            if kicked_this_round > 0:
                logger.info(
                    f"🏥 [RPC舰队] T2 健康检查: 存活={alive} 踢出={kicked_this_round} "
                    f"剩余={active_t2}/{len(self.t2_urls)}"
                )

    # ================= 优雅关闭 =================

    def shutdown(self):
        if self._health_task:
            self._health_task.cancel()
            self._health_task = None

        for name, executor in [
            ("T0", self._executor_t0),
            ("T1", self._executor_t1),
            ("T2", self._executor_t2),
        ]:
            try:
                executor.shutdown(wait=False)
                logger.info(f"🔌 [RPC舰队] {name} 线程池已关闭")
            except Exception as e:
                logger.warning(f"⚠️ [RPC舰队] {name} 线程池关闭异常: {e}")

    # ================= 节点管理 =================

    def add_node(self, url: str, tier: int = 1) -> str:
        url = url.strip()
        if url in self.nodes:
            return f"⚠️ 节点已存在"
        try:
            timeout = {0: 15, 1: 12, 2: 8}.get(tier, 12)
            self._add_node(url, tier, timeout)
            tier_name = {0: "T0-付费", 1: "T1-高免", 2: "T2-公益"}.get(tier, f"T{tier}")
            display = url.split("//")[-1].split("/")[0][:30]
            if tier == 0:
                self.t0_urls.append(url)
            elif tier == 1:
                self.t1_urls.append(url)
            else:
                self.t2_urls.append(url)
            return f"✅ 已添加 [{tier_name}] {display}"
        except Exception as e:
            return f"❌ 添加失败: {e}"

    def remove_node(self, url: str) -> str:
        url = url.strip()
        if url == self.paid_url:
            return "❌ 不能删除付费主将"
        if url not in self.nodes:
            matched = [u for u in self.nodes if url in u]
            if len(matched) == 1:
                url = matched[0]
            elif len(matched) > 1:
                return f"❌ 匹配多个，请提供完整URL"
            else:
                return f"❌ 节点不存在"
        display = url.split("//")[-1].split("/")[0][:30]
        tier = self.nodes[url]["tier"]
        del self.nodes[url]
        if tier == 0:
            self.t0_urls = [u for u in self.t0_urls if u != url]
        elif tier == 1:
            self.t1_urls = [u for u in self.t1_urls if u != url]
        else:
            self.t2_urls = [u for u in self.t2_urls if u != url]
            self.t2_kicked.discard(url)
        return f"🗑️ 已移除: {display} (T{tier})"

    def set_heavy_slots(self, slots: int) -> str:
        """热更新 get_logs 并发槽（后续请求生效）。"""
        try:
            new_slots = int(slots)
        except Exception:
            return "❌ 槽位必须是整数"

        new_slots = max(1, min(32, new_slots))
        old = self.heavy_slots
        if new_slots == old:
            return f"ℹ️ get_logs 并发槽保持不变: {old}"

        self.heavy_slots = new_slots
        self.heavy_sem = asyncio.Semaphore(new_slots)
        logger.info(f"⚙️ [RPC舰队] get_logs 并发槽热更新: {old} -> {new_slots}")
        return f"✅ get_logs 并发槽: {old} -> {new_slots}"

    def list_nodes(self) -> list:
        return [{"url": url, "tier": s["tier"], "req_count": s["req_count"],
                 "err_count": s["err_count"], "latency": s["latency"],
                 "kicked": s.get("kicked", False)}
                for url, s in self.nodes.items()]

    # ================= 分层节点选择 =================

    def _get_t0_node(self):
        if not self.t0_urls:
            return self._get_t1_node()
        if len(self.t0_urls) == 1:
            url = self.t0_urls[0]
            return url, self.nodes[url]["w3"]

        now = time.time()
        best_url = None
        best_latency = float('inf')
        for url in self.t0_urls:
            node = self.nodes.get(url)
            if not node:
                continue
            if node["cooldown_until"] > now:
                continue
            if node["latency"] < best_latency:
                best_latency = node["latency"]
                best_url = url

        if best_url is None:
            url = self.t0_urls[self.t0_robin_idx % len(self.t0_urls)]
            self.t0_robin_idx += 1
            return url, self.nodes[url]["w3"]
        return best_url, self.nodes[best_url]["w3"]

    def _get_t1_node(self, exclude_urls: set = None):
        now = time.time()
        exclude = exclude_urls or set()
        candidates = [
            u for u in self.t1_urls
            if u not in exclude and u in self.nodes and self.nodes[u]["cooldown_until"] < now
        ]
        if not candidates:
            return self._get_t0_node()
        candidates.sort(key=lambda u: self.nodes[u]["latency"])
        url = candidates[0]
        return url, self.nodes[url]["w3"]

    def _get_t2_node(self, exclude_urls: set = None):
        now = time.time()
        exclude = exclude_urls or set()
        candidates = [
            u for u in self.t2_urls
            if u not in exclude
               and u not in self.t2_kicked
               and u in self.nodes
               and self.nodes[u]["cooldown_until"] < now
        ]
        if not candidates:
            return self._get_t1_node(exclude_urls)
        url = random.choice(candidates)
        return url, self.nodes[url]["w3"]

    def _get_node_by_tier(self, tiers: list = None, exclude_urls: set = None):
        if tiers is None:
            tiers = [1, 2, 0]
        for t in tiers:
            if t == 0:
                return self._get_t0_node()
            elif t == 1:
                result = self._get_t1_node(exclude_urls)
                if result:
                    return result
            elif t == 2:
                result = self._get_t2_node(exclude_urls)
                if result:
                    return result
        return self._get_t0_node()

    def _get_multiple_nodes(self, count: int = 3, tiers: list = None) -> list:
        if tiers is None:
            tiers = [1, 2]
        now = time.time()
        candidates = []
        for t in tiers:
            tier_urls = {0: self.t0_urls, 1: self.t1_urls, 2: self.t2_urls}.get(t, [])
            for url in tier_urls:
                if url in self.nodes and self.nodes[url]["cooldown_until"] < now:
                    if t == 2 and url in self.t2_kicked:
                        continue
                    candidates.append(url)

        if not candidates:
            return [(self.paid_url, self.nodes[self.paid_url]["w3"])]

        if len(candidates) <= count:
            selected = candidates
        else:
            t1_cands = [u for u in candidates if self.nodes[u]["tier"] <= 1]
            t2_cands = [u for u in candidates if self.nodes[u]["tier"] == 2]
            t1_cands.sort(key=lambda u: self.nodes[u]["latency"])
            selected = t1_cands[:max(1, count // 2)]
            remaining = count - len(selected)
            if remaining > 0 and t2_cands:
                selected.extend(random.sample(t2_cands, min(remaining, len(t2_cands))))

        return [(url, self.nodes[url]["w3"]) for url in selected[:count]]

    # ================= 内部 safe_call (v12.0: + 熔断 + 超时兜底) =================

    async def _safe_call_internal(self, method_name, *args, tiers: list = None,
                                   escalate_on_fail=False, priority="normal", **kwargs):
        if tiers is None:
            tiers = [1, 2, 0]

        # ★ v12.0: 熔断检查 — 如果舰队熔断中，先等待
        await self.circuit_breaker.wait_if_tripped()

        is_heavy = (method_name == 'get_logs')
        acquired = False
        sem_ref = None
        exec_timeout = RPC_EXECUTOR_TIMEOUT_HEAVY if is_heavy else RPC_EXECUTOR_TIMEOUT

        if is_heavy:
            sem_ref = self.heavy_sem
            if priority == "high":
                try:
                    await asyncio.wait_for(sem_ref.acquire(), timeout=HEAVY_HIGH_TIMEOUT)
                    acquired = True
                    self.heavy_waits += 1
                except asyncio.TimeoutError:
                    self.heavy_drops += 1
                    return None
            else:
                if sem_ref.locked():
                    self.heavy_drops += 1
                    return None
                await sem_ref.acquire()
                acquired = True

        try:
            failed_urls = set()
            for attempt in range(4):
                current_tiers = tiers
                if escalate_on_fail and attempt >= 2:
                    current_tiers = [0] + tiers

                url, w3 = self._get_node_by_tier(current_tiers, exclude_urls=failed_urls)
                start_t = time.time()
                self.nodes[url]["req_count"] += 1

                executor = self._get_executor_for_url(url)

                try:
                    if method_name == 'get_logs':
                        filter_dict = args[0].copy() if args else kwargs.copy()
                        if 'fromBlock' in filter_dict and isinstance(filter_dict['fromBlock'], int):
                            filter_dict['fromBlock'] = hex(filter_dict['fromBlock'])
                        if 'toBlock' in filter_dict and isinstance(filter_dict['toBlock'], int):
                            filter_dict['toBlock'] = hex(filter_dict['toBlock'])

                        def _exec():
                            return w3.eth.get_logs(filter_dict)

                    elif method_name == 'call':
                        call_dict = args[0].copy() if args else kwargs.copy()

                        def _exec():
                            return w3.eth.call(call_dict)

                    else:
                        def _exec():
                            attr = getattr(w3.eth, method_name)
                            if callable(attr):
                                return attr(*args, **kwargs)
                            return attr

                    # ★ v12.0: 统一超时兜底 — 绝不让协程无限挂起
                    res = await asyncio.wait_for(
                        asyncio.get_running_loop().run_in_executor(executor, _exec),
                        timeout=exec_timeout
                    )

                    latency = time.time() - start_t
                    node = self.nodes[url]
                    node["latency"] = node["latency"] * 0.7 + latency * 0.3
                    node["success"] += 1
                    node["consecutive_fail"] = 0

                    # ★ v12.0: 记录成功，可能解除熔断
                    self.circuit_breaker.record_success()

                    return res

                except asyncio.TimeoutError:
                    node = self.nodes[url]
                    node["fail"] += 1
                    node["consecutive_fail"] += 1
                    node["err_count"] += 1
                    failed_urls.add(url)
                    logger.debug(
                        f"⏰ [RPC] {method_name} 超时 ({exec_timeout}s) @ {url[:30]}"
                    )
                    if attempt < 3:
                        await asyncio.sleep(0.3 * (attempt + 1))

                except Exception as e:
                    err_str = str(e).lower()
                    node = self.nodes[url]
                    node["fail"] += 1
                    node["consecutive_fail"] += 1
                    failed_urls.add(url)

                    if "execution reverted" in err_str:
                        node["success"] += 1
                        node["fail"] -= 1
                        node["consecutive_fail"] = 0
                        self.circuit_breaker.record_success()
                        return b''

                    node["err_count"] += 1
                    if "429" in err_str or "too many requests" in err_str or "rate limit" in err_str:
                        node["cooldown_until"] = time.time() + 60

                    if attempt < 3:
                        await asyncio.sleep(0.3 * (attempt + 1))

            # ★ v12.0: 4 次全部失败 → 通知熔断器
            self.circuit_breaker.record_all_fail()
            return None

        finally:
            if is_heavy and acquired and sem_ref is not None:
                sem_ref.release()

    # ================= 核心 safe_call =================

    async def safe_call(self, method_name, *args, tiers: list = None,
                        escalate_on_fail=False, priority="normal", source="unknown", **kwargs):
        """
        安全调用 RPC 方法
        
        Args:
            source: 调用来源标识，用于追踪 get_logs 积压来源
        """
        if method_name == 'block_number':
            return await self._get_block_number_dedup(tiers=tiers)

        # ★ v12.1: 追踪 get_logs 调用来源
        if method_name == 'get_logs':
            wait_start = time.time()
            
        result = await self._safe_call_internal(
            method_name, *args, tiers=tiers,
            escalate_on_fail=escalate_on_fail, priority=priority, **kwargs
        )
        
        # ★ v12.1: 记录 get_logs 调用统计
        if method_name == 'get_logs':
            wait_time = time.time() - wait_start
            # 执行时间由内部记录，这里简化处理
            await get_logs_tracer.record(source, wait_time, 0)
        
        return result

    # ================= chunked_logs =================

    async def safe_call_chunked_logs(self, filter_params: dict,
                                     chunk_size: int = CHUNK_MAX_BLOCKS,
                                     tiers: list = None,
                                     priority: str = "normal") -> Optional[list]:
        if tiers is None:
            tiers = [1, 2]

        from_block = filter_params.get('fromBlock', 0)
        to_block = filter_params.get('toBlock', 'latest')

        if to_block == 'latest':
            current = await self.safe_call('block_number', tiers=[2, 1])
            if current is None:
                return None
            to_block = current

        if isinstance(from_block, str):
            from_block = int(from_block, 16)
        if isinstance(to_block, str) and to_block != 'latest':
            to_block = int(to_block, 16)

        total_blocks = to_block - from_block + 1
        if total_blocks <= 0:
            return []

        if total_blocks > CHUNKED_LOGS_MAX_TOTAL_BLOCKS:
            from_block = to_block - CHUNKED_LOGS_MAX_TOTAL_BLOCKS + 1
            total_blocks = CHUNKED_LOGS_MAX_TOTAL_BLOCKS

        if total_blocks <= chunk_size:
            return await self._safe_call_internal(
                'get_logs', filter_params,
                tiers=tiers, escalate_on_fail=True, priority=priority
            )

        chunks = []
        current = from_block
        while current <= to_block:
            end = min(current + chunk_size - 1, to_block)
            chunk_filter = dict(filter_params)
            chunk_filter['fromBlock'] = current
            chunk_filter['toBlock'] = end
            chunks.append(chunk_filter)
            current = end + 1

        available_nodes = self._get_multiple_nodes(count=max(len(chunks), 3), tiers=tiers)
        sem = asyncio.Semaphore(CHUNK_MAX_CONCURRENT)
        results = [None] * len(chunks)
        failed_indices = []

        async def _fetch_chunk(idx, chunk_filter):
            async with sem:
                node_idx = idx % len(available_nodes)
                url, w3 = available_nodes[node_idx]
                executor = self._get_executor_for_url(url)
                try:
                    f_dict = chunk_filter.copy()
                    if isinstance(f_dict.get('fromBlock'), int):
                        f_dict['fromBlock'] = hex(f_dict['fromBlock'])
                    if isinstance(f_dict.get('toBlock'), int):
                        f_dict['toBlock'] = hex(f_dict['toBlock'])

                    def _exec():
                        return w3.eth.get_logs(f_dict)

                    self.nodes[url]["req_count"] += 1
                    start_t = time.time()
                    # ★ v12.0: 超时兜底
                    res = await asyncio.wait_for(
                        asyncio.get_running_loop().run_in_executor(executor, _exec),
                        timeout=RPC_EXECUTOR_TIMEOUT_HEAVY
                    )
                    latency = time.time() - start_t
                    node = self.nodes[url]
                    node["latency"] = node["latency"] * 0.7 + latency * 0.3
                    node["success"] += 1
                    results[idx] = res if res else []
                except Exception:
                    self.nodes[url]["fail"] += 1
                    self.nodes[url]["err_count"] += 1
                    failed_indices.append(idx)

        await asyncio.gather(*[_fetch_chunk(i, c) for i, c in enumerate(chunks)])

        if failed_indices:
            for idx in failed_indices:
                retry_result = await self._safe_call_internal(
                    'get_logs', chunks[idx],
                    tiers=[0, 1], escalate_on_fail=True, priority=priority
                )
                if retry_result is not None:
                    results[idx] = retry_result
                else:
                    return None

        all_logs = []
        for chunk_result in results:
            if chunk_result:
                all_logs.extend(chunk_result)
        return all_logs

    # ================= race =================

    async def safe_call_race(self, method_name, *args, node_count: int = 3,
                             tiers: list = None, priority: str = "normal", **kwargs):
        if method_name == 'block_number':
            return await self._get_block_number_dedup(tiers=tiers)

        if tiers is None:
            tiers = [1, 2]
        nodes = self._get_multiple_nodes(count=node_count, tiers=tiers)
        if not nodes:
            return await self.safe_call(method_name, *args, tiers=[0], priority=priority, **kwargs)

        async def _try_node(url, w3):
            self.nodes[url]["req_count"] += 1
            start_t = time.time()
            executor = self._get_executor_for_url(url)
            try:
                if method_name == 'call':
                    call_dict = args[0].copy() if args else kwargs.copy()

                    def _exec():
                        return w3.eth.call(call_dict)
                elif method_name == 'get_logs':
                    filter_dict = args[0].copy() if args else kwargs.copy()
                    if 'fromBlock' in filter_dict and isinstance(filter_dict['fromBlock'], int):
                        filter_dict['fromBlock'] = hex(filter_dict['fromBlock'])
                    if 'toBlock' in filter_dict and isinstance(filter_dict['toBlock'], int):
                        filter_dict['toBlock'] = hex(filter_dict['toBlock'])

                    def _exec():
                        return w3.eth.get_logs(filter_dict)
                else:
                    def _exec():
                        attr = getattr(w3.eth, method_name)
                        return attr(*args, **kwargs) if callable(attr) else attr

                # ★ v12.0: 超时兜底
                res = await asyncio.wait_for(
                    asyncio.get_running_loop().run_in_executor(executor, _exec),
                    timeout=RPC_EXECUTOR_TIMEOUT
                )
                latency = time.time() - start_t
                node = self.nodes[url]
                node["latency"] = node["latency"] * 0.7 + latency * 0.3
                node["success"] += 1
                self.circuit_breaker.record_success()
                return res
            except Exception:
                self.nodes[url]["fail"] += 1
                raise

        tasks = {asyncio.create_task(_try_node(url, w3)): url for url, w3 in nodes}
        try:
            done, pending = await asyncio.wait(
                tasks.keys(), timeout=RACE_TIMEOUT, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                if not task.exception():
                    for p in pending:
                        p.cancel()
                    return task.result()
            if pending:
                done2, pending2 = await asyncio.wait(
                    pending, timeout=RACE_TIMEOUT, return_when=asyncio.FIRST_COMPLETED
                )
                for task in done2:
                    if not task.exception():
                        for p in pending2:
                            p.cancel()
                        return task.result()
                for p in pending2:
                    p.cancel()
            return await self.safe_call(method_name, *args, tiers=[0], escalate_on_fail=True,
                                        priority=priority, **kwargs)
        except Exception:
            for t in tasks:
                t.cancel()
            return await self.safe_call(method_name, *args, tiers=[0], escalate_on_fail=True,
                                        priority=priority, **kwargs)

    # ================= 状态面板 =================

    def get_stats_text(self) -> str:
        now = time.time()
        
        # 计算各层统计
        def get_node_stats(url):
            stats = self.nodes.get(url, {})
            return {
                'url': url,
                'display': url.split("//")[-1].split("/")[0][:28],
                'latency': stats.get('latency', 0) * 1000,  # ms
                'req_count': stats.get('req_count', 0),
                'err_count': stats.get('err_count', 0),
                'cooldown_until': stats.get('cooldown_until', 0),
                'is_healthy': stats.get('cooldown_until', 0) < now,
                'last_used': stats.get('last_used', 0),
            }
        
        # T0 统计
        t0_nodes = [get_node_stats(u) for u in self.t0_urls]
        t0_healthy = sum(1 for n in t0_nodes if n['is_healthy'])
        t0_total_req = sum(n['req_count'] for n in t0_nodes)
        t0_total_err = sum(n['err_count'] for n in t0_nodes)
        t0_avg_lat = sum(n['latency'] for n in t0_nodes) / len(t0_nodes) if t0_nodes else 0
        
        # T1 统计
        t1_nodes = [get_node_stats(u) for u in self.t1_urls]
        t1_healthy = sum(1 for n in t1_nodes if n['is_healthy'])
        t1_cooling = [n for n in t1_nodes if not n['is_healthy']]
        t1_total_req = sum(n['req_count'] for n in t1_nodes)
        t1_total_err = sum(n['err_count'] for n in t1_nodes)
        t1_avg_lat = sum(n['latency'] for n in t1_nodes) / len(t1_nodes) if t1_nodes else 0
        
        # T2 统计（排除已踢出）
        t2_active_urls = [u for u in self.t2_urls if u not in self.t2_kicked]
        t2_nodes = [get_node_stats(u) for u in t2_active_urls]
        t2_healthy = sum(1 for n in t2_nodes if n['is_healthy'])
        t2_total_req = sum(n['req_count'] for n in t2_nodes)
        t2_total_err = sum(n['err_count'] for n in t2_nodes)
        t2_avg_lat = sum(n['latency'] for n in t2_nodes) / len(t2_nodes) if t2_nodes else 0
        
        # 已踢出节点统计
        t2_kicked_nodes = [get_node_stats(u) for u in self.t2_kicked]
        
        # 整体统计
        total_nodes = len(t0_nodes) + len(t1_nodes) + len(t2_nodes)
        total_healthy = t0_healthy + t1_healthy + t2_healthy
        total_req = t0_total_req + t1_total_req + t2_total_req
        total_err = t0_total_err + t1_total_err + t2_total_err
        success_rate = ((total_req - total_err) / total_req * 100) if total_req > 0 else 100
        
        bn_dedup_pct = (
            f"{self.block_number_dedup_hits / self.block_number_total_calls * 100:.0f}%"
            if self.block_number_total_calls > 0 else "N/A"
        )
        
        # 熔断器
        cb = self.circuit_breaker
        cb_status = "🟢 正常" if not cb._is_open else "🔴 熔断"
        
        lines = [
            f"📡 <b>RPC 舰队 v12.0</b>  |  {cb_status}",
            f"├ 总节点: {total_nodes} (健康 {total_healthy} / 冷却 {len(t1_cooling)} / 踢出 {len(self.t2_kicked)})",
            f"├ 总请求: {total_req:,}  |  成功率: {success_rate:.1f}%",
            f"├ 平均延迟: {((t0_avg_lat + t1_avg_lat + t2_avg_lat)/3):.0f}ms  |  去重节省: {bn_dedup_pct}",
            f"└ 熔断器: {cb_status}  |  连续失败 {cb._consecutive_all_fail}/{FLEET_BREAKER_THRESHOLD}  |  累计熔断 {cb._total_trips}次",
            "",
            f"<b>👑 T0 付费精锐</b> ({t0_healthy}/{len(t0_nodes)} 健康)",
        ]
        
        # T0 详情
        for n in t0_nodes:
            status = "🟢" if n['is_healthy'] else "🔴"
            err_rate = (n['err_count'] / n['req_count'] * 100) if n['req_count'] > 0 else 0
            lines.append(f"  {status} {n['display']:<28}")
            lines.append(f"     延迟: {n['latency']:.1f}ms  |  请求: {n['req_count']:,}  |  成功率: {100-err_rate:.1f}%")
        
        lines.append("")
        lines.append(f"<b>🛡️ T1 高质量免费</b> ({t1_healthy}/{len(t1_nodes)} 健康)")
        lines.append(f"  汇总: 平均延迟 {t1_avg_lat:.0f}ms  |  总请求 {t1_total_req:,}  |  异常率 {(t1_total_err/max(t1_total_req,1)*100):.1f}%")
        
        # T1 详情（前5个）
        t1_sorted = sorted(t1_nodes, key=lambda x: x['latency'] if x['is_healthy'] else 9999)
        for n in t1_sorted[:5]:
            status = "🟢" if n['is_healthy'] else "🔴"
            err_rate = (n['err_count'] / n['req_count'] * 100) if n['req_count'] > 0 else 0
            cooldown_str = ""
            if not n['is_healthy']:
                remain = int((n['cooldown_until'] - now) / 60)
                cooldown_str = f" [冷却 {remain}分]"
            lines.append(f"  {status} {n['display']:<26} {n['latency']:>5.0f}ms  {n['req_count']:>5}req  {100-err_rate:>5.1f}% {cooldown_str}")
        
        if len(t1_nodes) > 5:
            lines.append(f"  ... 及另外 {len(t1_nodes) - 5} 个节点")
        
        lines.append("")
        lines.append(f"<b>📦 T2 公益节点池</b> ({t2_healthy}/{len(t2_nodes)} 健康)")
        lines.append(f"  汇总: 平均延迟 {t2_avg_lat:.0f}ms  |  总请求 {t2_total_req:,}  |  异常率 {(t2_total_err/max(t2_total_req,1)*100):.1f}%")
        
        # T2 健康节点 TOP3
        t2_healthy_sorted = sorted([n for n in t2_nodes if n['is_healthy']], key=lambda x: x['latency'])[:3]
        if t2_healthy_sorted:
            lines.append(f"  健康节点 TOP3:")
            for i, n in enumerate(t2_healthy_sorted, 1):
                lines.append(f"    {i}. {n['display']:<24} {n['latency']:>5.0f}ms")
        
        # 已踢出节点
        if t2_kicked_nodes:
            lines.append(f"  已踢出节点: {len(t2_kicked_nodes)} 个")
            for n in t2_kicked_nodes[:3]:
                err_rate = (n['err_count'] / n['req_count'] * 100) if n['req_count'] > 0 else 100
                lines.append(f"    ❌ {n['display']:<24} 故障率 {err_rate:.0f}%")
            if len(t2_kicked_nodes) > 3:
                lines.append(f"    ... 及另外 {len(t2_kicked_nodes) - 3} 个")
        
        lines.append("")
        lines.append(f"<b>⚡ 实时性能指标</b>")
        lines.append(f"  get_logs 并发槽: {self.heavy_slots}  |  等待队列: {self.heavy_waits}  |  丢弃: {self.heavy_drops}")
        lines.append(f"  block_number 去重: {self.block_number_dedup_hits}/{self.block_number_total_calls} ({bn_dedup_pct})")
        lines.append(f"  线程池: T0={TIER0_POOL_SIZE} T1={TIER1_POOL_SIZE} T2={TIER2_POOL_SIZE}")
        
        return "\n".join(lines)