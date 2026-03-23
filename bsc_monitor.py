"""
bsc_monitor.py — 动量监控 + 池子状态 + 买家追踪
"""

import asyncio
import logging
from collections import OrderedDict
from web3 import Web3
from rpc_fleet import RpcFleet

logger = logging.getLogger("BSC_Monitor")

TRANSFER_TOPIC0 = Web3.keccak(text="Transfer(address,address,uint256)").hex()
FOURMEME_ROUTER = Web3.to_checksum_address("0x5c952063c7fc8610FFDB798152D69F0B9550762b")
ZERO_ADDRESS = Web3.to_checksum_address("0x0000000000000000000000000000000000000000")

INCREMENTAL_BLOCK_LAG = 2
VERIFY_RETRY_MAX = 3
INCREMENTAL_FETCH_INTERVAL = 4
MIN_NET_INFLOW_RATIO = 0.2

POOL_CACHE_MAX = 3000
PEAK_CACHE_MAX = 3000
SUPPLY_CACHE_MAX = 3000

POOL_CACHE_TTL_DEFAULT = 3.0
POOL_CACHE_TTL_HEARTBEAT = 8.0

FRAUD_BALANCE_DEVIATION_THRESHOLD = 0.20

# Four.meme 合约只读查询 selectors
TOKEN_INFO_SELECTOR = bytes.fromhex("e684626b")   # _tokenInfos(address)
BALANCE_OF_SELECTOR = bytes.fromhex("70a08231")   # balanceOf(address)


class LRUDict:
    def __init__(self, capacity: int):
        self._data = OrderedDict()
        self._capacity = capacity

    def get(self, key, default=None):
        if key in self._data:
            self._data.move_to_end(key)
            return self._data[key]
        return default

    def __contains__(self, key):
        return key in self._data

    def __setitem__(self, key, value):
        if key in self._data:
            self._data.move_to_end(key)
        self._data[key] = value
        if len(self._data) > self._capacity:
            self._data.popitem(last=False)

    def __getitem__(self, key):
        self._data.move_to_end(key)
        return self._data[key]

    def __len__(self):
        return len(self._data)

    def pop(self, key, default=None):
        return self._data.pop(key, default)


class IncrementalBalanceTracker:
    """增量余额跟踪器 — 含反欺骗能力"""

    def __init__(self, token_addr: str, total_supply: int):
        self.token_addr = Web3.to_checksum_address(token_addr)
        self.total_supply = total_supply
        self.balances = {}
        self.last_fetched_block = 0
        self.total_events = 0
        self.is_initialized = False
        self.total_buy_volume = 0
        self.total_sell_volume = 0
        self.large_sell_count = 0
        self.recent_sells = []
        self.dev_outflow = 0
        self._dev_addr = None
        # v14: 独立买家追踪 (供 Profiler 使用)
        self.unique_buyers = set()
        self.buyer_first_block = {}  # addr -> first_seen_block

    def set_dev_address(self, dev_addr: str):
        if dev_addr and dev_addr != ZERO_ADDRESS:
            self._dev_addr = Web3.to_checksum_address(dev_addr)

    def _safe_parse_amount(self, log_data) -> int:
        if not log_data:
            return 0
        try:
            if isinstance(log_data, bytes):
                raw = log_data
            elif isinstance(log_data, str):
                hex_str = log_data.replace('0x', '')
                raw = bytes.fromhex(hex_str)
            else:
                raw = bytes(log_data)
            if len(raw) == 0:
                return 0
            if len(raw) >= 32:
                return int.from_bytes(raw[:32], 'big')
            else:
                padded = raw.rjust(32, b'\x00')
                return int.from_bytes(padded, 'big')
        except Exception:
            return 0

    def update_from_logs(self, logs: list):
        if not logs:
            return
        exclude_addrs = {ZERO_ADDRESS, self.token_addr, FOURMEME_ROUTER}
        for log in logs:
            if len(log.get('topics', [])) != 3:
                continue
            try:
                from_addr = Web3.to_checksum_address("0x" + log['topics'][1].hex()[-40:])
                to_addr = Web3.to_checksum_address("0x" + log['topics'][2].hex()[-40:])
                amount = self._safe_parse_amount(log.get('data'))

                self.balances[from_addr] = self.balances.get(from_addr, 0) - amount
                self.balances[to_addr] = self.balances.get(to_addr, 0) + amount
                self.total_events += 1

                if from_addr in (FOURMEME_ROUTER, ZERO_ADDRESS) and to_addr not in exclude_addrs:
                    self.total_buy_volume += amount
                    # v14: 追踪独立买家
                    if to_addr not in self.unique_buyers:
                        self.unique_buyers.add(to_addr)
                        block_num = log.get('blockNumber', 0)
                        if isinstance(block_num, str):
                            block_num = int(block_num, 16) if block_num.startswith('0x') else int(block_num)
                        self.buyer_first_block[to_addr] = block_num

                elif to_addr == FOURMEME_ROUTER and from_addr not in (ZERO_ADDRESS, self.token_addr, FOURMEME_ROUTER):
                    self.total_sell_volume += amount
                    if self.total_supply > 0 and amount > self.total_supply * 0.01:
                        self.large_sell_count += 1
                        block_num = log.get('blockNumber', 0)
                        self.recent_sells.append((from_addr, amount, block_num))
                        if len(self.recent_sells) > 20:
                            self.recent_sells = self.recent_sells[-20:]

                if self._dev_addr and from_addr == self._dev_addr:
                    self.dev_outflow += amount
            except Exception:
                continue

    def get_router_balance_estimate(self) -> int:
        return self.balances.get(FOURMEME_ROUTER, 0)

    def get_stats(self) -> tuple:
        exclude_addrs = {ZERO_ADDRESS, self.token_addr, FOURMEME_ROUTER}
        valid = {addr: bal for addr, bal in self.balances.items() if addr not in exclude_addrs and bal > 0}
        max_pct = 0.0
        if valid and self.total_supply > 0:
            max_bal = max(valid.values())
            max_pct = (max_bal / self.total_supply) * 100.0
        return len(valid), max_pct

    def get_sell_pressure_info(
        self,
        sell_ratio_threshold: float = 40.0,
        dev_sold_pct_threshold: float = 5.0,
        large_sell_count_threshold: int = 2,
        min_volume_for_check: int = 0,
    ) -> dict:
        total_volume = self.total_buy_volume + self.total_sell_volume
        sell_ratio = (self.total_sell_volume / total_volume * 100.0) if total_volume > 0 else 0.0
        dev_sold_pct = (self.dev_outflow / self.total_supply * 100.0) if self.total_supply > 0 and self._dev_addr else 0.0

        threshold_active = total_volume >= max(0, int(min_volume_for_check or 0))
        reasons = []
        if threshold_active:
            if sell_ratio > float(sell_ratio_threshold):
                reasons.append(f"卖压占比{sell_ratio:.1f}%>{float(sell_ratio_threshold):.1f}%")
            if dev_sold_pct > float(dev_sold_pct_threshold):
                reasons.append(f"Dev净流出{dev_sold_pct:.2f}%>{float(dev_sold_pct_threshold):.2f}%")
            if self.large_sell_count >= int(large_sell_count_threshold):
                reasons.append(f"大额卖出次数{self.large_sell_count}>={int(large_sell_count_threshold)}")

        is_suspicious = len(reasons) > 0
        return {
            "sell_ratio": sell_ratio,
            "large_sells": self.large_sell_count,
            "dev_sold_pct": dev_sold_pct,
            "is_suspicious": is_suspicious,
            "reason": "；".join(reasons),
            "threshold_active": threshold_active,
            "buy_volume": self.total_buy_volume,
            "sell_volume": self.total_sell_volume,
            "total_volume": total_volume,
        }

    def get_top_holders(self, n: int = 5) -> list:
        exclude_addrs = {ZERO_ADDRESS, self.token_addr, FOURMEME_ROUTER}
        valid = {addr: bal for addr, bal in self.balances.items() if addr not in exclude_addrs and bal > 0}
        sorted_holders = sorted(valid.items(), key=lambda x: x[1], reverse=True)[:n]
        result = []
        for addr, bal in sorted_holders:
            pct = (bal / self.total_supply * 100.0) if self.total_supply > 0 else 0
            result.append((addr, pct))
        return result


class MomentumMonitor:
    def __init__(self, fleet: RpcFleet):
        self.fleet = fleet
        self.w3 = fleet.default_w3
        self.pool_cache = LRUDict(POOL_CACHE_MAX)
        self.initial_supply_cache = LRUDict(SUPPLY_CACHE_MAX)
        self.peak_pool_cache = LRUDict(PEAK_CACHE_MAX)
        self.curve_meta_cache = LRUDict(SUPPLY_CACHE_MAX)  # token -> onchain curve meta

        self.total_monitors = 0
        self.total_confirmed = 0
        self.total_timeout = 0
        self.total_rat_blocked = 0
        self.total_verify_retries = 0
        self.total_incremental_hits = 0
        self.total_chunked_fallbacks = 0
        self.total_sell_pressure_blocked = 0
        self.total_fraud_blocked = 0
        self.pool_cache_hits = 0
        self.pool_cache_misses = 0

    async def get_pool_state(self, token_addr: str, ttl: float = POOL_CACHE_TTL_DEFAULT, graduate_bnb: float = 24.0) -> tuple:
        """
        返回 (raised_bnb, router_token_balance)

        优先使用 Four.meme 合约原生 _tokenInfos：
        - raised_bnb = funds
        - router_balance ≈ lp_reserve + offers

        若原生查询失败，再回落到旧的 Router 余额线性估算。
        """
        token_chk = Web3.to_checksum_address(token_addr)
        key = f"pool_{token_chk}"
        now = asyncio.get_event_loop().time()

        cached = self.pool_cache.get(key)
        if cached is not None and now - cached[0] < ttl:
            self.pool_cache_hits += 1
            return cached[1]

        self.pool_cache_misses += 1

        try:
            # ===== 优先: Four.meme 原生 tokenInfo =====
            info_data = TOKEN_INFO_SELECTOR + bytes.fromhex(token_chk[2:].zfill(64))
            info_raw = await self.fleet.safe_call('call', {'to': FOURMEME_ROUTER, 'data': info_data}, tiers=[1, 2])

            if info_raw and len(info_raw) >= 32 * 13:
                words = [int.from_bytes(bytes(info_raw)[i:i + 32], 'big') for i in range(0, 32 * 13, 32)]

                total_supply = words[3]
                max_offers = words[4]
                max_raising_wei = words[5]
                launch_time = words[6]
                offers = words[7]
                funds_wei = words[8]
                last_price_wei = words[9]
                status = words[12] if words[12] <= 10 else (words[2] if words[2] <= 10 else 0)

                # quote: 0x0 代表原生 BNB；非 0x0 常见为稳定币计价模板
                quote_addr = Web3.to_checksum_address(f"0x{words[1] & ((1 << 160) - 1):040x}")
                quote_is_native = (quote_addr.lower() == ZERO_ADDRESS.lower())

                sane = (
                    total_supply > 0 and
                    max_offers > 0 and
                    max_raising_wei > 0 and
                    max_offers <= total_supply
                )

                if sane:
                    lp_reserve = max(total_supply - max_offers, 0)
                    router_balance = lp_reserve + max(offers, 0)
                    raised_bnb = float(funds_wei) / 1e18

                    self.initial_supply_cache[token_chk] = total_supply
                    self.curve_meta_cache[token_chk] = {
                        "total_supply": total_supply,
                        "max_offers": max_offers,
                        "max_raising_wei": max_raising_wei,
                        "offers": offers,
                        "funds_wei": funds_wei,
                        "last_price_wei": last_price_wei,
                        "status": status,
                        "launch_time": launch_time,
                        "quote_addr": quote_addr,
                        "quote_is_native": quote_is_native,
                    }

                    peak_key = f"peak_{token_chk}"
                    cur_peak = self.peak_pool_cache.get(peak_key, 0.0)
                    if raised_bnb > cur_peak:
                        self.peak_pool_cache[peak_key] = raised_bnb

                    self.pool_cache[key] = (now, (raised_bnb, int(router_balance)))
                    return raised_bnb, int(router_balance)

            # ===== 回落: 旧口径线性估算 =====
            if token_chk not in self.initial_supply_cache:
                ts_data = bytes.fromhex("18160ddd")
                ts_raw = await self.fleet.safe_call('call', {'to': token_chk, 'data': ts_data}, tiers=[2, 1])
                if ts_raw is None:
                    return -1.0, -1
                ts = int.from_bytes(ts_raw, 'big') if ts_raw else 1_000_000_000 * 10 ** 18
                self.initial_supply_cache[token_chk] = ts if ts > 0 else 1_000_000_000 * 10 ** 18

            t_0 = self.initial_supply_cache[token_chk]

            token_data = BALANCE_OF_SELECTOR + bytes.fromhex(FOURMEME_ROUTER[2:].zfill(64))
            token_bal_raw = await self.fleet.safe_call('call', {'to': token_chk, 'data': token_data}, tiers=[2, 1])
            if token_bal_raw is None:
                return -1.0, -1

            cur_tokens = int.from_bytes(token_bal_raw, 'big') if token_bal_raw else 0

            if cur_tokens == 0:
                self.pool_cache[key] = (now, (0.0, 0))
                return 0.0, 0

            lp_reserve = int(t_0 * 0.2)
            saleable_tokens = int(t_0 * 0.8)

            left_tokens = max(0, cur_tokens - lp_reserve)
            tokens_sold = saleable_tokens - left_tokens

            if tokens_sold <= 0:
                est_bnb = 0.0
            else:
                progress = float(tokens_sold) / float(saleable_tokens)
                est_bnb = progress * graduate_bnb

            peak_key = f"peak_{token_chk}"
            cur_peak = self.peak_pool_cache.get(peak_key, 0.0)
            if est_bnb > cur_peak:
                self.peak_pool_cache[peak_key] = est_bnb

            self.pool_cache[key] = (now, (est_bnb, cur_tokens))
            return est_bnb, cur_tokens

        except Exception as e:
            logger.debug(f"⚠️ [Monitor] 池子查询异常: {e}")
            return -1.0, -1

    def get_curve_meta(self, token_addr: str) -> dict:
        token_chk = Web3.to_checksum_address(token_addr)
        return self.curve_meta_cache.get(token_chk, {}) or {}

    def get_graduate_bnb(self, token_addr: str, default: float = 24.0) -> float:
        token_chk = Web3.to_checksum_address(token_addr)
        meta = self.curve_meta_cache.get(token_chk, {}) or {}
        max_raising_wei = int(meta.get("max_raising_wei", 0) or 0)
        if max_raising_wei > 0:
            return float(max_raising_wei) / 1e18
        return float(default)

    def invalidate_pool_cache(self, token_addr: str):
        token_chk = Web3.to_checksum_address(token_addr)
        self.pool_cache.pop(f"pool_{token_chk}", None)
        self.curve_meta_cache.pop(token_chk, None)

    def get_peak_pool_bnb(self, token_addr: str) -> float:
        token_chk = Web3.to_checksum_address(token_addr)
        return self.peak_pool_cache.get(f"peak_{token_chk}", 0.0)

    def set_peak_pool_bnb(self, token_addr: str, peak_bnb: float):
        token_chk = Web3.to_checksum_address(token_addr)
        peak_key = f"peak_{token_chk}"
        cur = self.peak_pool_cache.get(peak_key, 0.0)
        if peak_bnb > cur:
            self.peak_pool_cache[peak_key] = peak_bnb

    def get_initial_supply(self, token_addr: str) -> int:
        """v14: 获取缓存的 total_supply，供市值计算"""
        token_chk = Web3.to_checksum_address(token_addr)
        return self.initial_supply_cache.get(token_chk, 0)

    def get_bonding_curve_info(self, token_addr: str, pool_bnb: float = 0,
                                pool_tokens: int = 0, graduate_bnb: float = 24.0) -> dict:
        """
        Four.meme bonding curve 状态。

        优先使用合约原生字段：funds/maxRaising/lastPrice/totalSupply。
        若缺失则回退旧口径估算。
        """
        token_chk = Web3.to_checksum_address(token_addr)
        meta = self.curve_meta_cache.get(token_chk, {}) or {}

        # ===== 原生口径 =====
        if meta:
            total_supply = int(meta.get("total_supply", 0) or 0)
            max_offers = int(meta.get("max_offers", 0) or 0)
            offers = int(meta.get("offers", 0) or 0)
            funds_wei = int(meta.get("funds_wei", 0) or 0)
            max_raising_wei = int(meta.get("max_raising_wei", 0) or 0)
            last_price_wei = int(meta.get("last_price_wei", 0) or 0)
            status = int(meta.get("status", 0) or 0)

            lp_reserve = max(total_supply - max_offers, 0) if total_supply > 0 and max_offers > 0 else int(total_supply * 0.2)
            saleable = max_offers if max_offers > 0 else int(total_supply * 0.8)
            left_tokens = max(offers, 0)
            tokens_sold = max(saleable - left_tokens, 0)

            progress = (float(funds_wei) / float(max_raising_wei) * 100.0) if max_raising_wei > 0 else 0.0
            est_bnb = float(funds_wei) / 1e18

            if last_price_wei > 0 and total_supply > 0:
                mc_bnb = (float(last_price_wei) * float(total_supply)) / 1e36
                price_per_token = float(last_price_wei) / 1e18
            else:
                # 回退到旧的 x5 近似
                mc_bnb = est_bnb * 5.0
                price_per_token = mc_bnb / (float(total_supply) / (10**18)) if total_supply > 0 else 0

            return {
                "progress": round(max(0.0, min(100.0, progress)), 2),
                "tokens_sold": tokens_sold,
                "left_tokens": left_tokens,
                "est_bnb_raised": round(est_bnb, 4),
                "mc_bnb": round(mc_bnb, 4),
                "price_per_token_bnb": price_per_token,
                "status": status,
                "graduate_bnb": round(float(max_raising_wei) / 1e18, 6) if max_raising_wei > 0 else float(graduate_bnb),
                "source": "onchain_token_info",
            }

        # ===== 回退口径 =====
        total_supply = self.initial_supply_cache.get(token_chk, 1_000_000_000 * 10**18)

        lp_reserve = int(total_supply * 0.2)
        saleable = int(total_supply * 0.8)

        if pool_tokens <= 0:
            cached = self.pool_cache.get(f"pool_{token_chk}")
            if cached:
                pool_bnb, pool_tokens = cached[1]

        left_tokens = max(0, pool_tokens - lp_reserve) if pool_tokens > 0 else saleable
        tokens_sold = saleable - left_tokens

        progress = (float(tokens_sold) / float(saleable) * 100.0) if saleable > 0 else 0.0
        est_bnb = (float(tokens_sold) / float(saleable)) * graduate_bnb if saleable > 0 else 0.0

        if pool_bnb > 0:
            mc_bnb = pool_bnb * float(total_supply) / float(lp_reserve) if lp_reserve > 0 else 0
        else:
            mc_bnb = est_bnb * float(total_supply) / float(lp_reserve) if lp_reserve > 0 else 0

        price_per_token = mc_bnb / (float(total_supply) / (10**18)) if total_supply > 0 else 0

        return {
            "progress": round(progress, 2),
            "tokens_sold": tokens_sold,
            "left_tokens": left_tokens,
            "est_bnb_raised": round(est_bnb if pool_bnb <= 0 else pool_bnb, 4),
            "mc_bnb": round(mc_bnb, 4),
            "price_per_token_bnb": price_per_token,
            "status": 0,
            "graduate_bnb": float(graduate_bnb),
            "source": "fallback_estimation",
        }

    # ================= v11: 反欺骗验证 =================

    async def verify_balance_authenticity(self, token_addr: str,
                                           tracker: IncrementalBalanceTracker) -> tuple:
        if not tracker.is_initialized or tracker.total_events < 5:
            return True, 0.0, "事件不足，跳过验证"

        token_chk = Web3.to_checksum_address(token_addr)
        balance_data = bytes.fromhex("70a08231") + bytes.fromhex(FOURMEME_ROUTER[2:].zfill(64))
        balance_raw = await self.fleet.safe_call('call', {'to': token_chk, 'data': balance_data}, tiers=[2, 1])
        if balance_raw is None:
            return True, 0.0, "查询失败，跳过验证"

        chain_balance = int.from_bytes(balance_raw, 'big') if balance_raw else 0
        event_balance = tracker.get_router_balance_estimate()

        if event_balance <= 0 and chain_balance <= 0:
            return True, 0.0, "两者均为0"
        if event_balance <= 0:
            return True, 0.0, "事件追踪不完整"

        deviation = abs(chain_balance - event_balance) / max(event_balance, 1)

        if deviation > FRAUD_BALANCE_DEVIATION_THRESHOLD:
            self.total_fraud_blocked += 1
            return False, deviation, f"偏差{deviation * 100:.1f}%"

        return True, deviation, f"偏差{deviation * 100:.1f}% (可信)"

    # ================= 活人检测 =================

    async def get_buyer_stats_chunked(self, token_addr, from_block, to_block, total_supply):
        try:
            token_chk = Web3.to_checksum_address(token_addr)
            logs = await self.fleet.safe_call_chunked_logs({
                "fromBlock": from_block, "toBlock": to_block,
                "address": token_chk, "topics": [TRANSFER_TOPIC0]
            }, priority="high", tiers=[1, 2])
            if logs is None:
                return None, None
            if not logs:
                return 0, 0.0
            tracker = IncrementalBalanceTracker(token_addr, total_supply)
            tracker.update_from_logs(logs)
            return tracker.get_stats()
        except Exception as e:
            logger.warning(f"⚠️ [Monitor] 分片活人检测异常: {e}")
            return None, None

    # ================= 动量监控 (v14: on_confirmed 带买家数据) =================

    async def monitor_token(
        self, token_addr: str, on_confirmed: callable,
        duration: int, up_ticks_req: int, mom_thr: float,
        min_buyers: int, max_dev_pct: float, graduate_thr: float,
        creator_addr: str = None,
        confirm_delay_sec: float = 5.0,
        confirm_pullback_bnb: float = 0.05,
        sell_ratio_threshold: float = 40.0,
        dev_sold_pct_threshold: float = 5.0,
        large_sell_count_threshold: int = 2,
        sell_pressure_min_volume: int = 0,
    ):
        """
        v14: on_confirmed 签名变更:
          on_confirmed(addr, pool_bnb, pool_tok, buyer_addrs, buyer_blocks)
          buyer_addrs: set — 独立买家地址
          buyer_blocks: dict — {addr: first_seen_block}
        """
        self.total_monitors += 1
        monitor_id = self.total_monitors
        min_net_inflow = mom_thr * MIN_NET_INFLOW_RATIO

        logger.info(f"🎯 [Monitor] ═══ 动量监控 #{monitor_id} 启动: {token_addr[:10]}... ═══")

        start_time = asyncio.get_event_loop().time()
        start_block = await self.fleet.safe_call_race('block_number', node_count=2, tiers=[1, 2])
        if start_block is None:
            start_block = await self.fleet.safe_call('block_number', tiers=[2, 1])
        if start_block is None:
            return {"ok": False, "reason": "获取起始区块失败"}

        initial_bnb, _ = await self.get_pool_state(token_addr)
        if initial_bnb < 0:
            return {"ok": False, "reason": "读取池子状态失败"}

        last_bnb, net_inflow, up_ticks = initial_bnb, 0.0, 0
        heartbeat = 0
        pre_inflow = initial_bnb
        last_risk_check_time = 0
        sample_count = 0

        t_0 = self.initial_supply_cache.get(
            Web3.to_checksum_address(token_addr), 1_000_000_000 * 10 ** 18
        )
        tracker = IncrementalBalanceTracker(token_addr, t_0)
        tracker.last_fetched_block = start_block
        last_incremental_block = start_block

        if creator_addr:
            tracker.set_dev_address(creator_addr)

        pending_verify = False
        verify_retry_count = 0
        verify_cooldown_until = 0
        latest_buyers = 0
        latest_max_pct = 0.0

        while asyncio.get_event_loop().time() - start_time < duration:
            sample_count += 1
            current_bnb, cur_tokens = await self.get_pool_state(token_addr)

            if current_bnb < 0:
                await asyncio.sleep(2.5)
                continue

            delta = current_bnb - last_bnb
            elapsed = int(asyncio.get_event_loop().time() - start_time)
            remaining = duration - elapsed

            should_fetch_incremental = (sample_count % INCREMENTAL_FETCH_INTERVAL == 0)
            if net_inflow >= min_net_inflow * 0.7:
                should_fetch_incremental = True

            if should_fetch_incremental:
                try:
                    current_block = await self.fleet.safe_call('block_number', tiers=[2, 1])
                    if current_block and current_block > last_incremental_block:
                        inc_from = max(last_incremental_block + 1, current_block - INCREMENTAL_BLOCK_LAG)
                        inc_logs = await self.fleet.safe_call('get_logs', {
                            "fromBlock": inc_from, "toBlock": current_block,
                            "address": Web3.to_checksum_address(token_addr),
                            "topics": [TRANSFER_TOPIC0]
                        }, tiers=[2, 1], priority="normal", source="Monitor.incremental")
                        if inc_logs is not None:
                            if inc_logs:
                                tracker.update_from_logs(inc_logs)
                            tracker.last_fetched_block = current_block
                            last_incremental_block = current_block
                            if not tracker.is_initialized:
                                tracker.is_initialized = True
                except Exception:
                    pass

            if delta > 0.001:
                net_inflow += delta
                up_ticks += 1
            elif delta < -0.001:
                up_ticks = 0
            else:
                heartbeat += 1
                if heartbeat % 8 == 0:
                    logger.info(
                        f"⏳ [Monitor] #{monitor_id} {token_addr[:8]} | "
                        f"增量:{net_inflow:.4f} | tick:{up_ticks} | "
                        f"买家:{len(tracker.unique_buyers)} | {remaining}s"
                    )

            last_bnb = current_bnb
            total_momentum = pre_inflow + net_inflow

            momentum_ok = (
                net_inflow >= min_net_inflow and
                total_momentum >= mom_thr and
                (up_ticks >= up_ticks_req or (pre_inflow >= mom_thr and up_ticks >= 1))
            )

            should_verify = False
            now_ts = asyncio.get_event_loop().time()

            if momentum_ok and not pending_verify:
                if now_ts - last_risk_check_time > 10.0:
                    pre_confirm_bnb = current_bnb
                    await asyncio.sleep(confirm_delay_sec)
                    post_confirm_bnb, _ = await self.get_pool_state(token_addr)
                    if post_confirm_bnb < 0:
                        last_risk_check_time = now_ts
                        await asyncio.sleep(2.5)
                        continue
                    confirm_delta = post_confirm_bnb - pre_confirm_bnb
                    if confirm_delta < -confirm_pullback_bnb:
                        last_risk_check_time = now_ts
                        await asyncio.sleep(2.5)
                        continue
                    current_bnb = post_confirm_bnb
                    should_verify = True
                    last_risk_check_time = now_ts
            elif pending_verify:
                if now_ts > verify_cooldown_until and verify_retry_count < VERIFY_RETRY_MAX:
                    should_verify = True

            if should_verify:
                # 反欺骗
                is_authentic, deviation, fraud_detail = await self.verify_balance_authenticity(token_addr, tracker)
                if not is_authentic:
                    logger.error(f"🚨 [Monitor] #{monitor_id} {token_addr[:8]} 反欺骗拦截! {fraud_detail}")
                    return {"ok": False, "reason": f"反欺骗拦截: {fraud_detail}"}

                # 活人检测
                buyers = None
                max_pct = None

                if tracker.is_initialized and tracker.total_events > 0:
                    buyers, max_pct = tracker.get_stats()
                    self.total_incremental_hits += 1

                if buyers is None:
                    self.total_chunked_fallbacks += 1
                    cb = await self.fleet.safe_call_race('block_number', node_count=2, tiers=[1, 2])
                    if cb is None:
                        cb = await self.fleet.safe_call('block_number', tiers=[2, 1], escalate_on_fail=True)
                    if cb is not None:
                        buyers, max_pct = await self.get_buyer_stats_chunked(token_addr, start_block, cb, t_0)

                if buyers is None:
                    verify_retry_count += 1
                    self.total_verify_retries += 1
                    verify_cooldown_until = now_ts + 3.0
                    if verify_retry_count >= VERIFY_RETRY_MAX:
                        pending_verify = False
                        if tracker.total_events > 0:
                            buyers, max_pct = tracker.get_stats()
                        else:
                            return {"ok": False, "reason": "验资数据不足(链上日志获取失败)"}
                    else:
                        pending_verify = True
                        await asyncio.sleep(2.5)
                        continue

                pending_verify = False
                verify_retry_count = 0

                logger.info(
                    f"🕵️ [Monitor] #{monitor_id} {token_addr[:8]} 验资: "
                    f"买家={buyers}/{min_buyers} | 控盘={max_pct:.1f}%/{max_dev_pct}%"
                )

                latest_buyers = int(buyers or 0)
                latest_max_pct = float(max_pct or 0.0)

                if max_pct >= max_dev_pct:
                    self.total_rat_blocked += 1
                    return {"ok": False, "reason": f"控盘超限({max_pct:.1f}%≥{max_dev_pct:.1f}%)"}

                if tracker.total_events > 0:
                    sell_info = tracker.get_sell_pressure_info(
                        sell_ratio_threshold=sell_ratio_threshold,
                        dev_sold_pct_threshold=dev_sold_pct_threshold,
                        large_sell_count_threshold=large_sell_count_threshold,
                        min_volume_for_check=sell_pressure_min_volume,
                    )
                    if sell_info['is_suspicious']:
                        self.total_sell_pressure_blocked += 1
                        detail = sell_info.get('reason') or "卖压过高"
                        logger.warning(f"🚨 [Monitor] #{monitor_id} {token_addr[:8]} 卖压拦截: {detail}")
                        return {"ok": False, "reason": f"卖压拦截: {detail}"}

                if buyers >= min_buyers:
                    self.total_confirmed += 1
                    logger.info(f"🚀 [Monitor] #{monitor_id} {token_addr[:8]} 风控通过! 买家={buyers}")
                    # ★ v14: 传递买家数据给主调方
                    await on_confirmed(
                        token_addr, current_bnb, cur_tokens,
                        tracker.unique_buyers.copy(),
                        dict(tracker.buyer_first_block),
                    )
                    return {"ok": True, "reason": "通过"}

            await asyncio.sleep(2.5)

        self.total_timeout += 1

        unmet = []
        total_momentum = pre_inflow + net_inflow
        if net_inflow < min_net_inflow:
            unmet.append(f"净流入不足({net_inflow:.3f}<{min_net_inflow:.3f} BNB)")
        if total_momentum < mom_thr:
            unmet.append(f"总动量不足({total_momentum:.3f}<{mom_thr:.3f} BNB)")
        if not (up_ticks >= up_ticks_req or (pre_inflow >= mom_thr and up_ticks >= 1)):
            unmet.append(f"上涨tick不足({up_ticks}<{up_ticks_req})")
        if latest_buyers < min_buyers:
            unmet.append(f"独立买家不足({latest_buyers}<{min_buyers})")

        if not unmet:
            unmet.append("综合条件未满足")

        return {"ok": False, "reason": "；".join(unmet)}

    def get_status_text(self) -> str:
        total_pool_queries = self.pool_cache_hits + self.pool_cache_misses
        hit_rate = f"{self.pool_cache_hits / total_pool_queries * 100:.0f}%" if total_pool_queries > 0 else "N/A"

        return (
            f"📡 <b>动量监控 v14</b>\n"
            f"总监控: {self.total_monitors}\n"
            f"通过: {self.total_confirmed} | 超时: {self.total_timeout}\n"
            f"老鼠仓: {self.total_rat_blocked} | 卖压: {self.total_sell_pressure_blocked} | 反欺骗: {self.total_fraud_blocked}\n"
            f"增量命中: {self.total_incremental_hits} | 分片兜底: {self.total_chunked_fallbacks}\n"
            f"池子缓存: {hit_rate} ({self.pool_cache_hits}/{total_pool_queries})"
        )
