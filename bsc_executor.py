"""
bsc_executor.py — BSC 链上交易执行，RBF 重发，Nonce 管理
"""

import asyncio
import os
import logging
import time
import eth_abi
from typing import Optional, Tuple

from web3 import Web3
from rpc_fleet import RpcFleet, LRUCache, CACHE_MAX_DECIMALS, CACHE_MAX_NAME_SYM
from nonce_manager import NonceManager

logger = logging.getLogger("BSC_Executor")

# ================= 合约常量 =================
FOURMEME_ROUTER = Web3.to_checksum_address("0x5c952063c7fc8610FFDB798152D69F0B9550762b")
BUY_SELECTOR = "0x7f79f6df"
SELL_SELECTOR = "0xe564bedd"
APPROVE_SELECTOR = "0x095ea7b3"

# PancakeSwap V2
PANCAKE_ROUTER_V2 = Web3.to_checksum_address("0x10ED43C718714eb63d5aA57B78B54704E256024E")
PANCAKE_FACTORY_V2 = Web3.to_checksum_address("0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73")
WBNB_ADDRESS = Web3.to_checksum_address("0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c")

# PancakeSwap selectors
PANCAKE_SELL_SELECTOR = "0x791ac947"
GET_PAIR_SELECTOR = "0xe6a43905"
GET_RESERVES_SELECTOR = "0x0902f1ac"
TOKEN0_SELECTOR = "0x0dfe1681"

# v11.1: Gas 策略 — 动态百分位 + 兜底倍率
GAS_MULT_APPROVE = 1.1
GAS_MULT_BUY = 1.3
GAS_MULT_SELL = 1.5
GAS_MULT_EMERGENCY = 2.0

# v11.1: MEV 底线滑点
MEV_SLIPPAGE_FLOOR = 0.50

# v11.1: RBF 参数
RBF_CHECK_INTERVAL = 5
RBF_GAS_BUMP_PCT = 0.15
RBF_MAX_RETRIES = 3
RBF_TOTAL_TIMEOUT = 45

MAX_UINT256 = (1 << 256) - 1

# v11.5: fee_history 缓存
FEE_HISTORY_CACHE_TTL = 8.0  # 从 5s → 8s


class BSCExecutor:
    """
    BSC 链上执行器 v11.5

    重要: buy()/sell()/sell_on_pancakeswap() 使用 RBF 模式
    返回 tx_hash 即表示已上链成功 (内部已等待确认)
    返回 None 表示失败
    调用方不应再调 wait_tx() (会覆盖 last_gas_cost_bnb)
    """

    def __init__(self, private_key: str, fleet: RpcFleet = None):
        self.fleet = fleet if fleet else RpcFleet()
        self.w3 = self.fleet.default_w3
        self.account = self.w3.eth.account.from_key(private_key)
        self.address = self.account.address
        self.router = FOURMEME_ROUTER
        # 模拟盘标志 (由外部显式设置, 默认跟随环境变量)
        self.dry_run = os.getenv('DRY_RUN', 'false').lower() == 'true'
        self.pancake_router = PANCAKE_ROUTER_V2
        self.pancake_factory = PANCAKE_FACTORY_V2

        # Nonce 管理器
        self.nonce_mgr = NonceManager(self.fleet, self.address)

        # Approve 缓存 — 分内盘/外盘
        self.approved_fourmeme = set()
        self.approved_pancake = set()

        # LRU 缓存
        self._cache_name_sym = LRUCache(CACHE_MAX_NAME_SYM)
        self._cache_decimals = LRUCache(CACHE_MAX_DECIMALS)
        self.last_gas_cost_bnb = 0.0

        # v11.5: 动态 Gas 缓存 (TTL 延长到 8s)
        self._cached_gas_p90 = None
        self._cached_gas_p90_time = 0

        # 统计
        self.total_buys = 0
        self.total_sells = 0
        self.total_pancake_sells = 0
        self.total_buy_failures = 0
        self.total_sell_failures = 0
        self.total_simulations = 0
        self.total_sim_reverts = 0
        self.total_rbf_bumps = 0

        logger.info(f"🔑 [Executor] 钱包: {self.address}")
        logger.info(f"🔑 [Executor] 内盘Router: {FOURMEME_ROUTER}")
        logger.info(f"🔑 [Executor] 外盘Router: {PANCAKE_ROUTER_V2}")
        logger.info(f"🔑 [Executor] MEV底线滑点: {MEV_SLIPPAGE_FLOOR*100:.0f}%")

    async def initialize(self):
        await self.nonce_mgr.initialize()

    def get_fleet_status(self) -> str:
        return self.fleet.get_stats_text()

    # ================= v11.5: 动态 Gas 百分位 (TTL 延长) =================

    async def _get_dynamic_gas_price(self, gas_mult: float) -> Optional[int]:
        now = time.time()

        # v11.5: TTL 从 5s → 8s
        if self._cached_gas_p90 and now - self._cached_gas_p90_time < FEE_HISTORY_CACHE_TTL:
            adjusted_mult = 1.0 + (gas_mult - 1.0) * 0.5
            return int(self._cached_gas_p90 * adjusted_mult)

        try:
            def _fetch_fee_history():
                return self.w3.eth.fee_history(5, 'latest', [90])

            fee_history = await asyncio.wait_for(
                asyncio.get_running_loop().run_in_executor(
                    self.fleet._executor_t0,  # v11.5: 使用 T0 专属线程池
                    _fetch_fee_history
                ),
                timeout=3.0
            )

            if fee_history and 'reward' in fee_history:
                p90_values = [r[0] for r in fee_history['reward'] if r and r[0] > 0]
                if p90_values:
                    p90_gas = max(p90_values)
                    self._cached_gas_p90 = p90_gas
                    self._cached_gas_p90_time = now
                    adjusted_mult = 1.0 + (gas_mult - 1.0) * 0.5
                    result = int(p90_gas * adjusted_mult)
                    logger.debug(f"⛽ [Gas] P90={self.w3.from_wei(p90_gas, 'gwei'):.2f} Gwei × {adjusted_mult:.2f}")
                    return result
        except Exception:
            pass

        gas_price = await self.fleet.safe_call('gas_price', tiers=[0])
        if gas_price is None:
            return None
        return int(gas_price * gas_mult)

    # ================= decimals (三重解析 + LRU) =================

    async def get_token_decimals(self, token_addr: str) -> int:
        token_chk = Web3.to_checksum_address(token_addr)
        if token_chk in self._cache_decimals:
            return self._cache_decimals[token_chk]

        try:
            dec_raw = await self.fleet.safe_call(
                'call', {'to': token_chk, 'data': "0x313ce567"}, tiers=[2, 1]
            )
            if not dec_raw or len(dec_raw) < 32:
                return 18

            decimals = None
            try:
                raw_bytes = bytes(dec_raw) if not isinstance(dec_raw, bytes) else dec_raw
                decoded = eth_abi.decode(['uint8'], raw_bytes[-32:])
                decimals = decoded[0]
            except Exception:
                pass

            if decimals is None:
                try:
                    raw_bytes = bytes(dec_raw) if not isinstance(dec_raw, bytes) else dec_raw
                    val = int.from_bytes(raw_bytes[-32:], 'big')
                    if 0 <= val <= 18:
                        decimals = val
                except Exception:
                    pass

            if decimals is None:
                try:
                    hex_str = dec_raw.hex() if hasattr(dec_raw, 'hex') else str(dec_raw)
                    if hex_str.startswith('0x'):
                        hex_str = hex_str[2:]
                    val = int(hex_str, 16)
                    if 0 <= val <= 18:
                        decimals = val
                except Exception:
                    pass

            if decimals is not None and 0 <= decimals <= 18:
                self._cache_decimals[token_chk] = decimals
                return decimals
            return 18

        except Exception:
            return 18

    @staticmethod
    def format_token_amount(raw_amount: int, decimals: int = 18) -> str:
        if raw_amount <= 0:
            return "0"
        if decimals < 0 or decimals > 18:
            decimals = 18
        if decimals == 0 and raw_amount > 10 ** 15:
            decimals = 18
        if decimals == 0:
            return f"{raw_amount:,}"
        divisor = 10 ** decimals
        whole = raw_amount // divisor
        frac = raw_amount % divisor
        frac_str = str(frac).zfill(decimals)
        if whole >= 1_000_000:
            return f"{whole:,}.{frac_str[:2]}"
        elif whole >= 1_000:
            return f"{whole:,}.{frac_str[:4]}"
        elif whole > 0:
            return f"{whole:,}.{frac_str[:6]}"
        else:
            display = f"0.{frac_str}".rstrip('0')
            if display == "0.":
                display = "0"
            return display

    # ================= AMM 计算 =================

    def calc_min_tokens_out(self, bnb_in_wei: int, pool_bnb_wei: int, pool_tokens: int) -> int:
        if pool_bnb_wei <= 0 or pool_tokens <= 0:
            return 0
        bnb_after_fee = bnb_in_wei * 990 // 1000
        expected = (pool_tokens * bnb_after_fee) // (pool_bnb_wei + bnb_after_fee)
        return expected * 90 // 100

    def calc_expected_tokens(self, bnb_in_wei: int, pool_bnb_wei: int, pool_tokens: int) -> int:
        if pool_bnb_wei <= 0 or pool_tokens <= 0:
            return 0
        bnb_after_fee = bnb_in_wei * 990 // 1000
        return (pool_tokens * bnb_after_fee) // (pool_bnb_wei + bnb_after_fee)

    def calc_expected_bnb(self, tokens_in: int, pool_bnb_wei: int, pool_tokens: int) -> int:
        if pool_bnb_wei <= 0 or pool_tokens <= 0:
            return 0
        bnb_out = (tokens_in * pool_bnb_wei) // (pool_tokens + tokens_in)
        return bnb_out * 990 // 1000

    def calc_min_bnb_out(self, tokens_in: int, pool_bnb_wei: int, pool_tokens: int) -> int:
        if pool_bnb_wei <= 0 or pool_tokens <= 0:
            return 0
        bnb_out = (tokens_in * pool_bnb_wei) // (pool_tokens + tokens_in)
        return (bnb_out * 990 // 1000) * 90 // 100

    def calc_price_mult(self, init_bnb: float, cur_bnb: float) -> float:
        if init_bnb <= 0:
            return 1.0
        return cur_bnb / init_bnb

    @staticmethod
    def calc_market_cap_bnb(pool_bnb: float, graduate_thr: float = 24.0,
                            pool_tokens: int = 0, total_supply: int = 0,
                            last_price_wei: int = 0) -> float:
        """
        Four.meme FDV 估算（优先链上原生口径）。

        优先：lastPrice × totalSupply
          fdv_bnb = last_price_wei * total_supply / 1e36

        回退：旧的 x5 / 预留池子推导。
        """
        # 1) 原生口径：lastPrice + totalSupply（最接近行情站）
        if last_price_wei > 0 and total_supply > 0:
            mc = (float(last_price_wei) * float(total_supply)) / 1e36
            return round(min(mc, 1_000_000.0), 4)

        # 2) 无法拿到价格时再用旧口径
        if pool_bnb <= 0:
            return 0.0

        if pool_tokens > 0 and total_supply > 0:
            lp_reserve = int(total_supply * 0.2)
            if pool_tokens > lp_reserve:
                mc = pool_bnb * float(total_supply) / float(lp_reserve)
                return round(min(mc, 10000.0), 4)
            else:
                mc = pool_bnb * float(total_supply) / float(max(pool_tokens, 1))
                return round(min(mc, 10000.0), 4)

        mc = pool_bnb * 5.0
        return round(min(mc, 10000.0), 4)

    def estimate_token_value_bnb(self, token_amount: int, pool_bnb_wei: int, pool_tokens: int) -> float:
        if token_amount <= 0 or pool_bnb_wei <= 0 or pool_tokens <= 0:
            return 0.0
        expected_bnb_wei = self.calc_expected_bnb(token_amount, pool_bnb_wei, pool_tokens)
        return float(self.w3.from_wei(expected_bnb_wei, 'ether')) if expected_bnb_wei > 0 else 0.0

    # ================= 链上查询 =================

    async def get_token_name_symbol(self, token_addr: str) -> Tuple[str, str]:
        token_chk = Web3.to_checksum_address(token_addr)
        if token_chk in self._cache_name_sym:
            return self._cache_name_sym[token_chk]

        for attempt in range(4):
            try:
                name_raw = await self.fleet.safe_call(
                    'call', {'to': token_chk, 'data': "0x06fdde03"}, tiers=[2, 1]
                )
                sym_raw = await self.fleet.safe_call(
                    'call', {'to': token_chk, 'data': "0x95d89b41"}, tiers=[2, 1]
                )
                name = eth_abi.decode(['string'], bytes(name_raw))[0] if name_raw and len(name_raw) > 0 else "UNK_NAME"
                sym = eth_abi.decode(['string'], bytes(sym_raw))[0] if sym_raw and len(sym_raw) > 0 else "UNK_SYM"
                name = "".join(c for c in name if c.isprintable())[:15]
                sym = "".join(c for c in sym if c.isprintable())[:10]
                self._cache_name_sym[token_chk] = (name, sym)
                return name, sym
            except Exception:
                await asyncio.sleep(1.5)
        return "FourMemeCoin", "FMC"

    async def get_bnb_balance(self) -> float:
        bal = await self.fleet.safe_call('get_balance', self.address, tiers=[1, 2])
        return float(self.w3.from_wei(bal, 'ether')) if bal else 0.0

    async def get_token_balance(self, token_addr: str, use_paid: bool = False) -> int:
        token_chk = Web3.to_checksum_address(token_addr)
        data = bytes.fromhex("70a08231") + bytes.fromhex(self.address[2:].zfill(64))
        tiers = [0] if use_paid else [2, 1]
        res = await self.fleet.safe_call('call', {'to': token_chk, 'data': data}, tiers=tiers)
        if res and len(res) >= 32:
            return int.from_bytes(bytes(res)[-32:], 'big')
        return 0

    # ================= PancakeSwap 池子查询 =================

    async def get_pancake_pair(self, token_addr: str) -> Optional[str]:
        token_chk = Web3.to_checksum_address(token_addr)
        calldata = bytes.fromhex(GET_PAIR_SELECTOR[2:]) + eth_abi.encode(
            ['address', 'address'], [token_chk, WBNB_ADDRESS]
        )
        result = await self.fleet.safe_call(
            'call', {'to': self.pancake_factory, 'data': calldata}, tiers=[2, 1]
        )
        if result and len(result) >= 32:
            pair_addr = Web3.to_checksum_address("0x" + result.hex()[-40:])
            if pair_addr != "0x0000000000000000000000000000000000000000":
                return pair_addr
        return None

    async def get_pancake_reserves(self, pair_addr: str, token_addr: str) -> Tuple[int, int]:
        pair_chk = Web3.to_checksum_address(pair_addr)
        token_chk = Web3.to_checksum_address(token_addr)

        token0_raw = await self.fleet.safe_call(
            'call', {'to': pair_chk, 'data': TOKEN0_SELECTOR}, tiers=[2, 1]
        )
        if not token0_raw or len(token0_raw) < 32:
            return 0, 0

        token0 = Web3.to_checksum_address("0x" + token0_raw.hex()[-40:])
        token_is_token0 = (token0 == token_chk)

        reserves_raw = await self.fleet.safe_call(
            'call', {'to': pair_chk, 'data': GET_RESERVES_SELECTOR}, tiers=[2, 1]
        )
        if not reserves_raw or len(reserves_raw) < 64:
            return 0, 0

        raw_hex = reserves_raw.hex()
        if raw_hex.startswith('0x'):
            raw_hex = raw_hex[2:]

        reserve0 = int(raw_hex[0:64], 16)
        reserve1 = int(raw_hex[64:128], 16)

        if token_is_token0:
            return reserve0, reserve1
        else:
            return reserve1, reserve0

    async def is_pancake_pool_alive(self, token_addr: str) -> Tuple[bool, Optional[str], int, int]:
        pair = await self.get_pancake_pair(token_addr)
        if not pair:
            return False, None, 0, 0

        token_reserve, wbnb_reserve = await self.get_pancake_reserves(pair, token_addr)
        alive = wbnb_reserve > self.w3.to_wei(0.1, 'ether')
        return alive, pair, token_reserve, wbnb_reserve

    # ================= Approve =================

    async def approve_if_needed(self, token_addr: str, router: str = None) -> bool:
        # 🟢 模拟盘专属绿色通道：直接假装授权成功
        if self.dry_run:
            logger.info(f"🧪 [模拟盘] 假装授权成功！跳过链上 Approve: {token_addr[:8]}")
            return True

        if router is None:
            router = self.router

        approved_set = self.approved_fourmeme if router == FOURMEME_ROUTER else self.approved_pancake

        if token_addr in approved_set:
            return True

        token_chk = Web3.to_checksum_address(token_addr)
        try:
            allowance_data = (
                bytes.fromhex("dd62ed3e")
                + bytes.fromhex(self.address[2:].zfill(64))
                + bytes.fromhex(router[2:].zfill(64))
            )
            res = await self.fleet.safe_call(
                'call', {'to': token_chk, 'data': allowance_data}, tiers=[2, 1]
            )
            if res and len(res) >= 32:
                allowance = int.from_bytes(bytes(res)[-32:], 'big')
                if allowance > 10 ** 20:
                    approved_set.add(token_addr)
                    return True
        except Exception:
            pass

        calldata = bytes.fromhex(APPROVE_SELECTOR[2:]) + eth_abi.encode(
            ['address', 'uint256'], [router, MAX_UINT256]
        )
        result = await self._build_and_send(token_chk, 0, calldata, gas_mult=GAS_MULT_APPROVE, is_approve=True)
        if result:
            approved_set.add(token_addr)
        return bool(result)

    # ================= 模拟调用 (预检) =================

    async def simulate_call(self, to_addr: str, value_wei: int, calldata: bytes) -> bool:
        self.total_simulations += 1
        try:
            result = await self.fleet.safe_call(
                'call', {
                    'from': self.address,
                    'to': to_addr,
                    'value': hex(value_wei),
                    'data': calldata,
                }, tiers=[0]
            )
            if result == b'':
                self.total_sim_reverts += 1
                return False
            return True
        except Exception:
            self.total_sim_reverts += 1
            return False

    # ================= v11.1: RBF 交易重发 =================

    async def _wait_tx_with_rbf(self, tx_hash: str, signed_tx_dict: dict,
                                 gas_price: int, nonce: int) -> Tuple[bool, str]:
        """
        v11.1: 等待 TX 上链，超时未打包则 RBF 重发
        返回: (success, final_tx_hash)
        注意: 内部已设置 self.last_gas_cost_bnb
        """
        current_hash = tx_hash
        current_gas_price = gas_price
        start = time.time()

        for rbf_attempt in range(RBF_MAX_RETRIES + 1):
            check_start = time.time()
            while time.time() - check_start < RBF_CHECK_INTERVAL:
                if time.time() - start > RBF_TOTAL_TIMEOUT:
                    logger.warning(f"⏰ [RBF] TX {current_hash[:16]}... 总超时 ({RBF_TOTAL_TIMEOUT}s)")
                    return False, current_hash
                try:
                    receipt = await self.fleet.safe_call(
                        'get_transaction_receipt', current_hash, tiers=[1, 2]
                    )
                    if receipt:
                        status = receipt['status'] == 1
                        gas_used = receipt.get('gasUsed', 0)
                        effective_gas_price = receipt.get('effectiveGasPrice', 0)
                        if not effective_gas_price:
                            try:
                                tx_detail = await self.fleet.safe_call(
                                    'get_transaction', current_hash, tiers=[1, 2]
                                )
                                effective_gas_price = tx_detail.get('gasPrice', 0) if tx_detail else 0
                            except Exception:
                                effective_gas_price = 0
                        if gas_used and effective_gas_price:
                            gas_cost_wei = gas_used * effective_gas_price
                            self.last_gas_cost_bnb = float(self.w3.from_wei(gas_cost_wei, 'ether'))
                        logger.info(
                            f"{'✅' if status else '❌'} TX {current_hash[:16]}... "
                            f"| Gas: {gas_used} | 费: {self.last_gas_cost_bnb:.6f} BNB"
                            f"{f' (RBF #{rbf_attempt})' if rbf_attempt > 0 else ''}"
                        )
                        return status, current_hash
                except Exception:
                    pass
                await asyncio.sleep(1.5)

            if rbf_attempt >= RBF_MAX_RETRIES:
                break

            new_gas_price = int(current_gas_price * (1 + RBF_GAS_BUMP_PCT))
            self.total_rbf_bumps += 1
            logger.warning(
                f"🔄 [RBF] TX {current_hash[:16]}... 未打包 ({RBF_CHECK_INTERVAL}s)，"
                f"提价 {RBF_GAS_BUMP_PCT*100:.0f}%: "
                f"{self.w3.from_wei(current_gas_price, 'gwei'):.2f} → "
                f"{self.w3.from_wei(new_gas_price, 'gwei'):.2f} Gwei "
                f"[重试 {rbf_attempt+1}/{RBF_MAX_RETRIES}]"
            )

            try:
                new_tx = dict(signed_tx_dict)
                new_tx['gasPrice'] = new_gas_price
                new_tx['nonce'] = nonce

                new_signed = self.account.sign_transaction(new_tx)
                new_hash_bytes = await self.fleet.safe_call(
                    'send_raw_transaction', new_signed.rawTransaction, tiers=[0]
                )
                if new_hash_bytes:
                    current_hash = new_hash_bytes.hex()
                    current_gas_price = new_gas_price
                    logger.info(f"🔄 [RBF] 新TX: {current_hash[:16]}...")
                else:
                    logger.info(f"🔄 [RBF] 重发失败，检查原TX是否已上链...")
            except Exception as e:
                logger.warning(f"🔄 [RBF] 重发异常: {e}")

        return await self._final_wait(current_hash, max(0, RBF_TOTAL_TIMEOUT - (time.time() - start)))

    async def _final_wait(self, tx_hash: str, remaining_timeout: float) -> Tuple[bool, str]:
        start = time.time()
        while time.time() - start < remaining_timeout:
            try:
                receipt = await self.fleet.safe_call(
                    'get_transaction_receipt', tx_hash, tiers=[1, 2]
                )
                if receipt:
                    status = receipt['status'] == 1
                    gas_used = receipt.get('gasUsed', 0)
                    effective_gas_price = receipt.get('effectiveGasPrice', 0)
                    if gas_used and effective_gas_price:
                        self.last_gas_cost_bnb = float(self.w3.from_wei(gas_used * effective_gas_price, 'ether'))
                    return status, tx_hash
            except Exception:
                pass
            await asyncio.sleep(2)
        return False, tx_hash

    # ================= 核心交易构建 (v11.5: 乐观 Nonce) =================

    async def _build_and_send(self, to_addr, value_wei, calldata,
                               gas_mult: float = GAS_MULT_SELL,
                               is_approve=False, use_rbf=False) -> Optional[str]:
        """
        v11.5: 使用乐观 Nonce — acquire_nonce 后锁立即释放，
        不再阻塞其他并发交易（如并发 approve + sell）
        """
        nonce = None
        try:
            nonce = await self.nonce_mgr.acquire_nonce()

            enhanced_gas_price = await self._get_dynamic_gas_price(gas_mult)
            if enhanced_gas_price is None:
                return None

            tx = {
                'chainId': 56, 'from': self.address, 'to': to_addr,
                'value': value_wei, 'data': calldata, 'nonce': nonce,
                'gasPrice': enhanced_gas_price,
            }
            gas_est = await self.fleet.safe_call('estimate_gas', tx, tiers=[0])

            if gas_est is None:
                logger.error(
                    f"🚫 [Executor] Gas估算失败 (链上Revert)，放弃 → "
                    f"{to_addr[:10]}... | approve={is_approve}"
                )
                # ★ v15: 修复 nonce 泄漏 — estimate_gas 失败也要确认 nonce
                self.nonce_mgr.confirm_nonce(False)
                return None

            tx['gas'] = int(gas_est * 1.3)

            signed_tx = self.account.sign_transaction(tx)
            tx_hash_bytes = await self.fleet.safe_call(
                'send_raw_transaction', signed_tx.rawTransaction, tiers=[0]
            )
            if not tx_hash_bytes:
                self.nonce_mgr.confirm_nonce(False)
                # v11.5: 显式同步作为安全网（下次 acquire 也会自动同步）
                await self.nonce_mgr.sync_from_chain()
                return None

            # TX 已发出，nonce 确认有效
            self.nonce_mgr.confirm_nonce(True)

            tx_hash = tx_hash_bytes.hex()

            if is_approve:
                if use_rbf:
                    success, _ = await self._wait_tx_with_rbf(tx_hash, tx, enhanced_gas_price, nonce)
                    return success
                if await self.wait_tx(tx_hash):
                    return True
                return False

            # 非 approve: RBF 模式下等待确认后返回
            if use_rbf:
                success, final_hash = await self._wait_tx_with_rbf(tx_hash, tx, enhanced_gas_price, nonce)
                return final_hash if success else None

            return tx_hash

        except Exception as e:
            logger.error(f"🚫 [Executor] TX构建失败: {e}")
            if nonce is not None:
                self.nonce_mgr.confirm_nonce(False)
                await self.nonce_mgr.sync_from_chain()
            return None

    # ================= FourMeme 内盘买卖 =================

    async def buy(self, token_addr: str, bnb_amount_bnb: float,
                  pool_bnb_wei: int, pool_tokens: int) -> Optional[str]:
        """
        内盘买入
        返回: tx_hash (已上链成功) 或 None (失败)
        """
        self.total_buys += 1
        token_chk = Web3.to_checksum_address(token_addr)
        # 🟢 v15: 模拟盘 — 带真实感的虚拟执行
        if self.dry_run:
            import random
            # 模拟链上延迟 (0.5-2s)
            await asyncio.sleep(random.uniform(0.5, 2.0))
            # 模拟 gas 费 (0.0003-0.0008 BNB)
            self.last_gas_cost_bnb = round(random.uniform(0.0003, 0.0008), 6)
            # 模拟买税导致的代币损耗 (0-5%)
            sim_buy_tax = random.uniform(0.0, 0.05)
            sim_note = f" (模拟买税{sim_buy_tax*100:.1f}%)" if sim_buy_tax > 0.01 else ""
            logger.info(
                f"🧪 [模拟盘] 虚拟买入: {token_addr[:10]}... | "
                f"{bnb_amount_bnb:.4f} BNB | gas={self.last_gas_cost_bnb:.4f}{sim_note}"
            )
            fake_tx_hash = f"dry_buy_{int(time.time())}_{token_addr[-6:]}"
            return fake_tx_hash

        bnb_in_wei = self.w3.to_wei(bnb_amount_bnb, 'ether')
        min_amount = self.calc_min_tokens_out(bnb_in_wei, pool_bnb_wei, pool_tokens)

        calldata = bytes.fromhex(BUY_SELECTOR[2:]) + eth_abi.encode(
            ['address', 'address', 'uint256', 'uint256'],
            [token_chk, self.address, bnb_in_wei, min_amount]
        )

        sim_ok = await self.simulate_call(self.router, bnb_in_wei, calldata)
        if not sim_ok:
            logger.warning(f"🚫 [Executor] 买入模拟 Revert: {token_chk[:10]}... (可能貔貅盘)")
            self.total_buy_failures += 1
            return None

        result = await self._build_and_send(
            self.router, bnb_in_wei, calldata, gas_mult=GAS_MULT_BUY, use_rbf=True
        )
        if not result:
            self.total_buy_failures += 1
        return result

    async def sell(self, token_addr: str, token_amount: int,
                   pool_bnb_wei: int, pool_tokens: int) -> Optional[str]:
        """
        内盘卖出
        返回: tx_hash (已上链成功) 或 None (失败)
        """
        self.total_sells += 1
        # 🟢 v15: 模拟盘卖出 — 带真实感
        if self.dry_run:
            import random
            await asyncio.sleep(random.uniform(0.3, 1.5))
            self.last_gas_cost_bnb = round(random.uniform(0.0003, 0.0008), 6)
            # 模拟卖出滑点 (1-5%)
            sim_slippage = random.uniform(0.01, 0.05)
            logger.info(
                f"🧪 [模拟盘] 虚拟卖出: {token_addr[:10]}... | "
                f"数量 {token_amount} | gas={self.last_gas_cost_bnb:.4f} | 模拟滑点{sim_slippage*100:.1f}%"
            )
            fake_tx_hash = f"dry_sell_{int(time.time())}_{token_addr[-6:]}"
            return fake_tx_hash

        if not await self.approve_if_needed(token_addr, FOURMEME_ROUTER):
            self.total_sell_failures += 1
            return None
        token_chk = Web3.to_checksum_address(token_addr)
        min_bnb = self.calc_min_bnb_out(token_amount, pool_bnb_wei, pool_tokens)

        calldata = bytes.fromhex(SELL_SELECTOR[2:]) + eth_abi.encode(
            ['address', 'uint256', 'uint256', 'address'],
            [token_chk, token_amount, min_bnb, self.address]
        )
        result = await self._build_and_send(
            self.router, 0, calldata, gas_mult=GAS_MULT_SELL, use_rbf=True
        )
        if not result:
            self.total_sell_failures += 1
        return result

    # ================= PancakeSwap 外盘卖出 (含 MEV 保护) =================

    async def sell_on_pancakeswap(self, token_addr: str, token_amount: int,
                                   min_bnb_out: int = 0,
                                   emergency: bool = False) -> Optional[str]:
        """
        PancakeSwap V2 卖出
        返回: tx_hash (已上链成功) 或 None (失败)
        """
        
        is_dry_run = self.dry_run
        
        if is_dry_run:
            import random
            await asyncio.sleep(random.uniform(0.5, 2.0))
            self.last_gas_cost_bnb = round(random.uniform(0.0004, 0.0012), 6)
            self.total_pancake_sells += 1
            logger.info(
                f"🧪 [模拟盘] 外盘卖出: {token_addr[:10]}... | "
                f"数量 {token_amount} | gas={self.last_gas_cost_bnb:.4f}"
            )
            return f"dry_sell_ext_{int(time.time())}_{token_addr[-6:]}"  # 必须返回字符串格式 Hash

        self.total_pancake_sells += 1
        token_chk = Web3.to_checksum_address(token_addr)

        if not await self.approve_if_needed(token_addr, PANCAKE_ROUTER_V2):
            self.total_sell_failures += 1
            return None

        deadline = int(time.time()) + 300
        path = [token_chk, WBNB_ADDRESS]

        # MEV 底线滑点保护
        if min_bnb_out <= 0:
            try:
                pair = await self.get_pancake_pair(token_addr)
                if pair:
                    token_res, wbnb_res = await self.get_pancake_reserves(pair, token_addr)
                    if token_res > 0 and wbnb_res > 0:
                        expected_out = self.calc_expected_bnb(token_amount, wbnb_res, token_res)
                        min_bnb_out = int(expected_out * MEV_SLIPPAGE_FLOOR)
                        logger.info(
                            f"🛡️ [MEV] 底线滑点: 预期 {self.w3.from_wei(expected_out, 'ether'):.6f} BNB → "
                            f"最低 {self.w3.from_wei(min_bnb_out, 'ether'):.6f} BNB "
                            f"({MEV_SLIPPAGE_FLOOR*100:.0f}%)"
                        )
            except Exception as e:
                logger.warning(f"⚠️ [MEV] 底线计算失败: {e}，使用 min_bnb_out=0")
                min_bnb_out = 0

        calldata = bytes.fromhex(PANCAKE_SELL_SELECTOR[2:]) + eth_abi.encode(
            ['uint256', 'uint256', 'address[]', 'address', 'uint256'],
            [token_amount, min_bnb_out, path, self.address, deadline]
        )

        gas_mult = GAS_MULT_EMERGENCY if emergency else GAS_MULT_SELL
        result = await self._build_and_send(
            self.pancake_router, 0, calldata, gas_mult=gas_mult, use_rbf=True
        )
        if not result:
            self.total_sell_failures += 1
        return result

    # ================= TX 等待 (兼容旧调用，v11.2: 安全重入) =================

    async def wait_tx(self, tx_hash: str, timeout: int = 45) -> bool:
        """
        等待 TX 上链 (无 RBF 版本，保持向后兼容)
        v11.2: 不再盲目重置 last_gas_cost_bnb=0
        """
        start = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start < timeout:
            try:
                receipt = await self.fleet.safe_call(
                    'get_transaction_receipt', tx_hash, tiers=[1, 2]
                )
                if receipt:
                    status = receipt['status'] == 1
                    gas_used = receipt.get('gasUsed', 0)
                    effective_gas_price = receipt.get('effectiveGasPrice', 0)
                    if not effective_gas_price:
                        try:
                            tx_detail = await self.fleet.safe_call(
                                'get_transaction', tx_hash, tiers=[1, 2]
                            )
                            effective_gas_price = tx_detail.get('gasPrice', 0) if tx_detail else 0
                        except Exception:
                            effective_gas_price = 0

                    if gas_used and effective_gas_price:
                        gas_cost_wei = gas_used * effective_gas_price
                        self.last_gas_cost_bnb = float(self.w3.from_wei(gas_cost_wei, 'ether'))

                    logger.info(
                        f"{'✅' if status else '❌'} TX {tx_hash[:16]}... "
                        f"| Gas: {gas_used} | 费: {self.last_gas_cost_bnb:.6f} BNB"
                    )
                    return status
            except Exception:
                pass
            await asyncio.sleep(2)
        logger.warning(f"⏰ TX {tx_hash[:16]}... 超时 ({timeout}s)")
        return False

    def get_executor_stats(self) -> str:
        return (
            f"⚙️ <b>执行器统计 v11.5</b>\n"
            f"买入: {self.total_buys} (失败:{self.total_buy_failures})\n"
            f"内盘卖: {self.total_sells} | 外盘卖: {self.total_pancake_sells}\n"
            f"模拟: {self.total_simulations} (revert:{self.total_sim_reverts})\n"
            f"RBF提价: {self.total_rbf_bumps}次\n"
            f"{self.nonce_mgr.get_status_text()}"
        )