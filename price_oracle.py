"""
price_oracle.py — BNB 价格源
"""

import asyncio
import logging
import time
import contextlib

from web3 import Web3

logger = logging.getLogger("PriceOracle")

# PancakeSwap V2 WBNB/USDT 池子
PANCAKE_WBNB_USDT_PAIR = Web3.to_checksum_address("0x16b9a82891338f9bA80E2D6970FddA79D1eb0daE")
WBNB_ADDRESS = Web3.to_checksum_address("0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c")
USDT_ADDRESS = Web3.to_checksum_address("0x55d398326f99059fF775485246999027B3197955")

# Chainlink BSC BNB/USD 预言机
CHAINLINK_BNB_USD = Web3.to_checksum_address("0x0567F2323251f0Aab15c8dFb1967E4e8A7D42aeE")

# getReserves() selector
GET_RESERVES_SELECTOR = "0x0902f1ac"
# token0() selector
TOKEN0_SELECTOR = "0x0dfe1681"
# latestRoundData() selector
LATEST_ROUND_DATA_SELECTOR = "0xfeaf968c"

DEFAULT_BNB_PRICE = 600.0
REFRESH_INTERVAL = 60  # 秒
MAX_CONSECUTIVE_FAILURES = 5


class PriceOracle:
    """
    BNB 动态价格预言机
    - 主源: PancakeSwap V2 WBNB/USDT 池子
    - 备源: Chainlink BNB/USD 预言机
    - 兜底: 硬编码 DEFAULT_BNB_PRICE
    """

    def __init__(self, rpc_fleet):
        self.fleet = rpc_fleet
        self.current_price = DEFAULT_BNB_PRICE
        self.last_update = 0
        self.source = "默认"
        self.consecutive_failures = 0
        self.total_updates = 0
        self.total_failures = 0
        self._task = None
        self._wbnb_is_token0 = None  # 缓存 token0 顺序

        logger.info(f"💰 [PriceOracle] 初始化完成 | 默认价: ${DEFAULT_BNB_PRICE}")

    def start(self):
        """启动后台刷新协程"""
        if self._task is None:
            self._task = asyncio.create_task(self._refresh_loop())
            logger.info("💰 [PriceOracle] 后台刷新已启动 (每60s)")

    async def _refresh_loop(self):
        # 首次立即刷新
        await self._update_price()
        while True:
            await asyncio.sleep(REFRESH_INTERVAL)
            await self._update_price()

    async def shutdown(self):
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
            logger.info("💰 [PriceOracle] 后台刷新已关闭")

    async def _update_price(self):
        """尝试更新 BNB 价格"""
        # 主源: PancakeSwap V2
        price = await self._fetch_pancake_price()
        if price and price > 0:
            self.current_price = price
            self.source = "PancakeSwap"
            self.last_update = time.time()
            self.consecutive_failures = 0
            self.total_updates += 1
            logger.debug(f"💰 [PriceOracle] PancakeSwap 价格: ${price:.2f}")
            return

        # 备源: Chainlink
        price = await self._fetch_chainlink_price()
        if price and price > 0:
            self.current_price = price
            self.source = "Chainlink"
            self.last_update = time.time()
            self.consecutive_failures = 0
            self.total_updates += 1
            logger.debug(f"💰 [PriceOracle] Chainlink 价格: ${price:.2f}")
            return

        # 失败
        self.consecutive_failures += 1
        self.total_failures += 1
        if self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            self.source = f"兜底 (连续{self.consecutive_failures}次失败)"
            logger.warning(
                f"⚠️ [PriceOracle] 连续 {self.consecutive_failures} 次更新失败，"
                f"使用上次有效价格: ${self.current_price:.2f}"
            )

    async def _fetch_pancake_price(self) -> float:
        """从 PancakeSwap V2 WBNB/USDT 池子获取价格"""
        try:
            # 确定 token0 是 WBNB 还是 USDT
            if self._wbnb_is_token0 is None:
                token0_raw = await self.fleet.safe_call(
                    'call', {'to': PANCAKE_WBNB_USDT_PAIR, 'data': TOKEN0_SELECTOR},
                    tiers=[2, 1]
                )
                if token0_raw and len(token0_raw) >= 32:
                    token0_addr = Web3.to_checksum_address("0x" + token0_raw.hex()[-40:])
                    self._wbnb_is_token0 = (token0_addr == WBNB_ADDRESS)
                else:
                    return None

            # getReserves()
            reserves_raw = await self.fleet.safe_call(
                'call', {'to': PANCAKE_WBNB_USDT_PAIR, 'data': GET_RESERVES_SELECTOR},
                tiers=[2, 1]
            )
            if not reserves_raw or len(reserves_raw) < 64:
                return None

            raw_hex = reserves_raw.hex() if hasattr(reserves_raw, 'hex') else str(reserves_raw)
            if raw_hex.startswith('0x'):
                raw_hex = raw_hex[2:]

            reserve0 = int(raw_hex[0:64], 16)
            reserve1 = int(raw_hex[64:128], 16)

            if reserve0 == 0 or reserve1 == 0:
                return None

            # WBNB 18 decimals, USDT 18 decimals (BSC USDT)
            if self._wbnb_is_token0:
                bnb_reserve = reserve0
                usdt_reserve = reserve1
            else:
                bnb_reserve = reserve1
                usdt_reserve = reserve0

            price = usdt_reserve / bnb_reserve
            # 合理性检查
            if 10.0 < price < 50000.0:
                return round(price, 2)
            return None

        except Exception as e:
            logger.debug(f"💰 [PriceOracle] PancakeSwap 查询异常: {e}")
            return None

    async def _fetch_chainlink_price(self) -> float:
        """从 Chainlink BSC BNB/USD 预言机获取价格"""
        try:
            result_raw = await self.fleet.safe_call(
                'call', {'to': CHAINLINK_BNB_USD, 'data': LATEST_ROUND_DATA_SELECTOR},
                tiers=[2, 1]
            )
            if not result_raw or len(result_raw) < 160:
                return None

            raw_hex = result_raw.hex() if hasattr(result_raw, 'hex') else str(result_raw)
            if raw_hex.startswith('0x'):
                raw_hex = raw_hex[2:]

            # latestRoundData returns (roundId, answer, startedAt, updatedAt, answeredInRound)
            # answer is at offset 32-64 (second word), 8 decimals
            answer = int(raw_hex[64:128], 16)
            price = answer / 1e8

            if 10.0 < price < 50000.0:
                return round(price, 2)
            return None

        except Exception as e:
            logger.debug(f"💰 [PriceOracle] Chainlink 查询异常: {e}")
            return None

    def get_price(self) -> float:
        """获取当前 BNB/USD 价格"""
        return self.current_price

    def get_status_text(self) -> str:
        age = int(time.time() - self.last_update) if self.last_update > 0 else -1
        age_text = f"{age}s前" if age >= 0 else "从未更新"
        return (
            f"💰 <b>BNB 价格</b>\n"
            f"当前: <b>${self.current_price:.2f}</b>\n"
            f"数据源: {self.source}\n"
            f"更新: {age_text}\n"
            f"统计: {self.total_updates}成功/{self.total_failures}失败"
        )