"""
bsc_detector.py — Four.meme 新币事件监听 (WebSocket)
"""

import asyncio
import logging
import contextlib
from collections import OrderedDict
from web3 import Web3, AsyncWeb3
from web3.providers.persistent import WebSocketProvider
from rpc_fleet import RpcFleet

logger = logging.getLogger("BSC_Detector")

CREATE_TOPIC0 = "0x396d5e902b675b032348d3d2e9517ee8f0c4a926603fbc075d3d282ff00cad20"
FOURMEME_ROUTER = Web3.to_checksum_address("0x5c952063c7fc8610FFDB798152D69F0B9550762b")
SUFFIX_WHITELIST = ["4444", "7777", "ffff"]
MAX_QUERY_RANGE = 50
WS_RECONNECT_DELAY = 5
WS_IDLE_LOG_INTERVAL = 120
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


class LRUSet:
    def __init__(self, capacity: int):
        self.cache = OrderedDict()
        self.capacity = capacity

    def contains_and_add(self, key: str) -> bool:
        if key in self.cache:
            self.cache.move_to_end(key)
            return True
        self.cache[key] = None
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)
        return False


class FourMemeDetector:
    def __init__(self, fleet: RpcFleet, on_new_coin: callable):
        self.fleet = fleet
        self.on_new_coin = on_new_coin
        self.last_block = None
        self._seen = LRUSet(15000)
        self._tx_from_cache = OrderedDict()
        self._ws_task = None
        self._ws_stop = asyncio.Event()
        self.total_logs_scanned = 0
        self.total_events_found = 0
        self.total_vanity_matched = 0
        self.total_duplicates_skipped = 0
        self.total_gap_advances = 0
        self.total_log_failures = 0
        self.total_creator_from_tx = 0
        self.total_ws_events = 0
        self.total_ws_errors = 0
        self.poll_count = 0

    def _remember_tx_from(self, tx_hash: str, addr: str):
        self._tx_from_cache[tx_hash] = addr
        if len(self._tx_from_cache) > 4000:
            self._tx_from_cache.popitem(last=False)

    async def _resolve_creator(self, tx_hash: str, fallback_candidates: list):
        cached = self._tx_from_cache.get(tx_hash)
        if cached:
            self._tx_from_cache.move_to_end(tx_hash)
            return cached

        creator_addr = None
        try:
            tx = await self.fleet.safe_call('get_transaction', tx_hash, tiers=[1, 2])
            if tx:
                tx_from = tx.get('from') if isinstance(tx, dict) else getattr(tx, 'get', lambda *_: None)('from')
                if tx_from:
                    creator_addr = Web3.to_checksum_address(tx_from)
                    self._remember_tx_from(tx_hash, creator_addr)
                    self.total_creator_from_tx += 1
        except Exception:
            pass

        if creator_addr:
            return creator_addr

        if fallback_candidates:
            return fallback_candidates[0]
        return Web3.to_checksum_address(ZERO_ADDRESS)

    @staticmethod
    def _extract_addresses(log: dict) -> list:
        topics = log.get('topics', [])
        data = log.get('data', b'')
        addresses = []

        for t in topics[1:]:
            try:
                t_hex = t.hex() if hasattr(t, 'hex') else str(t)
                addr = Web3.to_checksum_address("0x" + t_hex[-40:])
                if addr != ZERO_ADDRESS:
                    addresses.append(addr)
            except Exception:
                pass

        try:
            d_hex = data.hex() if isinstance(data, (bytes, bytearray)) else str(data)
            if d_hex.startswith('0x'):
                d_hex = d_hex[2:]
            for i in range(0, len(d_hex), 64):
                chunk = d_hex[i:i + 64]
                if len(chunk) >= 40:
                    addr = Web3.to_checksum_address("0x" + chunk[-40:])
                    if addr != ZERO_ADDRESS:
                        addresses.append(addr)
        except Exception:
            pass

        # 去重保序
        deduped = []
        seen = set()
        for a in addresses:
            if a not in seen:
                deduped.append(a)
                seen.add(a)
        return deduped

    async def _process_log(self, log: dict, source: str):
        self.total_events_found += 1
        raw_hash = log.get('transactionHash', b'')
        sig = raw_hash.hex() if isinstance(raw_hash, (bytes, bytearray)) else str(raw_hash)
        tx_hash = sig if sig.startswith('0x') else f'0x{sig}'
        block_number = log.get('blockNumber')
        if isinstance(block_number, str) and block_number.startswith('0x'):
            try:
                block_number = int(block_number, 16)
            except Exception:
                pass
        elif hasattr(block_number, 'hex'):
            try:
                block_number = int(block_number.hex(), 16)
            except Exception:
                pass

        emitter = log.get('address')
        if isinstance(emitter, str) and emitter:
            with contextlib.suppress(Exception):
                emitter = Web3.to_checksum_address(emitter)

        if emitter and emitter != FOURMEME_ROUTER:
            logger.info(
                f"🚫 [Detector:{source}] 非Four.meme路由事件，忽略 | block={block_number} | emitter={emitter} | tx={tx_hash}"
            )
            return

        topics = log.get('topics', []) or []
        topic0 = None
        if topics:
            raw_topic0 = topics[0]
            topic0 = raw_topic0.hex() if hasattr(raw_topic0, 'hex') else str(raw_topic0)

        if self._seen.contains_and_add(sig):
            self.total_duplicates_skipped += 1
            logger.info(
                f"↩️ [Detector:{source}] 重复事件 | block={block_number} | emitter={emitter} | "
                f"topic0={topic0} | tx={tx_hash}"
            )
            return

        addresses = self._extract_addresses(log)

        token_addr = None
        for addr in addresses:
            if any(addr.lower().endswith(s) for s in SUFFIX_WHITELIST):
                token_addr = addr
                break

        if not token_addr:
            logger.info(
                f"🔕 [Detector:{source}] 非靓号，忽略 | block={block_number} | emitter={emitter} | "
                f"tx={tx_hash} | candidates={addresses[:4]}"
            )
            return

        self.total_vanity_matched += 1
        creator_candidates = [a for a in addresses if a != token_addr]
        creator_addr = await self._resolve_creator(sig, creator_candidates)

        logger.info(
            f"🎯 [Detector:{source}] 捕获靓号 #{self.total_vanity_matched} | block={block_number} | "
            f"emitter={emitter} | topic0={topic0} | token={token_addr} | creator={creator_addr} | tx={tx_hash}"
        )
        asyncio.create_task(self.on_new_coin(token_addr, creator_addr, 0.0))

    async def _ws_loop(self):
        ws_candidates = [
            "wss://lb.drpc.live/bsc/ArLqxkU7qUCKlQznKxPbkIVZGMkhIUIR8aU9dg7bSgwO",
            "wss://go.getblock.us/49112e55a44a4baa970339ddbd0235d2",
        ]
        paid_url = getattr(self.fleet, 'paid_url', None)
        if paid_url and paid_url.startswith('https://'):
            derived = 'wss://' + paid_url[len('https://'):]
            if derived not in ws_candidates:
                ws_candidates.append(derived)

        logger.info(f"📡 [Detector] WS监听候选: {len(ws_candidates)} 条")

        while not self._ws_stop.is_set():
            for ws_url in ws_candidates:
                if self._ws_stop.is_set():
                    return

                logger.info(f"📡 [Detector] WS监听启动: {ws_url[:56]}...")
                try:
                    async with AsyncWeb3(WebSocketProvider(ws_url)) as w3:
                        # Some BSC WS providers silently miss logs when `address` is
                        # included in the server-side filter. Subscribe by topic first
                        # and verify the emitter locally in `_process_log`.
                        sub_id = await w3.eth.subscribe('logs', {
                            'topics': [CREATE_TOPIC0],
                        })
                        logger.info(f"🟢 [Detector] WS订阅已建立: {sub_id}")

                        while not self._ws_stop.is_set():
                            try:
                                message = await asyncio.wait_for(
                                    anext(w3.socket.process_subscriptions()),
                                    timeout=WS_IDLE_LOG_INTERVAL,
                                )
                            except asyncio.TimeoutError:
                                logger.info(f"⌛ [Detector] WS空闲{WS_IDLE_LOG_INTERVAL}s | events={self.total_ws_events}")
                                continue

                            result = message.get('result') if hasattr(message, 'get') else None
                            if not hasattr(result, 'get'):
                                continue
                            self.total_ws_events += 1
                            await self._process_log(result, 'ws')
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.total_ws_errors += 1
                    logger.warning(f"⚠️ [Detector] WS监听异常 ({ws_url[:32]}...): {e}")
                    await asyncio.sleep(WS_RECONNECT_DELAY)
                    continue

            await asyncio.sleep(WS_RECONNECT_DELAY)

    async def run_loop(self):
        logger.info("👁️ [Detector] ═══ 雷达开启 (仅WS监听 + tx.from creator) ═══")
        if self._ws_task is None:
            self._ws_stop.clear()
            self._ws_task = asyncio.create_task(self._ws_loop())

        while True:
            self.poll_count += 1
            await asyncio.sleep(5)

    async def shutdown(self):
        self._ws_stop.set()
        if self._ws_task:
            self._ws_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._ws_task
            self._ws_task = None

    def get_status_text(self) -> str:
        return (
            f"👁️ <b>雷达状态</b>\n"
            f"心跳: {self.poll_count} | WS事件: {self.total_ws_events} | WS异常: {self.total_ws_errors}\n"
            f"扫描: {self.total_logs_scanned} | 靓号: {self.total_vanity_matched}\n"
            f"去重: {self.total_duplicates_skipped} | tx.from命中: {self.total_creator_from_tx}\n"
            f"模式: 仅WS监听 | 区块: {self.last_block or 'N/A'}"
        )
