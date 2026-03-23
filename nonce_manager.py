"""
nonce_manager.py — Nonce 并发管理
"""

import asyncio
import logging
import time

logger = logging.getLogger("NonceManager")

# v12.0: 常量
LOCK_ACQUIRE_TIMEOUT = 15.0      # 获取锁的最大等待时间
SYNC_COOLDOWN = 3.0              # 同步冷却时间
MAX_NONCE_GAP = 10               # nonce gap 超过此值强制同步
CHAIN_QUERY_TIMEOUT = 8.0        # 链上查询超时


class NonceManager:
    """
    Nonce 并发安全管理器 v12.0

    核心原则: NONCE 只增不减
    - acquire_nonce() 获取 nonce 并立即推进，释放锁
    - confirm_nonce(True): 成功，无需操作
    - confirm_nonce(False): 失败，标记需同步，下次 acquire 前自动修正
    - 绝不回退 nonce — 宁可产生 gap 也不冒撞车风险

    使用方式:
        nonce = await nonce_mgr.acquire_nonce()
        tx['nonce'] = nonce
        try:
            send_tx(tx)
            nonce_mgr.confirm_nonce(True)
        except:
            nonce_mgr.confirm_nonce(False)
    """

    def __init__(self, rpc_fleet, wallet_address: str):
        self.fleet = rpc_fleet
        self.wallet_address = wallet_address
        self._lock = asyncio.Lock()
        self._local_nonce = None
        self._initialized = False
        self.total_syncs = 0
        self.total_conflicts = 0

        # v12.0: 追踪
        self._pending_count = 0
        self._needs_sync = False           # 是否需要同步 (替代 _failed_nonces set)
        self._last_sync_time = 0
        self._consecutive_fails = 0        # 连续失败计数
        self._total_acquired = 0           # 总分配次数
        self._total_confirmed_ok = 0       # 确认成功次数
        self._total_confirmed_fail = 0     # 确认失败次数

    async def initialize(self):
        """从链上同步初始 nonce"""
        await self.sync_from_chain()
        self._initialized = True
        logger.info(
            f"🔑 [Nonce] 初始化完成 | 地址: {self.wallet_address[:10]}... | nonce: {self._local_nonce}"
        )

    async def sync_from_chain(self):
        """从链上获取最新 nonce，覆盖本地值"""
        try:
            async with asyncio.timeout(LOCK_ACQUIRE_TIMEOUT):
                async with self._lock:
                    chain_nonce = await asyncio.wait_for(
                        self.fleet.safe_call(
                            'get_transaction_count', self.wallet_address, 'pending',
                            tiers=[0]
                        ),
                        timeout=CHAIN_QUERY_TIMEOUT
                    )
                    if chain_nonce is not None:
                        old = self._local_nonce
                        # ★ v12.0: 只增不减 — 链上值低于本地值时不回退
                        # 例外: 首次初始化或标记了需同步
                        if old is not None and chain_nonce < old and not self._needs_sync:
                            logger.info(
                                f"🔑 [Nonce] 链上 {chain_nonce} < 本地 {old}，保留本地值 (只增不减)"
                            )
                        else:
                            self._local_nonce = chain_nonce
                            if old is not None and old != chain_nonce:
                                self.total_conflicts += 1
                                logger.warning(
                                    f"🔑 [Nonce] 链上同步: {old} → {chain_nonce} "
                                    f"(冲突修正 #{self.total_conflicts})"
                                )
                            else:
                                logger.debug(f"🔑 [Nonce] 链上同步: {chain_nonce}")

                        self.total_syncs += 1
                        self._last_sync_time = time.time()
                        self._needs_sync = False
                        self._consecutive_fails = 0
                    else:
                        logger.warning("⚠️ [Nonce] 链上查询失败，保留本地值")
        except asyncio.TimeoutError:
            logger.warning(f"⏰ [Nonce] sync_from_chain 超时 ({LOCK_ACQUIRE_TIMEOUT}s)")
        except Exception as e:
            logger.warning(f"⚠️ [Nonce] sync_from_chain 异常: {e}")

    async def acquire_nonce(self) -> int:
        """
        v12.0: 乐观获取 nonce (带超时保护)

        获取下一个可用 nonce，立即推进本地计数器并释放锁。
        ★ 绝不回退 — 分配出去的 nonce 不可撤销。
        """
        try:
            async with asyncio.timeout(LOCK_ACQUIRE_TIMEOUT):
                async with self._lock:
                    now = time.time()

                    # 如果标记了需同步 且 冷却期已过，先同步
                    if self._needs_sync and now - self._last_sync_time > SYNC_COOLDOWN:
                        try:
                            chain_nonce = await asyncio.wait_for(
                                self.fleet.safe_call(
                                    'get_transaction_count', self.wallet_address, 'pending',
                                    tiers=[0]
                                ),
                                timeout=CHAIN_QUERY_TIMEOUT
                            )
                            if chain_nonce is not None:
                                old = self._local_nonce
                                # 取较大值: 链上值 和 本地值，防止回退
                                self._local_nonce = max(
                                    chain_nonce,
                                    self._local_nonce if self._local_nonce is not None else 0
                                )
                                self.total_syncs += 1
                                self._last_sync_time = now
                                self._needs_sync = False
                                self._consecutive_fails = 0
                                if old != self._local_nonce:
                                    self.total_conflicts += 1
                                    logger.info(
                                        f"🔑 [Nonce] 自动修正: {old} → {self._local_nonce} "
                                        f"(链上={chain_nonce})"
                                    )
                        except (asyncio.TimeoutError, Exception) as e:
                            logger.debug(f"🔑 [Nonce] 自动同步失败: {e}，继续使用本地值")

                    # 首次使用，从链上获取
                    if self._local_nonce is None:
                        try:
                            chain_nonce = await asyncio.wait_for(
                                self.fleet.safe_call(
                                    'get_transaction_count', self.wallet_address, 'pending',
                                    tiers=[0]
                                ),
                                timeout=CHAIN_QUERY_TIMEOUT
                            )
                            self._local_nonce = chain_nonce if chain_nonce is not None else 0
                            self._last_sync_time = time.time()
                        except (asyncio.TimeoutError, Exception):
                            self._local_nonce = 0

                    nonce = self._local_nonce
                    self._local_nonce += 1  # ★ 乐观推进，绝不回退
                    self._pending_count += 1
                    self._total_acquired += 1

        except asyncio.TimeoutError:
            logger.error(f"⏰ [Nonce] acquire_nonce 锁超时 ({LOCK_ACQUIRE_TIMEOUT}s)!")
            # 极端情况: 锁超时，使用当前值+1 作为应急
            # 可能导致 nonce 冲突，但胜过完全卡死
            if self._local_nonce is not None:
                nonce = self._local_nonce
                self._local_nonce += 1
                self._needs_sync = True
            else:
                raise RuntimeError("Nonce manager 未初始化且锁超时")

        return nonce

    def confirm_nonce(self, success: bool):
        """
        v12.0: 确认 nonce 使用结果

        success=True: 交易已上链，nonce 有效
        success=False: 交易失败 — 标记需同步，下次 acquire 时自动修正
        ★ 绝不回退本地 nonce — 宁可 gap 也不撞车
        """
        self._pending_count = max(0, self._pending_count - 1)

        if success:
            self._total_confirmed_ok += 1
            self._consecutive_fails = 0
        else:
            self._total_confirmed_fail += 1
            self._consecutive_fails += 1
            self._needs_sync = True
            logger.debug(
                f"🔑 [Nonce] 交易失败 #{self._consecutive_fails}，"
                f"标记需同步 (pending={self._pending_count})"
            )

            # v12.0: 连续失败过多，在后台触发异步同步
            if self._consecutive_fails >= 3:
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(self._background_sync())
                except RuntimeError:
                    pass

    async def _background_sync(self):
        """后台异步同步 (不阻塞调用方)"""
        if time.time() - self._last_sync_time < SYNC_COOLDOWN:
            return
        try:
            await self.sync_from_chain()
        except Exception as e:
            logger.debug(f"🔑 [Nonce] 后台同步失败: {e}")

    # ★ v12.0: 彻底移除 release_without_confirm
    # 理由: 回退 nonce 在高并发下极易造成撞车卡死
    # 如果 acquire 了但没发 TX，这个 nonce 就浪费了
    # 链上同步会自动纠正，代价只是一个空隙

    def get_status_text(self) -> str:
        sync_status = "🟢 正常" if not self._needs_sync else "🟡 待同步"
        return (
            f"🔑 <b>Nonce 管理器 v12.0</b> [只增不减✅]\n"
            f"当前 nonce: {self._local_nonce}\n"
            f"进行中: {self._pending_count} | 状态: {sync_status}\n"
            f"分配: {self._total_acquired} | 成功: {self._total_confirmed_ok} | 失败: {self._total_confirmed_fail}\n"
            f"链上同步: {self.total_syncs}次\n"
            f"冲突修正: {self.total_conflicts}次"
        )