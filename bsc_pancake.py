"""
bsc_pancake.py — PancakeSwap 外盘仓位跟踪
"""

import asyncio
import logging
import os
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from typing import Optional, Callable

from web3 import Web3

logger = logging.getLogger("BSC_Pancake")

# ================= 外盘参数 =================
BLIND_WAIT_INTERVAL = 5
BLIND_WAIT_TIMEOUT = 600
TRACKING_INTERVAL = 10
ATH_DRAWDOWN_THRESHOLD = 0.50
MIN_LIQUIDITY_BNB = 0.5

ATH_TRACKING_TTL = 172800  # 48h
BALANCE_CHECK_INTERVAL = 1800  # 30min

WBNB_DECIMALS = 18
WBNB_ADDRESS = Web3.to_checksum_address("0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c")

BSCSCAN_TX_PREFIX = "https://bscscan.com/tx/"
BSCSCAN_ADDR_PREFIX = "https://bscscan.com/address/"

# v12.0: 超时常量
SELL_TIMEOUT = 120          # 卖出操作总超时
APPROVE_TIMEOUT = 60        # 授权操作超时
PRICE_QUERY_TIMEOUT = 10    # 价格查询超时
BALANCE_QUERY_TIMEOUT = 10  # 余额查询超时
POOL_CHECK_TIMEOUT = 10     # 池子检查超时

# v12.0: DB 路径
PANCAKE_DB_FILE = "pancake_positions.db"


# v12.0: 全局辅助函数 (单位转换)
def fmt_mc(v: float) -> str:
    if v >= 1_000_000: return f"${v/1_000_000:.2f}M"
    elif v >= 1_000: return f"${v/1_000:.1f}K"
    return f"${v:.0f}"


def _tx_link(tx_hash: str) -> str:
    if not tx_hash or tx_hash.startswith("dry_run"):
        return f"<code>{tx_hash}</code>"
    return f"<a href='{BSCSCAN_TX_PREFIX}{tx_hash}'>🔗 BSCScan</a>"


def _addr_link(addr: str, short: bool = True) -> str:
    display = f"{addr[:10]}...{addr[-6:]}" if short else addr
    return f"<a href='{BSCSCAN_ADDR_PREFIX}{addr}'>{display}</a>"


class PancakeStatus(Enum):
    MIGRATING = "migrating"
    BLIND_WAITING = "blind_wait"
    TRACKING = "tracking"
    SELLING = "selling"
    DONE = "done"
    ABANDONED = "abandoned"


class PancakePosition:
    """外盘仓位信息"""

    def __init__(self, token_addr: str, symbol: str, token_amount: int,
                 cost_bnb: float, ai_score: float = 0, decimals: int = 18):
        self.token_addr = Web3.to_checksum_address(token_addr)
        self.symbol = symbol
        self.token_amount = token_amount
        self.cost_bnb = cost_bnb
        self.ai_score = ai_score
        self.decimals = decimals

        self.status = PancakeStatus.MIGRATING
        self.pair_addr = None
        self.ath_bnb = 0.0
        self.ath_time = 0
        self.current_price_bnb = 0.0
        self.current_drawdown = 0.0
        self.migrate_time = time.time()
        self.pool_found_time = 0
        self.sell_tx = None
        self.realized_bnb = 0.0
        self.cancel_flag = False
        self.pre_approved = False
        self.tracking_start_time = 0
        self.last_balance_check = 0

        self._task: Optional[asyncio.Task] = None

    def to_db_dict(self) -> dict:
        """序列化为数据库字段"""
        return {
            "token_addr": self.token_addr,
            "symbol": self.symbol,
            "token_amount": str(self.token_amount),
            "cost_bnb": self.cost_bnb,
            "ai_score": self.ai_score,
            "decimals": self.decimals,
            "status": self.status.value,
            "pair_addr": self.pair_addr or "",
            "ath_bnb": self.ath_bnb,
            "ath_time": self.ath_time,
            "current_price_bnb": self.current_price_bnb,
            "migrate_time": self.migrate_time,
            "pool_found_time": self.pool_found_time,
            "realized_bnb": self.realized_bnb,
            "pre_approved": 1 if self.pre_approved else 0,
            "tracking_start_time": self.tracking_start_time,
        }

    @classmethod
    def from_db_row(cls, row) -> 'PancakePosition':
        """从数据库行还原"""
        # 将 sqlite3.Row 转换为 dict
        if not isinstance(row, dict):
            row = dict(row)
        pos = cls(
            token_addr=row["token_addr"],
            symbol=row.get("symbol", "Unknown"),
            token_amount=int(row["token_amount"]),
            cost_bnb=row["cost_bnb"],
            ai_score=row.get("ai_score", 0),
            decimals=row.get("decimals", 18),
        )
        pos.status = PancakeStatus(row.get("status", "migrating"))
        pos.pair_addr = row.get("pair_addr") or None
        if pos.pair_addr == "":
            pos.pair_addr = None
        pos.ath_bnb = row.get("ath_bnb", 0.0)
        pos.ath_time = row.get("ath_time", 0)
        pos.current_price_bnb = row.get("current_price_bnb", 0.0)
        pos.migrate_time = row.get("migrate_time", time.time())
        pos.pool_found_time = row.get("pool_found_time", 0)
        pos.realized_bnb = row.get("realized_bnb", 0.0)
        pos.pre_approved = bool(row.get("pre_approved", 0))
        pos.tracking_start_time = row.get("tracking_start_time", 0)
        return pos


class PancakeManager:
    """
    PancakeSwap 外盘管理器 v12.0
    ★ 所有仓位持久化到 SQLite — 重启后自动恢复
    """

    def __init__(self, executor, notify_fn: Callable, price_oracle=None, db_path: str = PANCAKE_DB_FILE):
        self.executor = executor
        self.fleet = executor.fleet
        self.notify = notify_fn
        self.price_oracle = price_oracle

        self.positions: dict[str, PancakePosition] = {}
        self.completed: list[dict] = []

        self.total_migrated = 0
        self.total_pool_found = 0
        self.total_auto_sold = 0
        self.total_force_sold = 0
        self.total_abandoned = 0
        self.total_timeout = 0
        self.total_ttl_expired = 0

        # v12.0: SQLite 持久化
        self._db_path = db_path
        self._db_pool = ThreadPoolExecutor(1, thread_name_prefix="pancake_db")
        self._init_db()

    # ================= v12.0: SQLite 持久化层 =================

    def _get_conn(self):
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._get_conn() as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS pancake_positions (
                token_addr TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                token_amount TEXT NOT NULL,
                cost_bnb REAL NOT NULL,
                ai_score REAL DEFAULT 0,
                decimals INTEGER DEFAULT 18,
                status TEXT NOT NULL DEFAULT 'migrating',
                pair_addr TEXT DEFAULT '',
                ath_bnb REAL DEFAULT 0.0,
                ath_time REAL DEFAULT 0,
                current_price_bnb REAL DEFAULT 0.0,
                migrate_time REAL NOT NULL,
                pool_found_time REAL DEFAULT 0,
                realized_bnb REAL DEFAULT 0.0,
                pre_approved INTEGER DEFAULT 0,
                tracking_start_time REAL DEFAULT 0,
                updated_at REAL DEFAULT 0
            )""")
            conn.execute("""CREATE TABLE IF NOT EXISTS pancake_stats (
                key TEXT PRIMARY KEY,
                value INTEGER DEFAULT 0
            )""")
            # 初始化统计计数器
            for key in ["total_migrated", "total_pool_found", "total_auto_sold",
                         "total_force_sold", "total_abandoned", "total_timeout",
                         "total_ttl_expired"]:
                conn.execute(
                    "INSERT OR IGNORE INTO pancake_stats (key, value) VALUES (?, 0)", (key,)
                )
            conn.commit()
        logger.info(f"🥞 [Pancake] SQLite 持久化就绪: {self._db_path}")

    def _db_save_position(self, pos: PancakePosition):
        """同步写入/更新单个仓位 (在 DB 线程池中调用)"""
        d = pos.to_db_dict()
        d["updated_at"] = time.time()
        with self._get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO pancake_positions
                (token_addr, symbol, token_amount, cost_bnb, ai_score, decimals,
                 status, pair_addr, ath_bnb, ath_time, current_price_bnb,
                 migrate_time, pool_found_time, realized_bnb, pre_approved,
                 tracking_start_time, updated_at)
                VALUES (:token_addr, :symbol, :token_amount, :cost_bnb, :ai_score,
                        :decimals, :status, :pair_addr, :ath_bnb, :ath_time,
                        :current_price_bnb, :migrate_time, :pool_found_time,
                        :realized_bnb, :pre_approved, :tracking_start_time, :updated_at)
            """, d)
            conn.commit()

    def _db_delete_position(self, token_addr: str):
        """同步删除仓位"""
        with self._get_conn() as conn:
            conn.execute("DELETE FROM pancake_positions WHERE token_addr = ?", (token_addr,))
            conn.commit()

    def _db_update_status(self, token_addr: str, status: str):
        """快速更新状态字段"""
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE pancake_positions SET status=?, updated_at=? WHERE token_addr=?",
                (status, time.time(), token_addr)
            )
            conn.commit()

    def _db_update_tracking(self, pos: PancakePosition):
        """更新追踪相关字段 (ATH/价格/余额)"""
        with self._get_conn() as conn:
            conn.execute("""
                UPDATE pancake_positions SET
                    ath_bnb=?, ath_time=?, current_price_bnb=?,
                    token_amount=?, pair_addr=?, pre_approved=?,
                    tracking_start_time=?, pool_found_time=?,
                    status=?, updated_at=?
                WHERE token_addr=?
            """, (pos.ath_bnb, pos.ath_time, pos.current_price_bnb,
                  str(pos.token_amount), pos.pair_addr or "", 1 if pos.pre_approved else 0,
                  pos.tracking_start_time, pos.pool_found_time,
                  pos.status.value, time.time(), pos.token_addr))
            conn.commit()

    def _db_increment_stat(self, key: str, delta: int = 1):
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE pancake_stats SET value = value + ? WHERE key = ?", (delta, key)
            )
            conn.commit()

    def _db_load_all_active(self) -> list[dict]:
        """加载所有非终态仓位"""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM pancake_positions WHERE status NOT IN ('done', 'abandoned')"
            ).fetchall()
            return [dict(r) for r in rows]

    def _db_load_stats(self) -> dict:
        with self._get_conn() as conn:
            rows = conn.execute("SELECT key, value FROM pancake_stats").fetchall()
            return {r["key"]: r["value"] for r in rows}

    async def _persist_position(self, pos: PancakePosition):
        """异步持久化仓位"""
        try:
            await asyncio.get_running_loop().run_in_executor(
                self._db_pool, self._db_save_position, pos
            )
        except Exception as e:
            logger.error(f"❌ [Pancake] DB 写入失败: {pos.symbol} | {e}")

    async def _persist_delete(self, token_addr: str):
        """异步删除仓位"""
        try:
            await asyncio.get_running_loop().run_in_executor(
                self._db_pool, self._db_delete_position, token_addr
            )
        except Exception as e:
            logger.error(f"❌ [Pancake] DB 删除失败: {token_addr[:10]} | {e}")

    async def _persist_status(self, token_addr: str, status: PancakeStatus):
        """异步更新状态"""
        try:
            await asyncio.get_running_loop().run_in_executor(
                self._db_pool, self._db_update_status, token_addr, status.value
            )
        except Exception as e:
            logger.error(f"❌ [Pancake] DB 状态更新失败: {e}")

    async def _persist_tracking(self, pos: PancakePosition):
        """异步更新追踪数据"""
        try:
            await asyncio.get_running_loop().run_in_executor(
                self._db_pool, self._db_update_tracking, pos
            )
        except Exception as e:
            logger.error(f"❌ [Pancake] DB 追踪更新失败: {e}")

    async def _persist_stat(self, key: str, delta: int = 1):
        try:
            await asyncio.get_running_loop().run_in_executor(
                self._db_pool, self._db_increment_stat, key, delta
            )
        except Exception:
            pass

    # ================= v12.0: 启动恢复 =================

    async def restore_from_db(self):
        """
        启动时从数据库恢复所有活跃的外盘仓位，重新启动追踪协程。
        这是防断电的核心: 重启后外盘仓位不丢失。
        """
        try:
            rows = await asyncio.get_running_loop().run_in_executor(
                self._db_pool, self._db_load_all_active
            )
            stats = await asyncio.get_running_loop().run_in_executor(
                self._db_pool, self._db_load_stats
            )
        except Exception as e:
            logger.error(f"❌ [Pancake] DB 加载失败: {e}")
            return 0

        # 恢复统计计数器
        self.total_migrated = stats.get("total_migrated", 0)
        self.total_pool_found = stats.get("total_pool_found", 0)
        self.total_auto_sold = stats.get("total_auto_sold", 0)
        self.total_force_sold = stats.get("total_force_sold", 0)
        self.total_abandoned = stats.get("total_abandoned", 0)
        self.total_timeout = stats.get("total_timeout", 0)
        self.total_ttl_expired = stats.get("total_ttl_expired", 0)

        restored = 0
        for row in rows:
            try:
                pos = PancakePosition.from_db_row(row)
                token_addr = pos.token_addr

                if token_addr in self.positions:
                    continue

                self.positions[token_addr] = pos

                # 根据保存的状态决定恢复策略
                if pos.status == PancakeStatus.BLIND_WAITING:
                    # 检查盲等是否已超时
                    elapsed = time.time() - pos.migrate_time
                    if elapsed >= BLIND_WAIT_TIMEOUT:
                        logger.info(f"🥞 [Restore] {pos.symbol} 盲等已超时，标记放弃")
                        pos.status = PancakeStatus.ABANDONED
                        await self._persist_status(token_addr, PancakeStatus.ABANDONED)
                        self._finalize(token_addr, 0.0)
                        continue
                    # 重新启动追踪协程
                    pos._task = asyncio.create_task(self._tracking_coroutine(token_addr))
                    restored += 1

                elif pos.status == PancakeStatus.TRACKING:
                    # 检查 TTL 是否已过期
                    if pos.tracking_start_time > 0:
                        tracking_age = time.time() - pos.tracking_start_time
                        if tracking_age >= ATH_TRACKING_TTL:
                            logger.info(f"🥞 [Restore] {pos.symbol} TTL 已过期，执行强平")
                            pos._task = asyncio.create_task(
                                self._execute_sell(token_addr, emergency=True)
                            )
                            restored += 1
                            continue
                    # 重新启动 ATH 追踪 (跳过盲等阶段)
                    pos._task = asyncio.create_task(self._resume_tracking(token_addr))
                    restored += 1

                elif pos.status == PancakeStatus.SELLING:
                    # 上次卖出可能中断，重新尝试
                    logger.info(f"🥞 [Restore] {pos.symbol} 上次卖出中断，重新执行")
                    pos._task = asyncio.create_task(
                        self._execute_sell(token_addr, emergency=True)
                    )
                    restored += 1

                elif pos.status == PancakeStatus.MIGRATING:
                    # 迁移中断，重新开始完整流程
                    pos._task = asyncio.create_task(self._tracking_coroutine(token_addr))
                    restored += 1

                else:
                    # done/abandoned 不应出现在活跃列表，清理
                    self._finalize(token_addr, pos.realized_bnb)

            except Exception as e:
                logger.error(f"❌ [Pancake] 恢复仓位失败: {row.get('symbol', '?')} | {e}")

        if restored > 0:
            logger.info(f"🥞 [Pancake] 从数据库恢复 {restored} 个外盘仓位")
            await self.notify(
                f"🥞 <b>外盘恢复</b> | 从数据库恢复 <b>{restored}</b> 个仓位\n"
                f"累计: 迁入={self.total_migrated} 建池={self.total_pool_found}\n"
                f"💡 /ext 查看状态"
            )
        return restored

    async def _resume_tracking(self, token_addr: str):
        """v12.0: 恢复 ATH 追踪 (跳过盲等阶段, 用于重启后恢复)"""
        pos = self.positions.get(token_addr)
        if not pos:
            return

        try:
            # 验证池子是否仍然存在
            if not pos.pair_addr:
                pair = await asyncio.wait_for(
                    self.executor.get_pancake_pair(token_addr),
                    timeout=POOL_CHECK_TIMEOUT
                )
                if pair:
                    pos.pair_addr = pair
                else:
                    logger.warning(f"🥞 [Resume] {pos.symbol} 池子不存在，重新盲等")
                    pos.status = PancakeStatus.BLIND_WAITING
                    await self._persist_status(token_addr, PancakeStatus.BLIND_WAITING)
                    # 重走完整流程
                    await self._tracking_coroutine(token_addr)
                    return

            # 确保预授权
            if not pos.pre_approved:
                asyncio.create_task(self._pre_approve_during_wait(token_addr))

            pos.status = PancakeStatus.TRACKING
            await self._persist_status(token_addr, PancakeStatus.TRACKING)

            logger.info(f"🥞 [Resume] {pos.symbol} 恢复 ATH 追踪 | ATH={pos.ath_bnb:.13f}")
            await self._track_ath(token_addr)

        except asyncio.CancelledError:
            logger.info(f"🛑 [Pancake] {pos.symbol} 恢复追踪被取消")
        except Exception as e:
            logger.error(f"❌ [Pancake] {pos.symbol} 恢复追踪异常: {e}", exc_info=True)
            pos.status = PancakeStatus.ABANDONED
            await self._persist_status(token_addr, PancakeStatus.ABANDONED)
        finally:
            # 确保异常退出时清理内存
            if pos.status in (PancakeStatus.ABANDONED,):
                self._finalize(token_addr, 0.0)

    # ================= 迁移入口 =================

    async def migrate_from_internal(self, token_addr: str, symbol: str,
                                    token_amount: int, cost_bnb: float,
                                    ai_score: float = 0, decimals: int = 18) -> bool:
        token_chk = Web3.to_checksum_address(token_addr)
        if token_chk in self.positions:
            logger.warning(f"⚠️ [Pancake] {symbol} 已在外盘追踪中，跳过")
            return True

        pos = PancakePosition(token_chk, symbol, token_amount, cost_bnb, ai_score, decimals)
        self.positions[token_chk] = pos
        self.total_migrated += 1

        # ★ v12.0: 立即持久化到数据库
        await self._persist_position(pos)
        await self._persist_stat("total_migrated")

        logger.info(f"🎓→🥞 [Pancake] {symbol} 迁移到外盘追踪 | 数量: {token_amount}")

        pos._task = asyncio.create_task(self._tracking_coroutine(token_chk))
        return True

    # ================= 核心追踪协程 (v12.0: try/finally + DB 同步) =================

    async def _tracking_coroutine(self, token_addr: str):
        pos = self.positions.get(token_addr)
        if not pos:
            return

        try:
            # ===== 第1步: 盲等建池 + 预授权 =====
            pos.status = PancakeStatus.BLIND_WAITING
            await self._persist_status(token_addr, PancakeStatus.BLIND_WAITING)

            asyncio.create_task(self._pre_approve_during_wait(token_addr))

            pool_found = await self._wait_for_pool(token_addr)

            if pos.cancel_flag:
                await self._handle_cancel(token_addr)
                return

            if not pool_found:
                self.total_timeout += 1
                await self._persist_stat("total_timeout")
                pos.status = PancakeStatus.ABANDONED
                await self._persist_status(token_addr, PancakeStatus.ABANDONED)
                await self.notify(
                    f"⏰ <b>外盘超时</b> | {pos.symbol}\n"
                    f"合约: {_addr_link(token_addr)}\n"
                    f"盲等 {BLIND_WAIT_TIMEOUT // 60} 分钟未建池\n"
                    f"成本: {pos.cost_bnb:.4f} BNB\n"
                    f"💡 /sell_ext {token_addr[:20]} 强平 | /ignore_ext {token_addr[:20]} 放生"
                )
                return

            # ===== 第2步: 确认池子 =====
            self.total_pool_found += 1
            await self._persist_stat("total_pool_found")
            pos.status = PancakeStatus.TRACKING
            pos.pool_found_time = time.time()
            wait_duration = int(pos.pool_found_time - pos.migrate_time)

            # ★ v12.0: 池子建立后立即持久化 pair_addr 等关键信息
            await self._persist_tracking(pos)

            approve_tag = "✅已授权" if pos.pre_approved else "⏳授权中"
            await self.notify(
                f"🥞 <b>外盘池子已建立!</b> | {pos.symbol}\n"
                f"合约: {_addr_link(token_addr)}\n"
                f"Pair: <code>{pos.pair_addr}</code>\n"
                f"等待时间: {wait_duration}s\n"
                f"预授权: {approve_tag}\n"
                f"成本: {pos.cost_bnb:.4f} BNB\n"
                f"🔗 <a href='https://pancakeswap.finance/swap?outputCurrency={token_addr}'>PancakeSwap</a>\n"
                f"开始 ATH 追踪 (回撤 {ATH_DRAWDOWN_THRESHOLD * 100:.0f}% 自动砸盘 | TTL {ATH_TRACKING_TTL // 3600}h)"
            )

            # 等预授权完成
            if not pos.pre_approved:
                for _ in range(10):
                    if pos.pre_approved or pos.cancel_flag:
                        break
                    await asyncio.sleep(1.5)

            # ===== 第3步: ATH 追踪 =====
            await self._track_ath(token_addr)

        except asyncio.CancelledError:
            logger.info(f"🛑 [Pancake] {pos.symbol} 追踪协程被取消")
        except Exception as e:
            logger.error(f"❌ [Pancake] {pos.symbol} 追踪异常: {e}", exc_info=True)
            pos.status = PancakeStatus.ABANDONED
            await self._persist_status(token_addr, PancakeStatus.ABANDONED)
            await self.notify(
                f"❌ <b>外盘追踪异常</b> | {pos.symbol}\n"
                f"合约: {_addr_link(token_addr)}\n"
                f"异常: {str(e)[:100]}\n"
                f"成本: {pos.cost_bnb:.4f} BNB\n"
                f"💡 /sell_ext {token_addr[:20]} 强平"
            )
        finally:
            # ★ v12.0: 铁血清理 — 无论如何都确保状态一致
            # 如果协程退出时仓位仍在内存且处于终态，执行清理
            final_pos = self.positions.get(token_addr)
            if final_pos and final_pos.status in (PancakeStatus.ABANDONED,):
                self._finalize(token_addr, 0.0)

    # ================= 闲时预授权 =================

    async def _pre_approve_during_wait(self, token_addr: str):
        pos = self.positions.get(token_addr)
        if not pos:
            return

        try:
            await asyncio.sleep(3)
            if pos.cancel_flag:
                return

            logger.info(f"🔑 [Pancake] {pos.symbol} 开始预授权 PancakeSwap...")
            success = await asyncio.wait_for(
                self.executor.approve_if_needed(
                    token_addr, router=self.executor.pancake_router
                ),
                timeout=APPROVE_TIMEOUT
            )
            if success:
                pos.pre_approved = True
                # ★ 持久化授权状态
                await self._persist_tracking(pos)
                logger.info(f"🔑 [Pancake] {pos.symbol} 预授权完成 ✅")
            else:
                logger.warning(f"⚠️ [Pancake] {pos.symbol} 预授权失败，砸盘时将重试")
        except asyncio.TimeoutError:
            logger.warning(f"⏰ [Pancake] {pos.symbol} 预授权超时 ({APPROVE_TIMEOUT}s)")
        except Exception as e:
            logger.warning(f"⚠️ [Pancake] {pos.symbol} 预授权异常: {e}")

    async def _wait_for_pool(self, token_addr: str) -> bool:
        pos = self.positions[token_addr]
        start = time.time()
        check_count = 0

        while time.time() - start < BLIND_WAIT_TIMEOUT:
            if pos.cancel_flag:
                return False

            check_count += 1
            try:
                alive, pair, token_res, wbnb_res = await asyncio.wait_for(
                    self.executor.is_pancake_pool_alive(token_addr),
                    timeout=POOL_CHECK_TIMEOUT
                )
            except asyncio.TimeoutError:
                await asyncio.sleep(BLIND_WAIT_INTERVAL)
                continue
            except Exception:
                await asyncio.sleep(BLIND_WAIT_INTERVAL)
                continue

            if alive and pair:
                pos.pair_addr = pair
                logger.info(
                    f"🥞 [Pancake] {pos.symbol} 池子已建立! "
                    f"pair={pair[:10]}... | WBNB={self.executor.w3.from_wei(wbnb_res, 'ether'):.4f}"
                )
                return True

            if pair and not alive:
                logger.debug(
                    f"🥞 [Pancake] {pos.symbol} 池子存在但流动性不足"
                )

            if check_count % 12 == 0:
                elapsed = int(time.time() - start)
                logger.info(
                    f"⏳ [Pancake] {pos.symbol} 盲等建池中... "
                    f"({elapsed}s / {BLIND_WAIT_TIMEOUT}s)"
                )

            await asyncio.sleep(BLIND_WAIT_INTERVAL)

        return False

    # ================= ATH 追踪 (v12.0: 周期性持久化 + 超时保护) =================

    async def _track_ath(self, token_addr: str):
        pos = self.positions[token_addr]
        check_count = 0
        last_db_sync = 0  # 上次 DB 同步时间
        DB_SYNC_INTERVAL = 60  # 每 60 秒同步一次追踪数据到 DB

        if pos.tracking_start_time <= 0:
            pos.tracking_start_time = time.time()
        pos.last_balance_check = time.time()

        # ★ 持久化追踪开始时间
        await self._persist_tracking(pos)

        while True:
            if pos.cancel_flag:
                await self._handle_cancel(token_addr)
                return

            check_count += 1
            now = time.time()

            # ===== TTL 超时强平 =====
            tracking_age = now - pos.tracking_start_time
            if tracking_age >= ATH_TRACKING_TTL:
                self.total_ttl_expired += 1
                await self._persist_stat("total_ttl_expired")
                value_bnb = 0.0
                if pos.current_price_bnb > 0:
                    value_bnb = pos.current_price_bnb * pos.token_amount / (10 ** WBNB_DECIMALS)
                pnl_est = value_bnb - pos.cost_bnb
                await self.notify(
                    f"⏰ <b>外盘 TTL 超时强平</b> | {pos.symbol}\n"
                    f"合约: {_addr_link(token_addr)}\n"
                    f"追踪: {tracking_age / 3600:.1f}h / {ATH_TRACKING_TTL // 3600}h\n"
                    f"残值: ~{value_bnb:.4f} BNB\n"
                    f"成本: {pos.cost_bnb:.4f} BNB\n"
                    f"预估盈亏: {pnl_est:+.4f} BNB\n"
                    f"操作: 强平清理..."
                )
                await self._execute_sell(token_addr, emergency=True)
                return

            # ===== 定期余额检查 =====
            if now - pos.last_balance_check >= BALANCE_CHECK_INTERVAL:
                pos.last_balance_check = now
                
                # 🟢 模拟盘跳过定期查额，防止误报代币消失
                is_dry_run = getattr(self.executor, 'dry_run', False)
                if is_dry_run:
                    logger.debug(f"🧪 [模拟盘] {pos.symbol} 跳过定期余额检查")
                else:
                    try:
                        real_bal = await asyncio.wait_for(
                            self.executor.get_token_balance(token_addr),
                            timeout=BALANCE_QUERY_TIMEOUT
                        )
                        if real_bal <= 0:
                            track_duration = int((now - pos.tracking_start_time) / 60)
                            await self.notify(
                                f"👻 <b>外盘代币消失</b> | {pos.symbol}\n"
                                f"合约: {_addr_link(token_addr)}\n"
                                f"链上余额=0 (可能被 rug/手动转走)\n"
                                f"成本: {pos.cost_bnb:.4f} BNB (全损)\n"
                                f"已追踪: {track_duration} 分钟\n"
                                f"自动清理..."
                            )
                            pos.status = PancakeStatus.DONE
                            await self._persist_status(token_addr, PancakeStatus.DONE)
                            self._finalize(token_addr, 0.0)
                            return
                        if real_bal != pos.token_amount:
                            old_amt = pos.token_amount
                            pos.token_amount = real_bal
                            if abs(old_amt - real_bal) > old_amt * 0.01:
                                logger.info(
                                    f"🔄 [Pancake] {pos.symbol} 余额同步: {old_amt} → {real_bal}"
                                )
                    except asyncio.TimeoutError:
                        logger.debug(f"⏰ [Pancake] {pos.symbol} 余额检查超时")
                    except Exception as e:
                        logger.debug(f"⚠️ [Pancake] {pos.symbol} 余额检查异常: {e}")

            # 查询当前价格 (加超时保护)
            try:
                price_bnb = await asyncio.wait_for(
                    self._get_token_price_bnb(token_addr),
                    timeout=PRICE_QUERY_TIMEOUT
                )
            except asyncio.TimeoutError:
                price_bnb = None
            except Exception:
                price_bnb = None

            if price_bnb is None or price_bnb <= 0:
                if check_count % 6 == 0:
                    logger.debug(f"⚠️ [Pancake] {pos.symbol} 价格查询失败")
                await asyncio.sleep(TRACKING_INTERVAL)
                continue

            pos.current_price_bnb = price_bnb

            if price_bnb > pos.ath_bnb:
                pos.ath_bnb = price_bnb
                pos.ath_time = time.time()

            if pos.ath_bnb > 0:
                pos.current_drawdown = 1.0 - (price_bnb / pos.ath_bnb)
            else:
                pos.current_drawdown = 0.0

            # ★ v12.0: 定期持久化追踪数据 (每 DB_SYNC_INTERVAL 秒)
            if now - last_db_sync >= DB_SYNC_INTERVAL:
                last_db_sync = now
                await self._persist_tracking(pos)

            # 心跳日志
            if check_count % 6 == 0:
                value_bnb = price_bnb * pos.token_amount / (10 ** WBNB_DECIMALS)
                ttl_remain = max(0, ATH_TRACKING_TTL - tracking_age)
                logger.info(
                    f"📊 [Pancake] {pos.symbol} | "
                    f"ATH: {pos.ath_bnb:.13f} | 当前: {price_bnb:.13f} | "
                    f"回撤: {pos.current_drawdown * 100:.1f}% | "
                    f"估值: {value_bnb:.4f} BNB | "
                    f"TTL: {ttl_remain / 3600:.1f}h"
                )

            # 触发砸盘
            if pos.current_drawdown >= ATH_DRAWDOWN_THRESHOLD and pos.ath_bnb > 0:
                value_bnb = price_bnb * pos.token_amount / (10 ** WBNB_DECIMALS)
                pnl_est = value_bnb - pos.cost_bnb
                
                # 获取实时 BNB 价格，失败则用默认值 600
                bnb_usd_price = self.price_oracle.get_price() if self.price_oracle else 600.0
                ath_mc_usd = (pos.ath_bnb * bnb_usd_price) * 1_000_000_000
                cur_mc_usd = (price_bnb * bnb_usd_price) * 1_000_000_000

                await self.notify(
                    f"📉 <b>外盘回撤砸盘触发!</b> | {pos.symbol}\n"
                    f"合约: {_addr_link(token_addr)}\n"
                    f"ATH市值: {fmt_mc(ath_mc_usd)}\n"
                    f"当前市值: {fmt_mc(cur_mc_usd)} (价格: ${price_bnb * bnb_usd_price:.7f})\n"
                    f"回撤: <b>{pos.current_drawdown * 100:.1f}%</b> (阈值 {ATH_DRAWDOWN_THRESHOLD * 100:.0f}%)\n"
                    f"估值: ~{value_bnb:.4f} BNB | 成本: {pos.cost_bnb:.4f} BNB\n"
                    f"预估盈亏: {pnl_est:+.4f} BNB\n"
                    f"操作: <b>市价全部卖出...</b>"
                )
                await self._execute_sell(token_addr, emergency=False)
                return

            await asyncio.sleep(TRACKING_INTERVAL)

    async def _get_token_price_bnb(self, token_addr: str) -> Optional[float]:
        pos = self.positions.get(token_addr)
        if not pos or not pos.pair_addr:
            return None

        try:
            token_res, wbnb_res = await self.executor.get_pancake_reserves(
                pos.pair_addr, token_addr
            )
            if token_res <= 0 or wbnb_res <= 0:
                return None
            return wbnb_res / token_res
        except Exception:
            return None

    # ================= 卖出执行 (v12.0: 全程超时 + DB 状态同步) =================

    async def _execute_sell(self, token_addr: str, emergency: bool = False):
        pos = self.positions.get(token_addr)
        if not pos:
            return

        pos.status = PancakeStatus.SELLING
        # ★ 卖出前立即持久化状态 — 即使此刻断电，重启后也知道正在卖出
        await self._persist_status(token_addr, PancakeStatus.SELLING)

        # 🟢 模拟盘拦截：如果是模拟盘，直接使用数据库里的虚拟余额，不去查链
        is_dry_run = getattr(self.executor, 'dry_run', False)
        
        if is_dry_run:
            real_balance = pos.token_amount
            logger.info(f"🧪 [模拟盘] 跳过链上查额，使用虚拟余额: {real_balance}")
        else:
            try:
                real_balance = await asyncio.wait_for(
                    self.executor.get_token_balance(token_addr),
                    timeout=BALANCE_QUERY_TIMEOUT
                )
            except (asyncio.TimeoutError, Exception):
                real_balance = pos.token_amount  # 降级使用内存值

        if real_balance <= 0:
            logger.warning(f"⚠️ [Pancake] {pos.symbol} 余额为0，跳过卖出")
            pos.status = PancakeStatus.DONE
            await self._persist_status(token_addr, PancakeStatus.DONE)
            await self.notify(
                f"⚠️ <b>外盘卖出跳过</b> | {pos.symbol}\n"
                f"原因: 链上代币余额为 0\n"
                f"成本: {pos.cost_bnb:.4f} BNB (全损)"
            )
            self._finalize(token_addr, 0.0)
            return

        sell_amount = min(real_balance, pos.token_amount)

        try:
            pre_sell_bnb = await asyncio.wait_for(
                self.executor.get_bnb_balance(),
                timeout=BALANCE_QUERY_TIMEOUT
            )
        except (asyncio.TimeoutError, Exception):
            pre_sell_bnb = 0.0

        value_bnb_est = 0.0
        if pos.current_price_bnb > 0:
            value_bnb_est = pos.current_price_bnb * sell_amount / (10 ** WBNB_DECIMALS)

        # ★ 卖出操作加总超时保护
        tx = None
        try:
            tx = await asyncio.wait_for(
                self.executor.sell_on_pancakeswap(
                    token_addr, sell_amount, min_bnb_out=0, emergency=emergency
                ),
                timeout=SELL_TIMEOUT
            )
        except asyncio.TimeoutError:
            logger.error(f"⏰ [Pancake] {pos.symbol} 卖出总超时 ({SELL_TIMEOUT}s)")
            tx = None
        except Exception as e:
            logger.error(f"❌ [Pancake] {pos.symbol} 卖出异常: {e}")
            tx = None

        if tx:
            gas_cost = self.executor.last_gas_cost_bnb

            await asyncio.sleep(1.5)
            try:
                post_sell_bnb = await asyncio.wait_for(
                    self.executor.get_bnb_balance(),
                    timeout=BALANCE_QUERY_TIMEOUT
                )
            except (asyncio.TimeoutError, Exception):
                post_sell_bnb = 0.0

            actual_realized = post_sell_bnb - pre_sell_bnb + gas_cost
            if actual_realized < 0:
                actual_realized = 0.0

            if post_sell_bnb <= 0 and pre_sell_bnb <= 0:
                actual_realized = value_bnb_est

            pos.realized_bnb = actual_realized
            pos.sell_tx = tx

            if emergency:
                self.total_force_sold += 1
                await self._persist_stat("total_force_sold")
            else:
                self.total_auto_sold += 1
                await self._persist_stat("total_auto_sold")

            net_pnl = actual_realized - pos.cost_bnb - gas_cost
            deviation_pct = 0.0
            if value_bnb_est > 0:
                deviation_pct = ((actual_realized - value_bnb_est) / value_bnb_est) * 100

            pnl_pct = (net_pnl / pos.cost_bnb * 100) if pos.cost_bnb > 0 else 0
            sell_type = "强平" if emergency else "砸盘"

            await self.notify(
                f"✅ <b>外盘{sell_type}成功</b> | {pos.symbol}\n"
                f"回收: <b>{actual_realized:.4f} BNB</b>"
                f"{f' (预估{value_bnb_est:.4f}, 偏差{deviation_pct:+.1f}%)' if abs(deviation_pct) > 1 else ''}\n"
                f"Gas: {gas_cost:.6f} BNB\n"
                f"净盈亏: <b>{net_pnl:+.4f} BNB</b> ({pnl_pct:+.1f}%)\n"
                f"TX: {_tx_link(tx)}\n"
                f"📊 外盘累计: 自动{self.total_auto_sold} | 强平{self.total_force_sold}"
            )
        else:
            await self.notify(
                f"❌ <b>外盘卖出构建失败</b> | {pos.symbol}\n"
                f"合约: {_addr_link(token_addr)}\n"
                f"估值: ~{value_bnb_est:.4f} BNB\n"
                f"💡 /sell_ext {token_addr[:20]} 重试"
            )
            pos.status = PancakeStatus.TRACKING
            await self._persist_status(token_addr, PancakeStatus.TRACKING)
            return

        pos.status = PancakeStatus.DONE
        await self._persist_status(token_addr, PancakeStatus.DONE)
        self._finalize(token_addr, pos.realized_bnb)

    def _finalize(self, token_addr: str, realized_bnb: float):
        """清理完成的仓位 — 从内存和数据库同时删除"""
        pos = self.positions.pop(token_addr, None)
        if pos:
            self.completed.append({
                "symbol": pos.symbol,
                "token_addr": token_addr,
                "cost_bnb": pos.cost_bnb,
                "realized_bnb": realized_bnb,
                "ath_bnb": pos.ath_bnb,
                "time": time.time(),
            })
            if len(self.completed) > 50:
                self.completed = self.completed[-50:]

        # ★ v12.0: 从数据库删除 (fire-and-forget, 不阻塞)
        try:
            loop = asyncio.get_running_loop()
            loop.run_in_executor(self._db_pool, self._db_delete_position, token_addr)
        except RuntimeError:
            # 没有运行中的事件循环（关闭时），同步删除
            try:
                self._db_delete_position(token_addr)
            except Exception:
                pass

    # ================= TG 控制接口 =================

    async def handle_cancel(self, token_addr: str):
        pos = self.positions.get(Web3.to_checksum_address(token_addr))
        if pos:
            pos.cancel_flag = True

    async def _handle_cancel(self, token_addr: str):
        pos = self.positions.get(token_addr)
        if not pos:
            return

        pos.status = PancakeStatus.ABANDONED
        self.total_abandoned += 1
        await self._persist_stat("total_abandoned")
        await self._persist_status(token_addr, PancakeStatus.ABANDONED)
        self._finalize(token_addr, 0.0)

    async def force_sell(self, token_addr: str):
        token_chk = Web3.to_checksum_address(token_addr)
        pos = self.positions.get(token_chk)
        if not pos:
            return "❌ 未找到该外盘仓位"

        if pos._task and not pos._task.done():
            pos.cancel_flag = True
            pos._task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(pos._task), timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                pass

        pos.cancel_flag = False  # 重置，允许卖出操作
        await self._execute_sell(token_chk, emergency=True)
        return f"✅ {pos.symbol} 强平指令已执行"

    async def ignore_position(self, token_addr: str):
        token_chk = Web3.to_checksum_address(token_addr)
        pos = self.positions.get(token_chk)
        if not pos:
            return "❌ 未找到该外盘仓位"

        if pos._task and not pos._task.done():
            pos.cancel_flag = True
            pos._task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(pos._task), timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                pass

        pos.status = PancakeStatus.ABANDONED
        self.total_abandoned += 1
        await self._persist_stat("total_abandoned")
        await self._persist_status(token_chk, PancakeStatus.ABANDONED)
        self._finalize(token_chk, 0.0)
        return f"✅ {pos.symbol} 已放生 (不再追踪)"

    # ================= v12.0: 优雅关闭 =================

    def shutdown(self):
        """关闭 DB 线程池，确保最后的写入完成"""
        # 先同步写入所有内存中的仓位
        for token_addr, pos in list(self.positions.items()):
            try:
                self._db_save_position(pos)
            except Exception as e:
                logger.error(f"❌ [Pancake] 关闭时保存失败: {pos.symbol} | {e}")

        self._db_pool.shutdown(wait=True)
        logger.info("🔌 [Pancake] DB 线程池已关闭")

    # ================= TG 状态查询 =================

    def get_positions_text(self) -> str:
        if not self.positions:
            return "📭 无外盘追踪中的代币"

        lines = [f"🥞 <b>外盘追踪中</b> ({len(self.positions)} 个) [持久化✅]\n"]

        for addr, pos in self.positions.items():
            status_emoji = {
                PancakeStatus.BLIND_WAITING: "⏳",
                PancakeStatus.TRACKING: "🟢",
                PancakeStatus.SELLING: "🔄",
                PancakeStatus.MIGRATING: "🔄",
            }.get(pos.status, "❓")

            elapsed = int(time.time() - pos.migrate_time)

            if pos.status == PancakeStatus.BLIND_WAITING:
                approve_tag = "✅" if pos.pre_approved else "⏳"
                lines.append(
                    f"{status_emoji} <b>{pos.symbol}</b> ({_addr_link(addr)})\n"
                    f"├ 状态: ⏳ 盲等建池中 (已等 {elapsed}s) 授权:{approve_tag}\n"
                    f"└ 操作: /sell_ext {addr[:20]} | /ignore_ext {addr[:20]}"
                )
            elif pos.status == PancakeStatus.TRACKING:
                drawdown_pct = pos.current_drawdown * 100
                if drawdown_pct < 20:
                    dd_tag = "安全 🟢"
                elif drawdown_pct < 40:
                    dd_tag = "注意 🟡"
                else:
                    dd_tag = f"危险 🔴 (阈值: {ATH_DRAWDOWN_THRESHOLD * 100:.0f}%)"

                value_bnb = pos.current_price_bnb * pos.token_amount / (
                            10 ** WBNB_DECIMALS) if pos.current_price_bnb > 0 else 0

                ttl_remain = 0
                if pos.tracking_start_time > 0:
                    ttl_remain = max(0, ATH_TRACKING_TTL - (time.time() - pos.tracking_start_time))
                ttl_text = f"{ttl_remain / 3600:.1f}h" if ttl_remain > 0 else "过期"

                pnl_est = value_bnb - pos.cost_bnb
                pnl_emoji = "📈" if pnl_est >= 0 else "📉"
                # 获取实时 BNB 价格，失败则用默认值 600
                bnb_usd_price = self.price_oracle.get_price() if self.price_oracle else 600.0

                # 1. 算出准确的美元单价
                cur_price_usd = pos.current_price_bnb * bnb_usd_price
                
                # 2. 算出准确的美元市值
                cur_mc_usd = cur_price_usd * 1_000_000_000
                ath_mc_usd = (pos.ath_bnb * bnb_usd_price) * 1_000_000_000

                current_mc_bnb = pos.current_price_bnb * 1_000_000_000

                lines.append(
                    f"{status_emoji} <b>{pos.symbol}</b> ({_addr_link(addr)})\n"
                    f"├ ATH市值: {fmt_mc(ath_mc_usd)}\n"
                    f"├ 当前市值: {fmt_mc(cur_mc_usd)} (价格: ${cur_price_usd:.7f})\n"
                    f"├ 回撤: {drawdown_pct:.1f}% ({dd_tag})\n"
                    f"├ 估值: {value_bnb:.4f} BNB | 成本: {pos.cost_bnb:.4f} BNB\n"
                    f"├ {pnl_emoji} 浮盈亏: {pnl_est:+.4f} BNB\n"
                    f"├ TTL: {ttl_text}\n"
                    f"└ 操作: /sell_ext {addr[:20]} (强平) | /ignore_ext {addr[:20]} (放生)"
                )
            else:
                lines.append(
                    f"{status_emoji} <b>{pos.symbol}</b> | 状态: {pos.status.value}"
                )

        return "\n".join(lines)

    def get_summary_text(self) -> str:
        active = len(self.positions)
        blind = sum(1 for p in self.positions.values() if p.status == PancakeStatus.BLIND_WAITING)
        tracking = sum(1 for p in self.positions.values() if p.status == PancakeStatus.TRACKING)
        return (
            f"🥞 <b>外盘管理器 v12.0</b> [持久化✅]\n"
            f"追踪中: {active} (盲等:{blind} 追踪:{tracking})\n"
            f"累计: 迁入={self.total_migrated} 建池={self.total_pool_found} "
            f"自动卖={self.total_auto_sold} 强平={self.total_force_sold} "
            f"放生={self.total_abandoned} 超时={self.total_timeout} "
            f"TTL过期={self.total_ttl_expired}"
        )

    def get_status_text(self) -> str:
        """兼容 TG 状态页调用。"""
        summary = self.get_summary_text()
        positions = self.get_positions_text()
        return f"{summary}\n\n{positions}"