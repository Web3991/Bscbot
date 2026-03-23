"""
bsc_main.py — 主控 / 流水线调度
"""

import os
import sys
import time
import json
import shutil
import signal
import asyncio
import sqlite3
import logging
import logging.handlers
import re as _re
import html as _html
import contextlib
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import aiohttp
from web3 import Web3

from rpc_fleet import RpcFleet
from bsc_detector import FourMemeDetector
from bsc_monitor import MomentumMonitor
from bsc_executor import BSCExecutor
from bsc_pancake import PancakeManager, PancakeStatus
from ai_narrative import AINarrativeEngine
from risk_manager import RiskGuard
from price_oracle import PriceOracle
from tg_controller import TelegramController
from dev_screener import DevScreener
from buyer_profiler import BuyerProfiler

# ================= 日志配置 =================
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "bsc_bot.log")
LOG_MAX_BYTES = 50 * 1024 * 1024
LOG_BACKUP_COUNT = 14
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

BOT_VERSION = "14.0"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    file_handler = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    for name in ["asyncio", "web3", "urllib3"]:
        logging.getLogger(name).setLevel(logging.WARNING)


setup_logging()
logger = logging.getLogger("BSC_Main")

# ================= 常量 =================
DB_FILE = "bsc_state.db"
SYS_CFG_FILE = "bsc_sys_config.json"
RISK_CFG_FILE = "bsc_risk_config.json"

ORPHAN_DEAD_POOL_MINUTES = 3
ORPHAN_MAX_AGE_MINUTES = 120
DEAD_POOL_SLOW_INTERVAL = 15
DEAD_POOL_TIMEOUT_SEC = 180
TIME_DECAY_SL_SEC = 90
TIME_DECAY_SL_MULT = 0.9
BALANCE_QUERY_RETRIES = 3
BALANCE_QUERY_DELAY = 1.5
GRADUATION_CONFIRM_SEC = 60
GRADUATION_PEAK_PCT_THRESHOLD = 85.0
MONITOR_SEM_TIMEOUT = 2.0
LAUNCH_WARNING_PCT = 98.0
BALANCE_RECHECK_DELAY = 1.5
BALANCE_FALLBACK_FACTOR = 0.85

SELL_MACHINE_MAX_CONSECUTIVE_ERRORS = 10
SELL_MACHINE_ERROR_SLEEP = 5
DEFAULT_THREAD_POOL_SIZE = 50
BUY_TAX_SYNC_DELAY = 2.0

BSCSCAN_TX_PREFIX = "https://bscscan.com/tx/"
BSCSCAN_ADDR_PREFIX = "https://bscscan.com/address/"
FOURMEME_TOKEN_PREFIX = "https://four.meme/token/"
LOW_BALANCE_WARNING_BNB = 0.15

STABLE_QUOTE_ADDRS = {
    # USD1 (当前 Four 模板常见)
    "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d",
    # BSC 常见稳定币
    "0x55d398326f99059ff775485246999027b3197955",  # USDT
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",  # USDC
    "0xe9e7cea3dedca5984780bafc599bd69add087d56",  # BUSD
    "0xc5f0f7b66764f6ec8c8dff7ba683102295e16409",  # FDUSD
}

WAL_CHECKPOINT_INTERVAL = 3600
RESOURCE_CLEANUP_INTERVAL = 600
BUYER_HISTORY_CLEANUP_INTERVAL = 4 * 3600
BUYER_HISTORY_RETENTION_DAYS = 7
STALE_SNIPE_TIMEOUT = 1800
STALE_LOCK_TIMEOUT = 7200

SELL_OPERATION_TIMEOUT = 120
BUY_OPERATION_TIMEOUT = 120
BALANCE_CHECK_TIMEOUT = 10
SELL_MACHINE_ABSOLUTE_TTL_SEC = 7 * 24 * 3600

# 批量通知：优先凑满 10 条再推，不足 10 条则 5 分钟兜底一次
BATCH_NOTIFY_INTERVAL = 300
BATCH_NOTIFY_MAX_QUEUE = 10


def _tx_link(tx_hash: str) -> str:
    if not tx_hash or tx_hash.startswith("dry_run"):
        return f"<code>{tx_hash}</code>"
    return f"<a href='{BSCSCAN_TX_PREFIX}{tx_hash}'>🔗 BSCScan</a>"


def _addr_link(addr: str, short: bool = True) -> str:
    display = f"{addr[:10]}...{addr[-6:]}" if short else addr
    return f"<a href='{BSCSCAN_ADDR_PREFIX}{addr}'>{display}</a>"


def _four_token_link(addr: str, short: bool = True) -> str:
    display = f"{addr[:10]}...{addr[-6:]}" if short else addr
    return f"<a href='{FOURMEME_TOKEN_PREFIX}{addr}'>{display}</a>"


# ================= 异步安全锁 =================
class CancellationSafeLock:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._created_at = time.time()

    def locked(self):
        return self._lock.locked()

    @property
    def age(self) -> float:
        return time.time() - self._created_at

    async def __aenter__(self):
        t = asyncio.get_running_loop().create_task(self._lock.acquire())
        try:
            await asyncio.shield(t)
        except asyncio.CancelledError:
            if t.done() and t.result():
                self._lock.release()
            elif not t.done():
                t.cancel()
            raise
        return self

    async def __aexit__(self, *_):
        self._lock.release()


# ================= 配置管理 =================
class ConfigManager:
    def __init__(self):
        self.pk = ""
        self.sys_cfg = {"GROK_API_KEY": "", "EXTRA_RPCS": [], "DRY_RUN": None, "GET_LOGS_SLOTS": 8}
        self.risk_cfg = {
            "BUY_BNB_AMT": 0.05, "HARD_CAP_BNB": 0.08,
            "MOMENTUM_BNB_THRESHOLD": 2.5, "MONITOR_DURATION_SEC": 60,
            "UP_TICKS_REQUIRED": 2, "MIN_UNIQUE_BUYERS": 5,
            "MAX_DEV_HOLDING_PCT": 12.0,
            # 卖压拦截（支持热更新）
            "SELL_PRESSURE_RATIO_PCT": 40.0,
            "SELL_PRESSURE_DEV_OUTFLOW_PCT": 5.0,
            "SELL_PRESSURE_LARGE_SELL_COUNT": 2,
            "SELL_PRESSURE_MIN_VOLUME_TOKENS": 0,
            "MAX_CONCURRENT_POSITIONS": 3,
            "MAX_EXTERNAL_TRACKING": 10,
            "MAX_CONCURRENT_MONITORS": 10, "GRADUATE_THRESHOLD_BNB": 24.0,
            "TRAILING_SL_PCT": 0.8, "HARD_SL_MULT": 0.7,
            "DOUBLE_SELL_MULT": 2.0, "DOUBLE_SELL_PCT": 0.5,
            "SIDEWAYS_EXIT_SEC": 3600,
            "SIDEWAYS_MIN_MULT": 0.95,
            "SIDEWAYS_MAX_MULT": 1.20,
            "AI_SCORE_THRESHOLD": 55, "AI_MULTIPLIER_HIGH": 1.5,
            "AI_MULTIPLIER_MID": 1.0, "AI_MULTIPLIER_LOW": 0.5,
            "DAILY_LOSS_LIMIT_BNB": -0.5, "DAILY_TRADE_LIMIT": 20,
            "MAX_CONSECUTIVE_LOSSES": 3, "COOLDOWN_MINUTES": 30,
            "MOMENTUM_CONFIRM_DELAY_SEC": 5, "MOMENTUM_CONFIRM_PULLBACK_BNB": 0.05,
        }
        self.load()

    def load(self):
        for f, t in [(SYS_CFG_FILE, self.sys_cfg), (RISK_CFG_FILE, self.risk_cfg)]:
            if os.path.exists(f):
                try:
                    with open(f) as fp:
                        t.update(json.load(fp))
                except Exception:
                    pass
        if pk := os.environ.get("BSC_PRIVATE_KEY"):
            self.pk = pk
        if gk := os.environ.get("GROK_API_KEY"):
            self.sys_cfg["GROK_API_KEY"] = gk

    def save_risk(self):
        try:
            with open(RISK_CFG_FILE + ".tmp", "w") as f:
                json.dump(self.risk_cfg, f, indent=4)
            os.replace(RISK_CFG_FILE + ".tmp", RISK_CFG_FILE)
        except Exception as e:
            logger.warning(f"⚠️ [Config] 保存失败: {e}")

    def save_sys(self):
        try:
            with open(SYS_CFG_FILE + ".tmp", "w") as f:
                json.dump(self.sys_cfg, f, indent=4)
            os.replace(SYS_CFG_FILE + ".tmp", SYS_CFG_FILE)
        except Exception as e:
            logger.warning(f"⚠️ [Config] 保存失败: {e}")

    def is_ready(self):
        return bool(self.pk)


# ================= SQLite 持仓管理 =================
class SQLiteStateManager:
    def __init__(self, db=DB_FILE):
        self._pool = ThreadPoolExecutor(1)
        self._conn = sqlite3.connect(db, check_same_thread=False)
        self._db_path = db
        c = self._conn
        c.execute("PRAGMA journal_mode=WAL;")
        c.execute("""CREATE TABLE IF NOT EXISTS positions (
            mint TEXT PRIMARY KEY, symbol TEXT, amt TEXT, orig_amt TEXT,
            sig TEXT, ts REAL, buy_bnb REAL, realized_bnb REAL,
            realized_cost REAL, narrative TEXT, status TEXT DEFAULT 'active',
            init_pool_bnb TEXT DEFAULT '0', ai_score REAL DEFAULT 0
        )""")
        for col_def in [
            "sell_stage INTEGER DEFAULT 0", "gas_cost REAL DEFAULT 0.0",
            "decimals INTEGER DEFAULT 18", "peak_curve_pct REAL DEFAULT 0.0",
            "effective_cost REAL DEFAULT 0.0",
        ]:
            try:
                c.execute(f"ALTER TABLE positions ADD COLUMN {col_def}")
            except Exception:
                pass
        c.commit()

    async def _run(self, fn):
        return await asyncio.get_running_loop().run_in_executor(self._pool, fn)

    def _write(self, ops):
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            ops(self._conn)
            self._conn.execute("COMMIT")
        except Exception as e:
            logger.error(f"❌ [DB] 写入异常: {e}")
            try:
                self._conn.execute("ROLLBACK")
            except Exception:
                pass
            raise

    def _row(self, sql, p=()):
        c = self._conn.cursor()
        c.row_factory = sqlite3.Row
        c.execute(sql, p)
        return c.fetchone()

    def _rows(self, sql, p=()):
        c = self._conn.cursor()
        c.row_factory = sqlite3.Row
        c.execute(sql, p)
        return c.fetchall()

    async def get_active(self):
        return await self._run(
            lambda: {r["mint"]: dict(r) for r in self._rows("SELECT * FROM positions WHERE status='active'")})

    async def add_pos(self, mint, symbol, amt, sig, cost, narrative, init_bnb,
                      ai_score=0, gas_cost=0.0, decimals=18, effective_cost=0.0):
        eff = effective_cost if effective_cost > 0 else cost
        await self._run(lambda: self._write(
            lambda c: c.execute(
                "INSERT OR REPLACE INTO positions "
                "(mint,symbol,amt,orig_amt,sig,ts,buy_bnb,realized_bnb,realized_cost,"
                "narrative,status,init_pool_bnb,ai_score,sell_stage,gas_cost,decimals,"
                "peak_curve_pct,effective_cost) "
                "VALUES (?,?,?,?,?,?,?,0.0,0.0,?,'active',?,?,0,?,?,0.0,?)",
                (mint, symbol, str(amt), str(amt), sig, time.time(), cost,
                 narrative, str(init_bnb), ai_score, gas_cost, decimals, eff))))

    async def update_sell_stage(self, mint, stage):
        await self._run(lambda: self._write(
            lambda c: c.execute("UPDATE positions SET sell_stage=? WHERE mint=?", (stage, mint))))

    async def update_peak_curve_pct(self, mint, pct):
        await self._run(lambda: self._write(
            lambda c: c.execute("UPDATE positions SET peak_curve_pct = MAX(COALESCE(peak_curve_pct,0),?) WHERE mint=?", (pct, mint))))

    async def add_gas_cost(self, mint, gas_bnb):
        await self._run(lambda: self._write(
            lambda c: c.execute("UPDATE positions SET gas_cost = gas_cost + ? WHERE mint=?", (gas_bnb, mint))))

    async def update_amt(self, mint, new_amt):
        await self._run(lambda: self._write(
            lambda c: c.execute("UPDATE positions SET amt=? WHERE mint=?", (str(new_amt), mint))))

    async def mark_abnormal(self, mint, reason: str):
        tag = f" | ABNORMAL:{reason}" if reason else " | ABNORMAL"
        await self._run(lambda: self._write(
            lambda c: c.execute(
                "UPDATE positions SET status='abnormal', narrative=COALESCE(narrative,'') || ? WHERE mint=?",
                (tag, mint)
            )
        ))

    async def realize(self, mint, deducted, gained, cost_slice):
        def _op():
            def ops(c):
                r = c.cursor().execute(
                    "SELECT amt, realized_bnb, realized_cost FROM positions WHERE mint=?", (mint,)).fetchone()
                if r:
                    new_amt = max(0, int(r[0]) - deducted)
                    c.execute("UPDATE positions SET amt=?, realized_bnb=?, realized_cost=? WHERE mint=?",
                              (str(new_amt), r[1] + gained, r[2] + cost_slice, mint))
            self._write(ops)
        await self._run(_op)

    async def finalize(self, mint):
        def _op():
            res = [None]
            def ops(c):
                r = self._row("SELECT * FROM positions WHERE mint=?", (mint,))
                if r:
                    res[0] = dict(r)
                    c.execute("DELETE FROM positions WHERE mint=?", (mint,))
            self._write(ops)
            return res[0]
        return await self._run(_op)

    def wal_checkpoint(self):
        try:
            self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        except Exception as e:
            logger.warning(f"⚠️ [DB] WAL checkpoint 失败: {e}")

    def shutdown(self):
        self.wal_checkpoint()
        self._pool.shutdown(wait=True)
        self._conn.close()


# ================= v14: 批量通知队列 =================
class BatchNotifier:
    """把拦截类通知攒起来批量发，减少 TG 刷屏。"""

    CATEGORY_META = {
        "dev_block": {"emoji": "🔍", "label": "Dev审查拦截"},
        "ai_block": {"emoji": "🧠", "label": "AI评分拦截"},
        "profiler_block": {"emoji": "🕵️", "label": "买家侧写拦截"},
        "monitor_timeout": {"emoji": "⏰", "label": "综合观察未达标"},
        "risk_block": {"emoji": "🛡️", "label": "风控总闸拦截"},
        "capacity_block": {"emoji": "📦", "label": "仓位上限拦截"},
        "recheck_block": {"emoji": "📉", "label": "二次回撤拦截"},
        "balance_block": {"emoji": "💸", "label": "余额不足拦截"},
    }

    def __init__(self, tg, interval=BATCH_NOTIFY_INTERVAL, max_queue=BATCH_NOTIFY_MAX_QUEUE, context_cb=None):
        self.tg = tg
        self.interval = interval
        self.max_queue = max_queue
        self.context_cb = context_cb
        self._queue = []
        self._task = None

    def start(self):
        if self._task is None:
            self._task = asyncio.create_task(self._flush_loop())

    def add(self, category: str, text=None, **kwargs):
        """添加一条结构化拦截通知到队列。兼容旧版 text 调用。"""
        if kwargs:
            entry = {"category": category, **kwargs}
        else:
            entry = {"category": category, "summary": text or ""}
        self._queue.append(entry)
        if len(self._queue) >= self.max_queue:
            asyncio.create_task(self._flush())

    def _fmt_entry(self, item: dict) -> str:
        meta = self.CATEGORY_META.get(item.get("category"), {"emoji": "❌", "label": item.get("category", "未知")})
        symbol = _html.escape(str(item.get("symbol") or "?"))
        token = _html.escape(str(item.get("token") or ""))
        reason = item.get("reason") or item.get("summary") or ""
        stage = item.get("stage")
        extra = item.get("extra")
        concise_categories = {"dev_block", "monitor_timeout"}
        if extra and item.get("category") not in concise_categories:
            reason = f"{reason} | {extra}" if reason else str(extra)
        stage_safe = _html.escape(str(stage)) if stage else ""
        reason_safe = _html.escape(str(reason)) if reason else ""
        lines = [f"• {meta['emoji']} <b>{meta['label']}</b> | <b>{symbol}</b> | <code>{token or '-'}</code>"]
        if stage_safe:
            lines.append(f"  阶段: {stage_safe}")
        if reason_safe:
            lines.append(f"  原因: {reason_safe}")
        return "\n".join(lines)

    async def _flush_loop(self):
        while True:
            await asyncio.sleep(self.interval)
            await self._flush()

    async def _flush(self):
        if not self._queue:
            return
        batch = self._queue[:self.max_queue]
        self._queue = self._queue[self.max_queue:]

        summary_parts = []
        batch_counts = {}
        for item in batch:
            c = item.get("category", "unknown")
            batch_counts[c] = batch_counts.get(c, 0) + 1
        for k, v in batch_counts.items():
            meta = self.CATEGORY_META.get(k, {"emoji": "❌", "label": k})
            summary_parts.append(f"{meta['emoji']} {meta['label']}: {v}")

        holdings_text = ""
        if self.context_cb:
            try:
                ctx = await self.context_cb()
                holdings_text = f"\n💼 内盘:{ctx.get('inner', 0)} | 外盘:{ctx.get('outer', 0)}"
            except Exception:
                holdings_text = ""

        msg = f"📊 <b>拦截摘要</b> ({len(batch)}条){holdings_text}\n" + " | ".join(summary_parts)

        if batch:
            msg += "\n━━━ 最近拦截明细 ━━━\n"
            for item in batch[-10:]:
                msg += self._fmt_entry(item) + "\n"

        if self.tg:
            try:
                await self.tg.send_alert(msg)
            except Exception:
                pass

    async def shutdown(self):
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        with contextlib.suppress(Exception):
            await self._flush()


# ================= 总指挥 =================
class BotOrchestrator:
    def __init__(self, cfg: ConfigManager, tg: TelegramController, dry_run: bool):
        self.cfg = cfg
        self.tg = tg
        self.dry_run = dry_run
        self.paused = False
        self.virtual_bnb = 1.0
        self.emergency_mode = False

        db_suffix = "_dry" if dry_run else ""
        self.state_mgr = SQLiteStateManager(db=f"bsc_state{db_suffix}.db")
        self.risk = self._new_risk_guard(dry_run)
        self.ai = AINarrativeEngine(
            api_key=cfg.sys_cfg.get("GROK_API_KEY"),
            base_buy=cfg.risk_cfg["BUY_BNB_AMT"],
            hard_cap=cfg.risk_cfg["HARD_CAP_BNB"])
        self.ai.update_config(
            multiplier_high=cfg.risk_cfg.get("AI_MULTIPLIER_HIGH"),
            multiplier_mid=cfg.risk_cfg.get("AI_MULTIPLIER_MID"),
            multiplier_low=cfg.risk_cfg.get("AI_MULTIPLIER_LOW"),
            score_threshold=cfg.risk_cfg.get("AI_SCORE_THRESHOLD"))

        self.exec = None
        self.fleet = None
        self.detector = None
        self.monitor = None
        self.price_oracle = None
        self.pancake_mgr = None
        self.http_session = None
        self.dev_screener = None
        self.buyer_profiler = None
        self.batch_notifier = None

        self.active_snipes = set()
        self._snipe_timestamps = {}
        self.launch_positions = set()
        self.graduated_positions = set()
        self.sell_locks = {}
        self.monitor_sem = asyncio.Semaphore(cfg.risk_cfg.get("MAX_CONCURRENT_MONITORS", 10))

        self._wal_task = None
        self._cleanup_task = None
        self._bg_tasks = set()
        self._last_buyer_history_cleanup_ts = 0.0
        self._buy_gate_lock = asyncio.Lock()
        self._buy_reservations = set()
        self._ext_cap_warn_ts = {}
        self._quote_price_cache = {}

    def _db_suffix(self, dry_run: bool = None) -> str:
        flag = self.dry_run if dry_run is None else dry_run
        return "_dry" if flag else ""

    def _state_db_path(self, dry_run: bool = None) -> str:
        return f"bsc_state{self._db_suffix(dry_run)}.db"

    def _risk_db_path(self, dry_run: bool = None) -> str:
        return f"risk_stats{self._db_suffix(dry_run)}.db"

    def _buyer_history_db_path(self, dry_run: bool = None) -> str:
        return f"buyer_history{self._db_suffix(dry_run)}.db"

    def _pancake_db_path(self, dry_run: bool = None) -> str:
        return f"pancake_positions{self._db_suffix(dry_run)}.db"

    def _new_risk_guard(self, dry_run: bool):
        return RiskGuard(
            db_path=self._risk_db_path(dry_run),
            config={k: self.cfg.risk_cfg[k] for k in [
                "DAILY_LOSS_LIMIT_BNB", "DAILY_TRADE_LIMIT",
                "MAX_CONSECUTIVE_LOSSES", "COOLDOWN_MINUTES",
                "TRAILING_SL_PCT", "HARD_SL_MULT",
                "DOUBLE_SELL_PCT", "GRADUATE_THRESHOLD_BNB"
            ]})

    def _spawn_task(self, coro):
        task = asyncio.create_task(coro)
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)
        return task

    def _ensure_pancake_db_layout(self, dry_run: bool):
        target = self._pancake_db_path(dry_run)
        legacy = "pancake_positions.db"
        if dry_run and not os.path.exists(target) and os.path.exists(legacy):
            try:
                shutil.copy2(legacy, target)
                logger.info(f"📦 [ModeSwitch] 兼容复制外盘库: {legacy} -> {target}")
            except Exception as e:
                logger.warning(f"⚠️ [ModeSwitch] 外盘库兼容复制失败: {e}")
        return target

    async def _batch_notify_context(self):
        inner = 0
        outer = 0
        try:
            active = await self.state_mgr.get_active()
            inner = len(active)
        except Exception:
            pass
        try:
            outer = len(self.pancake_mgr.positions) if self.pancake_mgr else 0
        except Exception:
            pass
        return {"inner": inner, "outer": outer}

    async def _assert_mode_switch_safe(self):
        active = await self.state_mgr.get_active()
        if active:
            return False, f"❌ 当前仍有 {len(active)} 个内盘持仓，禁止热切换"
        ext_count = len(self.pancake_mgr.positions) if self.pancake_mgr else 0
        if ext_count > 0:
            return False, f"❌ 当前仍有 {ext_count} 个外盘追踪仓位，禁止热切换"
        if self.active_snipes:
            return False, f"❌ 当前仍有 {len(self.active_snipes)} 个监控/候选目标，禁止热切换"
        if any(lock.locked() for lock in self.sell_locks.values()):
            return False, "❌ 当前存在卖出中的仓位，禁止热切换"
        return True, ""

    def sync_config(self):
        """v15: 全链路配置同步 — AI/Risk/DevScreener/Profiler"""
        r = self.cfg.risk_cfg
        self.ai.update_config(
            base_buy=r.get("BUY_BNB_AMT"), hard_cap=r.get("HARD_CAP_BNB"),
            multiplier_high=r.get("AI_MULTIPLIER_HIGH"), multiplier_mid=r.get("AI_MULTIPLIER_MID"),
            multiplier_low=r.get("AI_MULTIPLIER_LOW"), score_threshold=r.get("AI_SCORE_THRESHOLD"))
        self.risk.update_config(
            DAILY_LOSS_LIMIT_BNB=r.get("DAILY_LOSS_LIMIT_BNB"), DAILY_TRADE_LIMIT=r.get("DAILY_TRADE_LIMIT"),
            MAX_CONSECUTIVE_LOSSES=r.get("MAX_CONSECUTIVE_LOSSES"), COOLDOWN_MINUTES=r.get("COOLDOWN_MINUTES"),
            TRAILING_SL_PCT=r.get("TRAILING_SL_PCT"), HARD_SL_MULT=r.get("HARD_SL_MULT"),
            DOUBLE_SELL_PCT=r.get("DOUBLE_SELL_PCT"), GRADUATE_THRESHOLD_BNB=r.get("GRADUATE_THRESHOLD_BNB"))
        # ★ v15: 同步 DevScreener 和 Profiler 参数
        if self.dev_screener:
            dev_keys = {k: r[k] for k in r if k.startswith("DEV_") and k in self.dev_screener.cfg}
            if dev_keys:
                self.dev_screener.update_config(**dev_keys)
        if self.buyer_profiler:
            prof_keys = {k: r[k] for k in r if (k.startswith("PROFILER_") or k.startswith("SCORE_")) and k in self.buyer_profiler.cfg}
            if prof_keys:
                self.buyer_profiler.update_config(**prof_keys)

    async def _notify(self, text):
        logger.info(_re.sub(r'<[^>]+>', '', text))
        if self.tg:
            asyncio.create_task(self.tg.send_alert(text))

    async def _reserve_buy_slot(self, token: str) -> tuple[bool, str]:
        """原子预占买入名额，修复 can_buy→record_buy TOCTOU。"""
        async with self._buy_gate_lock:
            if token in self._buy_reservations:
                return False, "该目标已在买入队列中"

            can_buy, reason = self.risk.can_buy(token)
            if not can_buy:
                return False, reason

            daily = self.risk.get_daily_stats()
            trades = int(daily.get("trades", 0) or 0)
            trade_limit = int(self.cfg.risk_cfg.get("DAILY_TRADE_LIMIT", 100) or 100)
            effective = trades + len(self._buy_reservations)
            if effective >= trade_limit:
                return False, f"日出手预占已满: {effective}/{trade_limit}"

            self._buy_reservations.add(token)
            return True, ""

    async def _release_buy_slot(self, token: str):
        async with self._buy_gate_lock:
            self._buy_reservations.discard(token)

    async def _ensure_external_capacity(self, token: str, sym: str = "", cooldown_sec: int = 300) -> bool:
        """外盘追踪容量检查（超限时节流告警）。"""
        if not self.pancake_mgr:
            return False

        try:
            token_chk = Web3.to_checksum_address(token)
        except Exception:
            token_chk = token

        if token_chk in self.pancake_mgr.positions:
            return True

        limit = int(self.cfg.risk_cfg.get("MAX_EXTERNAL_TRACKING", 10) or 10)
        limit = max(1, limit)
        current = len(self.pancake_mgr.positions)
        if current < limit:
            return True

        now = time.time()
        last = float(self._ext_cap_warn_ts.get(token_chk, 0) or 0)
        if now - last >= cooldown_sec:
            self._ext_cap_warn_ts[token_chk] = now
            await self._notify(
                f"📦 <b>外盘追踪槽位已满，暂缓迁移</b> | {sym or token[:10]}\n"
                f"合约: {_four_token_link(token)}\n"
                f"当前: {current}/{limit}\n"
                f"动作: 保留内盘协程，稍后自动重试迁移"
            )
        return False

    def _get_bnb_usd(self) -> float:
        return self.price_oracle.get_price() if self.price_oracle else 600.0

    async def _get_quote_bnb_price(self, quote_addr: str, ttl_sec: int = 30) -> float:
        if not quote_addr or not self.exec:
            return 0.0
        addr = quote_addr.lower()
        now = time.time()
        cached = self._quote_price_cache.get(addr)
        if cached and (now - cached[0]) <= ttl_sec:
            return float(cached[1] or 0.0)

        price = 0.0
        try:
            pair = await self.exec.get_pancake_pair(quote_addr)
            if pair:
                quote_res, wbnb_res = await self.exec.get_pancake_reserves(pair, quote_addr)
                if quote_res > 0 and wbnb_res > 0:
                    price = float(wbnb_res) / float(quote_res)
        except Exception:
            price = 0.0

        self._quote_price_cache[addr] = (now, price)
        return price

    async def _fmt_market_cap(self, pool_bnb: float, graduate_thr: float = 24.0,
                              pool_tokens: int = 0, total_supply: int = 0,
                              token_addr: str = None) -> str:
        last_price_wei = 0
        quote_is_native = True
        quote_addr = ZERO_ADDRESS

        if token_addr and self.monitor:
            meta = self.monitor.get_curve_meta(token_addr)
            if meta:
                total_supply = total_supply or int(meta.get("total_supply", 0) or 0)
                last_price_wei = int(meta.get("last_price_wei", 0) or 0)
                quote_is_native = bool(meta.get("quote_is_native", True))
                quote_addr = str(meta.get("quote_addr", ZERO_ADDRESS) or ZERO_ADDRESS)

        bnb_usd = max(self._get_bnb_usd(), 1e-9)

        mc_bnb = 0.0
        mc_usd = 0.0

        # 优先链上口径：lastPrice * totalSupply
        if last_price_wei > 0 and total_supply > 0:
            if quote_is_native:
                mc_bnb = (float(last_price_wei) * float(total_supply)) / 1e36
                mc_usd = mc_bnb * bnb_usd
            else:
                quote_mc = (float(last_price_wei) * float(total_supply)) / 1e36
                q = quote_addr.lower()
                if q in STABLE_QUOTE_ADDRS:
                    # 稳定币模板：quote 单位按 USD 处理
                    mc_usd = quote_mc
                    mc_bnb = mc_usd / bnb_usd
                else:
                    # 非稳定币模板：先换算 quote->BNB
                    quote_bnb = await self._get_quote_bnb_price(quote_addr)
                    if quote_bnb > 0:
                        mc_bnb = quote_mc * quote_bnb
                        mc_usd = mc_bnb * bnb_usd
                    else:
                        # 无法定价 quote，回落到旧口径避免直接空白
                        mc_bnb = BSCExecutor.calc_market_cap_bnb(
                            pool_bnb, graduate_thr,
                            pool_tokens=pool_tokens,
                            total_supply=total_supply,
                            last_price_wei=0
                        )
                        mc_usd = mc_bnb * bnb_usd
        else:
            mc_bnb = BSCExecutor.calc_market_cap_bnb(
                pool_bnb, graduate_thr,
                pool_tokens=pool_tokens,
                total_supply=total_supply,
                last_price_wei=0
            )
            mc_usd = mc_bnb * bnb_usd

        if mc_usd >= 1_000_000:
            return f"~{mc_bnb:.2f} BNB (~${mc_usd/1_000_000:.2f}M)"
        elif mc_usd >= 1000:
            return f"~{mc_bnb:.2f} BNB (~${mc_usd/1000:.1f}K)"
        else:
            return f"~{mc_bnb:.4f} BNB (~${mc_usd:.0f})"

    async def _fmt_tokens(self, token_addr, raw_amount):
        try:
            decimals = await self.exec.get_token_decimals(token_addr)
            return BSCExecutor.format_token_amount(raw_amount, decimals)
        except Exception:
            return BSCExecutor.format_token_amount(raw_amount, 18)

    async def _query_token_balance_with_retry(self, token_addr, expected_tokens=0):
        token_chk = Web3.to_checksum_address(token_addr)
        for attempt in range(BALANCE_QUERY_RETRIES):
            try:
                bal = await asyncio.wait_for(
                    self.exec.get_token_balance(token_chk, use_paid=True), timeout=BALANCE_CHECK_TIMEOUT)
                if bal > 0:
                    return bal
            except (asyncio.TimeoutError, Exception):
                pass
            if attempt < BALANCE_QUERY_RETRIES - 1:
                await asyncio.sleep(BALANCE_QUERY_DELAY)
        if expected_tokens > 0:
            return int(expected_tokens * BALANCE_FALLBACK_FACTOR)
        return 0

    async def _count_real_positions(self):
        active = await self.state_mgr.get_active()
        count = 0
        for mint, pos in active.items():
            if self._is_orphan_candidate(pos):
                continue
            if mint in self.launch_positions or mint in self.graduated_positions:
                continue
            count += 1
        return count

    async def _handoff_to_external(self, mint: str):
        """把仓位从内盘状态库移交给外盘管理，不计入内盘平仓统计。"""
        with contextlib.suppress(Exception):
            await self.state_mgr.finalize(mint)
        self.active_snipes.discard(mint)
        self._snipe_timestamps.pop(mint, None)
        self.launch_positions.discard(mint)
        self.graduated_positions.discard(mint)
        # lock 在当前协程退出后会自然释放；此处不强行 pop，避免上下文中途失效

    def _is_orphan_candidate(self, pos):
        now = time.time()
        age_min = (now - pos.get('ts', now)) / 60
        ai_score = pos.get('ai_score', 0)
        init_bnb = float(pos.get('init_pool_bnb', 0))
        sell_stage = pos.get('sell_stage', 0) or 0
        if sell_stage >= 2:
            return False
        if age_min > ORPHAN_MAX_AGE_MINUTES and ai_score <= 0 and init_bnb <= 0:
            return True
        return False

    # ================= 维护任务 =================

    async def _wal_checkpoint_loop(self):
        while True:
            await asyncio.sleep(WAL_CHECKPOINT_INTERVAL)
            try:
                await asyncio.get_running_loop().run_in_executor(None, self.state_mgr.wal_checkpoint)
            except Exception as e:
                logger.warning(f"⚠️ WAL checkpoint 异常: {e}")

    async def _resource_cleanup_loop(self):
        while True:
            await asyncio.sleep(RESOURCE_CLEANUP_INTERVAL)
            try:
                now = time.time()
                active_db = await self.state_mgr.get_active()
                active_mints = set(active_db.keys())

                stale_snipes = [m for m in list(self.active_snipes)
                                if self._snipe_timestamps.get(m, 0) > 0
                                and now - self._snipe_timestamps.get(m, 0) > STALE_SNIPE_TIMEOUT
                                and m not in active_mints]
                for m in stale_snipes:
                    self.active_snipes.discard(m)
                    self._snipe_timestamps.pop(m, None)

                stale_locks = [m for m in list(self.sell_locks)
                               if m not in active_mints and not self.sell_locks[m].locked()
                               and self.sell_locks[m].age > STALE_LOCK_TIMEOUT]
                for m in stale_locks:
                    self.sell_locks.pop(m, None)

                for s in [self.launch_positions, self.graduated_positions]:
                    for m in [m for m in s if m not in active_mints]:
                        s.discard(m)

                total = len(stale_snipes) + len(stale_locks)
                if total > 0:
                    logger.info(f"🧹 [Cleanup] 清理: snipes={len(stale_snipes)} locks={len(stale_locks)}")

                # 买家历史定期清理：低频执行，避免频繁写锁
                if self.buyer_profiler:
                    if (now - self._last_buyer_history_cleanup_ts) >= BUYER_HISTORY_CLEANUP_INTERVAL:
                        deleted = await self.buyer_profiler.cleanup_old_history(days=BUYER_HISTORY_RETENTION_DAYS)
                        self._last_buyer_history_cleanup_ts = now
                        if deleted > 0:
                            logger.info(
                                f"🧹 [Cleanup] buyer_history 清理完成: -{deleted} 条 (>{BUYER_HISTORY_RETENTION_DAYS}天)"
                            )
            except Exception as e:
                logger.warning(f"⚠️ [Cleanup] 异常: {e}")

    # ================= 低余额预警 =================

    async def _check_low_balance_warning(self):
        if self.dry_run:
            return
        try:
            bal = await asyncio.wait_for(self.exec.get_bnb_balance(), timeout=BALANCE_CHECK_TIMEOUT)
            if bal < LOW_BALANCE_WARNING_BNB:
                bnb_usd = self._get_bnb_usd()
                await self._notify(
                    f"⚠️ <b>余额预警!</b>\n"
                    f"余额: <b>{bal:.4f} BNB</b> (~${bal * bnb_usd:.2f})\n"
                    f"低于 {LOW_BALANCE_WARNING_BNB} BNB，请充值!")
        except Exception:
            pass

    # ================= 孤儿检测/清理 (简化) =================

    async def detect_orphan_positions(self):
        active = await self.state_mgr.get_active()
        orphans = {}
        now = time.time()
        for mint, pos in active.items():
            age_min = int((now - pos.get('ts', now)) / 60)
            pool_bnb, pool_tok = await self.monitor.get_pool_state(mint)
            if pool_bnb < 0:
                continue
            init_bnb = float(pos.get('init_pool_bnb', 0))
            mult = self.exec.calc_price_mult(init_bnb, pool_bnb) if init_bnb > 0 else 0.0
            reason = None
            if pool_bnb <= 0.001 and age_min >= ORPHAN_DEAD_POOL_MINUTES:
                sell_stage = pos.get('sell_stage', 0) or 0
                if sell_stage >= 2 or mint in self.graduated_positions:
                    continue
                reason = f"死池 ({pool_bnb:.4f} BNB)"
            elif mult <= 0.01 and age_min >= ORPHAN_MAX_AGE_MINUTES:
                reason = f"倍数 {mult:.2f}x 超时"
            if reason:
                orphans[mint] = {'symbol': pos.get('symbol', 'UNK'), 'age_min': age_min,
                                 'pool_bnb': pool_bnb, 'buy_bnb': pos.get('buy_bnb', 0), 'reason': reason}
        return orphans

    async def clean_orphan_positions(self):
        orphans = await self.detect_orphan_positions()
        count = 0
        for mint, info in orphans.items():
            pos = await self.state_mgr.finalize(mint)
            self.active_snipes.discard(mint)
            self._snipe_timestamps.pop(mint, None)
            self.launch_positions.discard(mint)
            self.graduated_positions.discard(mint)
            self.sell_locks.pop(mint, None)
            if pos:
                cost = pos.get('buy_bnb', 0)
                realized = pos.get('realized_bnb', 0)
                gas = pos.get('gas_cost', 0) or 0
                self.risk.record_trade(mint, realized - cost - gas, is_orphan=True)
            count += 1
        if count > 0:
            await self._notify(f"🧹 清理 <b>{count}</b> 个孤儿仓位")
        return count

    # ================= 启动 (v14: 只初始化一次，无 LazyPool) =================

    async def startup(self, priv_key):
        self.http_session = aiohttp.ClientSession()
        self.fleet = RpcFleet(
            extra_rpcs=self.cfg.sys_cfg.get("EXTRA_RPCS"),
            heavy_slots=self.cfg.sys_cfg.get("GET_LOGS_SLOTS", 8),
        )
        self.exec = BSCExecutor(priv_key, self.fleet)
        self.exec.dry_run = self.dry_run  # 显式传递
        await self.exec.initialize()

        self.monitor = MomentumMonitor(self.fleet)
        self.detector = FourMemeDetector(self.fleet, self._on_new)
        self.price_oracle = PriceOracle(self.fleet)
        pancake_db_path = self._ensure_pancake_db_layout(self.dry_run)
        self.pancake_mgr = PancakeManager(self.exec, self._notify, self.price_oracle, db_path=pancake_db_path)

        # ★ v14: 风控模块只初始化一次
        self.dev_screener = DevScreener(fleet=self.fleet, risk_guard=self.risk)
        self.buyer_profiler = BuyerProfiler(
            fleet=self.fleet,
            risk_guard=self.risk,
            db_path=self._buyer_history_db_path(self.dry_run),
            legacy_db_path=self._risk_db_path(self.dry_run),
        )

        # ★ v14: 批量通知
        self.batch_notifier = BatchNotifier(self.tg, context_cb=self._batch_notify_context)
        self.batch_notifier.start()

        self.fleet.start_health_check()
        self.price_oracle.start()
        self._spawn_task(self.detector.run_loop())

        self._wal_task = self._spawn_task(self._wal_checkpoint_loop())
        self._cleanup_task = self._spawn_task(self._resource_cleanup_loop())

        real_bal = await self.exec.get_bnb_balance()
        if self.dry_run:
            self.virtual_bnb = 10.0

        # 恢复内盘持仓
        active = await self.state_mgr.get_active()
        restored = 0
        for mint, pos in active.items():
            if self._is_orphan_candidate(pos):
                continue

            # v16: 启动恢复时优先检查链上状态，已毕业直接迁移外盘
            force_migrate = False
            try:
                await self.monitor.get_pool_state(mint, ttl=0.0)
                meta = self.monitor.get_curve_meta(mint) if self.monitor else {}
                if int(meta.get("status", 0) or 0) == 3:
                    force_migrate = True
            except Exception:
                force_migrate = False

            restore_cost = pos.get('effective_cost', 0) or pos.get('buy_bnb', 0)
            if force_migrate:
                can_migrate = await self._ensure_external_capacity(mint, pos.get('symbol', mint[:8]))
                if can_migrate:
                    migrated = await self.pancake_mgr.migrate_from_internal(
                        mint, pos['symbol'], int(pos['amt']), restore_cost,
                        ai_score=pos.get('ai_score', 0) or 0,
                        decimals=pos.get('decimals', 18) or 18)
                    if migrated:
                        self.graduated_positions.add(mint)
                        self.launch_positions.add(mint)
                        await self._handoff_to_external(mint)
                        restored += 1
                        continue

            self.active_snipes.add(mint)
            self._snipe_timestamps[mint] = pos.get('ts', time.time())
            sell_stage = pos.get('sell_stage', 0) or 0
            peak_pct = pos.get('peak_curve_pct', 0) or 0
            if peak_pct > 0:
                graduate_thr = self.monitor.get_graduate_bnb(mint, self.cfg.risk_cfg.get("GRADUATE_THRESHOLD_BNB", 24.0))
                self.monitor.set_peak_pool_bnb(mint, peak_pct / 100.0 * graduate_thr)
            if sell_stage >= 2:
                self.launch_positions.add(mint)
            self._spawn_task(self._sell_machine_safe(
                mint, pos['symbol'], int(pos['amt']),
                restore_cost, float(pos['init_pool_bnb']), self.cfg.risk_cfg,
                restore_stage=sell_stage, restore_peak_pct=peak_pct,
                entry_ts=pos.get('ts', time.time())))
            restored += 1

        ext_restored = await self.pancake_mgr.restore_from_db()

        # v16: 内外盘去重 — 已在外盘追踪中的仓位从内盘状态库剔除
        handoff_cleaned = 0
        active_after = await self.state_mgr.get_active()
        ext_mints_lower = {m.lower() for m in self.pancake_mgr.positions.keys()}
        for mint in list(active_after.keys()):
            if mint.lower() in ext_mints_lower:
                await self._handoff_to_external(mint)
                handoff_cleaned += 1

        mode = "🧪 模拟盘" if self.dry_run else "⚔️ 实盘"
        bnb_price = self._get_bnb_usd()
        await self._notify(
            f"🟢 <b>BSC v{BOT_VERSION} 已上线</b>\n"
            f"模式: {mode}\n"
            f"钱包: <code>{self.exec.address}</code>\n"
            f"余额: {real_bal:.4f} BNB (~${real_bal * bnb_price:,.0f})\n"
            f"RPC: T0:{len(self.fleet.t0_urls)} T1:{len(self.fleet.t1_urls)} T2:{len(self.fleet.t2_urls)}\n"
            f"恢复: 内盘 {restored} | 外盘 {ext_restored}"
            f"{f' | 去重移交 {handoff_cleaned}' if handoff_cleaned else ''}\n"
            f"流水线: Dev审查 → 并行监控(动量+买家) → AI → 买入")

    # ================= Dev画像分流: A直买 / B动量 / C踹掉 =================

    async def _on_new(self, token, creator, _):
        if self.paused:
            return
        if len(self.active_snipes) > 30 or token in self.active_snipes:
            return

        display_sym = f"{token[:10]}"
        try:
            _, resolved_sym = await asyncio.wait_for(self.exec.get_token_name_symbol(token), timeout=2.5)
            if resolved_sym:
                display_sym = resolved_sym
        except Exception:
            pass

        real_count = await self._count_real_positions()
        if real_count >= self.cfg.risk_cfg["MAX_CONCURRENT_POSITIONS"]:
            self.batch_notifier.add(
                "capacity_block",
                token=token,
                symbol=display_sym,
                stage="发现后预检",
                reason="达到最大同时持仓数，跳过新目标",
                extra=f"当前: {real_count}/{self.cfg.risk_cfg['MAX_CONCURRENT_POSITIONS']}"
            )
            return
        can_buy, reason = self.risk.can_buy(token)
        if not can_buy:
            self.batch_notifier.add(
                "risk_block",
                token=token,
                symbol=display_sym,
                stage="总闸风控",
                reason=reason,
                extra=f"当前真实仓位: {real_count}/{self.cfg.risk_cfg['MAX_CONCURRENT_POSITIONS']}"
            )
            return

        dev_assessment = None
        if self.dev_screener:
            dev_assessment = await self.dev_screener.assess(creator)
            grade = dev_assessment["grade"]
            reason = dev_assessment["reason"]
            detail = dev_assessment.get("details", {})
            extra = (
                f"creator: {creator if creator else 'N/A'} | "
                f"nonce={detail.get('nonce', 'N/A')} | "
                f"bal={detail.get('balance_bnb', 'N/A')} | "
                f"1h/6h/12h={detail.get('deploy_1h', 'N/A')}/{detail.get('deploy_6h', 'N/A')}/{detail.get('deploy_12h', 'N/A')}"
            )
            if grade == "C":
                self.batch_notifier.add(
                    "dev_block",
                    token=token,
                    symbol=display_sym,
                    stage="Dev画像 C级",
                    reason=reason,
                )
                return
            if grade == "A":
                name, sym = await self.exec.get_token_name_symbol(token)
                pool_bnb, pool_tok = await self.monitor.get_pool_state(token)
                if pool_bnb <= 0 or pool_tok <= 0:
                    self.batch_notifier.add(
                        "risk_block",
                        token=token,
                        symbol=sym or display_sym,
                        stage="A级直买降级",
                        reason="池子状态未取到，降级进入动量",
                        extra=extra
                    )
                else:
                    self.active_snipes.add(token)
                    self._snipe_timestamps[token] = time.time()
                    await self._notify(
                        f"🅰️ <b>Dev画像 A级直买</b> | {sym or display_sym}\n"
                        f"合约: {_four_token_link(token)}\n"
                        f"理由: {reason}\n"
                        f"画像: {extra}"
                    )
                    try:
                        await self.execute_snipe(
                            token, name, sym or display_sym, pool_bnb, pool_tok, self.cfg.risk_cfg,
                            self.cfg.risk_cfg.get('BUY_BNB_AMT', 0.05),
                            99.0, 'DEV_A', reason, creator
                        )
                    finally:
                        # 兜底释放（execute_snipe 内部已释放则这里为幂等）
                        await self._release_buy_slot(token)
                    return

        # B级 / 无法评级 -> 进入动量
        self.active_snipes.add(token)
        self._snipe_timestamps[token] = time.time()
        self._spawn_task(self._track(token, creator))

    # ================= v14: 追踪 — 并行观察期 =================

    async def _track(self, token, creator_addr=None):
        """
        v14 并行流水线:
        1. Monitor: 追踪动量 + 自动收集买家数据 (60s)
        2. Monitor 确认后 → Profiler + AI 并行执行
        3. 全部通过 → 买入
        """
        r = self.cfg.risk_cfg
        snipe_triggered = False
        try:
            name, sym = await self.exec.get_token_name_symbol(token)
            try:
                await asyncio.wait_for(self.monitor_sem.acquire(), timeout=MONITOR_SEM_TIMEOUT)
            except asyncio.TimeoutError:
                return

            confirmed_data = None

            async def _on_confirmed(addr, pool_bnb, pool_tok, buyer_addrs=None, buyer_blocks=None):
                nonlocal confirmed_data
                confirmed_data = (addr, pool_bnb, pool_tok, buyer_addrs or set(), buyer_blocks or {})

            monitor_result = None
            try:
                monitor_result = await self.monitor.monitor_token(
                    token, _on_confirmed,
                    r["MONITOR_DURATION_SEC"], r["UP_TICKS_REQUIRED"],
                    r["MOMENTUM_BNB_THRESHOLD"], r["MIN_UNIQUE_BUYERS"],
                    r.get("MAX_DEV_HOLDING_PCT", 12.0), r.get("GRADUATE_THRESHOLD_BNB", 24.0),
                    creator_addr=creator_addr,
                    confirm_delay_sec=r.get("MOMENTUM_CONFIRM_DELAY_SEC", 5),
                    confirm_pullback_bnb=r.get("MOMENTUM_CONFIRM_PULLBACK_BNB", 0.05),
                    sell_ratio_threshold=r.get("SELL_PRESSURE_RATIO_PCT", 40.0),
                    dev_sold_pct_threshold=r.get("SELL_PRESSURE_DEV_OUTFLOW_PCT", 5.0),
                    large_sell_count_threshold=r.get("SELL_PRESSURE_LARGE_SELL_COUNT", 2),
                    sell_pressure_min_volume=int(r.get("SELL_PRESSURE_MIN_VOLUME_TOKENS", 0) or 0),
                )
            finally:
                self.monitor_sem.release()

            if not confirmed_data:
                reason = (monitor_result or {}).get("reason") or f"{r['MONITOR_DURATION_SEC']}s 内未达到确认条件"
                self.batch_notifier.add(
                    "monitor_timeout",
                    token=token,
                    symbol=sym,
                    stage="综合观察",
                    reason=reason,
                )
                return

            addr, pool_bnb, pool_tok, buyer_addrs, buyer_blocks = confirmed_data

            # ★ v14: Profiler + AI 并行执行
            profiler_task = None
            if self.buyer_profiler and buyer_addrs:
                creation_block = max(0, min(buyer_blocks.values()) - 1) if buyer_blocks else 0
                profiler_task = asyncio.create_task(
                    self.buyer_profiler.profile(addr, buyer_addrs, buyer_blocks, creation_block))

            ai_task = asyncio.create_task(
                self._safe_ai_analyze(name, sym))

            # 等待 AI 结果
            buy_bnb, ai_score, ai_type, ai_reason = await ai_task

            # 等待 Profiler 结果
            if profiler_task:
                try:
                    profile_passed, profile_report = await asyncio.wait_for(profiler_task, timeout=10)
                except asyncio.TimeoutError:
                    profile_passed, profile_report = False, f"侧写等待超时 (10s)，当前单拦截"

                if not profile_passed:
                    self.batch_notifier.add(
                        "profiler_block",
                        token=addr,
                        symbol=sym,
                        stage="买家侧写",
                        reason=profile_report.splitlines()[0],
                        extra=profile_report[:220]
                    )
                    if "超时" not in profile_report:
                        self.risk.add_to_blacklist(addr, "Cabal控盘")
                        if creator_addr:
                            self.risk.add_dev_to_blacklist(creator_addr, "Cabal关联")
                    return

            # AI 拦截检查
            if ai_score < self.cfg.risk_cfg.get("AI_SCORE_THRESHOLD", 55):
                self.batch_notifier.add(
                    "ai_block",
                    token=addr,
                    symbol=sym,
                    stage="AI打分",
                    reason=f"{ai_score:.0f} 分 < 阈值 {self.cfg.risk_cfg.get('AI_SCORE_THRESHOLD', 55):.0f}",
                    extra=f"类型: {ai_type} | 理由: {ai_reason[:140]}"
                )
                return

            # 最终检查
            real_count = await self._count_real_positions()
            if real_count >= self.cfg.risk_cfg["MAX_CONCURRENT_POSITIONS"]:
                self.batch_notifier.add(
                    "capacity_block",
                    token=addr,
                    symbol=sym,
                    stage="最终开仓检查",
                    reason="达到最大同时持仓数，拒绝新开仓",
                    extra=f"当前: {real_count}/{self.cfg.risk_cfg['MAX_CONCURRENT_POSITIONS']}"
                )
                return

            recheck_bnb, recheck_tok = await self.monitor.get_pool_state(addr)
            if recheck_bnb >= 0 and recheck_bnb < pool_bnb * 0.85:
                self.batch_notifier.add(
                    "recheck_block",
                    token=addr,
                    symbol=sym,
                    stage="买前二次确认",
                    reason="确认后池子回撤过大，放弃开仓",
                    extra=f"初始: {pool_bnb:.4f} BNB → 复检: {recheck_bnb:.4f} BNB"
                )
                return
            if recheck_bnb >= 0:
                pool_bnb, pool_tok = recheck_bnb, recheck_tok

            snipe_triggered = True
            await self.execute_snipe(addr, name, sym, pool_bnb, pool_tok, r,
                                     buy_bnb, ai_score, ai_type, ai_reason, creator_addr)

        except Exception as e:
            logger.error(f"❌ [Orch] 追踪异常: {token[:10]}... | {e}")
            self.batch_notifier.add(
                "risk_block",
                token=token,
                symbol=sym if 'sym' in locals() else '未解析',
                stage="追踪异常",
                reason=str(e)[:160],
                extra="该目标已中止，需人工查看日志"
            )
        finally:
            if not snipe_triggered:
                self.active_snipes.discard(token)
                self._snipe_timestamps.pop(token, None)
            # 兜底释放（若 execute_snipe 中途异常，避免占位泄漏）
            await self._release_buy_slot(token)

    async def _safe_ai_analyze(self, name, sym):
        """AI分析带超时保护"""
        try:
            return await asyncio.wait_for(
                self.ai.analyze_token(name, sym, session=self.http_session), timeout=15.0)
        except Exception:
            return self.ai.base_buy, 60.0, "AI_ERROR", "AI异常"

    # ================= 开仓执行 =================

    async def execute_snipe(self, token, name, sym, pool_bnb, pool_tok, r,
                            buy_bnb, ai_score, ai_type, ai_reason, creator_addr=None):
        slot_reserved = False
        ok, reserve_reason = await self._reserve_buy_slot(token)
        if not ok:
            self.batch_notifier.add(
                "risk_block",
                token=token,
                symbol=sym,
                stage="原子买入闸",
                reason=reserve_reason,
            )
            self.active_snipes.discard(token)
            self._snipe_timestamps.pop(token, None)
            return
        slot_reserved = True

        graduate_thr = self.monitor.get_graduate_bnb(token, r.get("GRADUATE_THRESHOLD_BNB", 24.0))
        curve_pct = min(100.0, (pool_bnb / graduate_thr) * 100.0) if graduate_thr > 0 else 0
        pool_bnb_wei = self.exec.w3.to_wei(pool_bnb, 'ether')
        bnb_in_wei = self.exec.w3.to_wei(buy_bnb, 'ether')
        expected_tokens = self.exec.calc_expected_tokens(bnb_in_wei, pool_bnb_wei, pool_tok)
        if expected_tokens <= 0:
            expected_tokens = int(buy_bnb * 1e18 / max(pool_bnb, 0.001) * 0.8)

        token_decimals = await self.exec.get_token_decimals(token)
        total_gas_cost = 0.0

        # ★ v14: 市值计算用实际数据
        total_supply = self.monitor.get_initial_supply(token)
        mc_text = await self._fmt_market_cap(pool_bnb, graduate_thr,
                                              pool_tokens=pool_tok, total_supply=total_supply,
                                              token_addr=token)

        if self.dry_run:
            if self.virtual_bnb < buy_bnb:
                self.batch_notifier.add(
                    "balance_block",
                    token=token,
                    symbol=sym,
                    stage="模拟盘买入",
                    reason="虚拟余额不足",
                    extra=f"需要: {buy_bnb:.4f} BNB | 当前: {self.virtual_bnb:.4f} BNB"
                )
                self.active_snipes.discard(token)
                self._snipe_timestamps.pop(token, None)
                if slot_reserved:
                    await self._release_buy_slot(token)
                    slot_reserved = False
                return
            token_bal = expected_tokens
            self.virtual_bnb -= buy_bnb
            tx_sig = f"dry_run_{int(time.time())}"
            effective_cost = buy_bnb
        else:
            try:
                tx_sig = await asyncio.wait_for(
                    self.exec.buy(token, buy_bnb, pool_bnb_wei, pool_tok), timeout=BUY_OPERATION_TIMEOUT)
            except (asyncio.TimeoutError, Exception) as e:
                logger.error(f"❌ 买入失败: {sym} | {e}")
                tx_sig = None

            if not tx_sig:
                await self._notify(
                    f"❌ <b>买入失败</b> | {sym}\n"
                    f"合约: {_addr_link(token)}\n投入: {buy_bnb:.4f} BNB | AI: {ai_score:.0f}分")
                self.active_snipes.discard(token)
                self._snipe_timestamps.pop(token, None)
                if slot_reserved:
                    await self._release_buy_slot(token)
                    slot_reserved = False
                return

            total_gas_cost += self.exec.last_gas_cost_bnb
            await asyncio.sleep(BUY_TAX_SYNC_DELAY)
            token_bal = await self._query_token_balance_with_retry(token, expected_tokens)

            effective_cost = buy_bnb
            if token_bal > 0 and pool_tok > 0 and pool_bnb_wei > 0:
                post_buy_bnb, post_buy_tok = await self.monitor.get_pool_state(token)
                if post_buy_bnb > 0 and post_buy_tok > 0:
                    actual_value = self.exec.estimate_token_value_bnb(
                        token_bal, self.exec.w3.to_wei(post_buy_bnb, 'ether'), post_buy_tok)
                else:
                    actual_value = self.exec.estimate_token_value_bnb(token_bal, pool_bnb_wei, pool_tok)
                if 0 < actual_value < buy_bnb:
                    effective_cost = actual_value

            if not self.dry_run:
                approve_result = await self.exec.approve_if_needed(token)
                if approve_result and self.exec.last_gas_cost_bnb > 0:
                    total_gas_cost += self.exec.last_gas_cost_bnb
                    await self.state_mgr.add_gas_cost(token, self.exec.last_gas_cost_bnb)

        add_pos_ok = False
        try:
            await self.state_mgr.add_pos(
                token, sym, token_bal, tx_sig, buy_bnb,
                f"AI:{ai_type} Score:{ai_score:.0f}", pool_bnb, ai_score,
                gas_cost=total_gas_cost, decimals=token_decimals, effective_cost=effective_cost)
            add_pos_ok = True
        except Exception as e:
            logger.error(f"❌ add_pos 失败: {e}")

        if add_pos_ok:
            self.risk.record_buy(token)
        else:
            logger.error(f"❌ 跳过 record_buy: {sym} 持仓未成功写入状态库")
        bnb_usd = self._get_bnb_usd()
        token_display = BSCExecutor.format_token_amount(token_bal, token_decimals)

        tax_note = ""
        if effective_cost < buy_bnb * 0.98:
            tax_note = f"\n⚠️ 买税: ~{(1 - effective_cost / buy_bnb) * 100:.1f}%"

        real_count = await self._count_real_positions()
        current_bal = await self.exec.get_bnb_balance() if not self.dry_run else self.virtual_bnb

        # ★ v16: 买入通知发送前再拉一次实时池子，避免快币几秒漂移导致展示错位
        disp_pool_bnb = pool_bnb
        disp_pool_tok = pool_tok
        disp_curve_pct = curve_pct
        disp_mc_text = mc_text
        disp_graduate_thr = graduate_thr

        if self.monitor:
            try:
                latest_bnb, latest_tok = await self.monitor.get_pool_state(token, ttl=0.0)
                if latest_bnb > 0:
                    disp_pool_bnb = latest_bnb
                    if latest_tok > 0:
                        disp_pool_tok = latest_tok
                    disp_graduate_thr = self.monitor.get_graduate_bnb(token, graduate_thr)
                    if disp_graduate_thr > 0:
                        disp_curve_pct = min(100.0, (disp_pool_bnb / disp_graduate_thr) * 100.0)
                    total_supply_latest = self.monitor.get_initial_supply(token)
                    disp_mc_text = await self._fmt_market_cap(
                        disp_pool_bnb, disp_graduate_thr,
                        pool_tokens=disp_pool_tok if disp_pool_tok and disp_pool_tok > 0 else 0,
                        total_supply=total_supply_latest,
                        token_addr=token
                    )
            except Exception:
                pass

        # ★ v15: 丰富买入通知
        daily = self.risk.get_daily_stats()
        mode_tag = "🧪" if self.dry_run else "⚔️"

        pool_line = f"{disp_pool_bnb:.4f} BNB"
        if self.monitor:
            meta = self.monitor.get_curve_meta(token)
            if meta and not bool(meta.get("quote_is_native", True)):
                quote_addr = str(meta.get("quote_addr", "") or "").lower()
                if quote_addr in STABLE_QUOTE_ADDRS:
                    pool_line = f"{disp_pool_bnb:.2f} USD (~{disp_pool_bnb / max(bnb_usd, 1e-9):.4f} BNB)"
                else:
                    quote_bnb = await self._get_quote_bnb_price(quote_addr) if quote_addr else 0.0
                    if quote_bnb > 0:
                        pool_line = f"{disp_pool_bnb:.2f} Quote (~{disp_pool_bnb * quote_bnb:.4f} BNB)"
                    else:
                        pool_line = f"{disp_pool_bnb:.2f} Quote"

        await self._notify(
            f"🎯 <b>买入成功</b> {mode_tag} | {sym}\n"
            f"合约: {_four_token_link(token)}\n"
            f"数量: <b>{token_display}</b>\n"
            f"成本: <b>{buy_bnb:.4f} BNB</b> (~${buy_bnb * bnb_usd:.2f}){tax_note}\n"
            f"AI: <b>{ai_score:.0f}分</b> {ai_type} | {ai_reason}\n"
            f"池子: {pool_line} | 曲线: <b>{disp_curve_pct:.1f}%</b> | MC: {disp_mc_text}\n"
            f"TX: {_tx_link(tx_sig)}\n"
            f"━━━ 实时状态 ━━━\n"
            f"💰 余额: {current_bal:.4f} BNB (~${current_bal * bnb_usd:,.0f})\n"
            f"📋 持仓: {real_count}/{r['MAX_CONCURRENT_POSITIONS']}\n"
            f"📊 今日: {daily['trades']}笔 {daily['wins']}W/{daily['losses']}L 盈亏{daily['pnl_bnb']:+.4f}")

        await self._check_low_balance_warning()

        self._spawn_task(self._sell_machine_safe(
            token, sym, token_bal, effective_cost, pool_bnb, r, creator_addr=creator_addr))

        if slot_reserved:
            await self._release_buy_slot(token)
            slot_reserved = False

    # ================= 卖出状态机 (保持原逻辑，修复 finally) =================

    async def _sell_machine_safe(self, token, sym, *args, **kwargs):
        try:
            await self._sell_machine(token, sym, *args, **kwargs)
        except Exception as e:
            logger.error(f"❌ [SellMachine] {sym} 崩溃: {e}", exc_info=True)
            try:
                await self._finalize_position(token, sym, is_orphan=True, trigger_reason="状态机崩溃")
            except Exception:
                pass

    async def _sell_machine(self, token, sym, init_tok, cost_bnb, init_bnb, r,
                            restore_stage=0, creator_addr=None, restore_peak_pct=0.0,
                            entry_ts=None):
        if token not in self.sell_locks:
            self.sell_locks[token] = CancellationSafeLock()

        cached_amt = init_tok
        peak_mult = 1.0
        stage1_sold = restore_stage >= 1
        stage2_sold = restore_stage >= 2
        dead_pool_since = 0
        dead_pool_consecutive = 0
        launch_warned = False
        graduated_notified = False
        entry_time = float(entry_ts) if entry_ts else time.time()
        ever_pumped = False
        peak_curve_pct = restore_peak_pct
        zero_balance_streak = 0
        check_count = 0
        consecutive_loop_errors = 0

        def _get_params():
            cr = self.cfg.risk_cfg
            return {
                "hard_sl": cr.get("HARD_SL_MULT", 0.7),
                "trailing_sl": cr.get("TRAILING_SL_PCT", 0.8),
                "graduate_thr": cr.get("GRADUATE_THRESHOLD_BNB", 24.0),
                "double_mult": cr.get("DOUBLE_SELL_MULT", 2.0),
                "double_pct": cr.get("DOUBLE_SELL_PCT", 0.5),
                "sideways_sec": cr.get("SIDEWAYS_EXIT_SEC", 3600),
                "sideways_min": cr.get("SIDEWAYS_MIN_MULT", 0.95),
                "sideways_max": cr.get("SIDEWAYS_MAX_MULT", 1.20),
            }

        async with self.sell_locks[token]:
            while True:
                if self.emergency_mode:
                    return

                await asyncio.sleep(DEAD_POOL_SLOW_INTERVAL if dead_pool_consecutive >= 3 else 2)

                if self.emergency_mode:
                    return

                abs_age = time.time() - entry_time
                if abs_age >= SELL_MACHINE_ABSOLUTE_TTL_SEC:
                    ttl_days = SELL_MACHINE_ABSOLUTE_TTL_SEC / 86400.0
                    reason = f"绝对TTL到达({ttl_days:.0f}天)"
                    try:
                        await self.state_mgr.mark_abnormal(token, reason)
                    except Exception as e:
                        logger.warning(f"⚠️ [SM] {sym} 标记异常失败: {e}")

                    self.active_snipes.discard(token)
                    self._snipe_timestamps.pop(token, None)

                    await self._notify(
                        f"🧊 <b>卖出协程TTL到达，已停止自动管理</b> | {sym}\n"
                        f"合约: {_four_token_link(token)}\n"
                        f"已运行: <b>{int(abs_age/3600)}h</b>\n"
                        f"处理: 已标记为异常仓位（status=abnormal），请人工决策"
                    )
                    return

                try:
                    check_count += 1
                    p = _get_params()
                    graduate_thr = self.monitor.get_graduate_bnb(token, p["graduate_thr"])
                    cur_bnb, cur_tokens = await self.monitor.get_pool_state(token)

                    if cur_bnb < 0:
                        continue

                    consecutive_loop_errors = 0
                    curve_pct = min(100.0, (cur_bnb / graduate_thr) * 100.0) if graduate_thr > 0 else 0.0

                    curve_status = 0
                    if self.monitor:
                        meta = self.monitor.get_curve_meta(token)
                        if meta:
                            curve_status = int(meta.get("status", 0) or 0)

                    # v16: 只要链上已进入 COMPLETED(3)，立即迁移外盘（不再依赖“死池=0.01”）
                    if curve_status == 3 and not graduated_notified:
                        can_migrate = await self._ensure_external_capacity(token, sym)
                        if not can_migrate:
                            continue

                        pos_data = await self.state_mgr._run(
                            lambda: self.state_mgr._row("SELECT * FROM positions WHERE mint=?", (token,)))
                        if pos_data and not isinstance(pos_data, dict):
                            pos_data = dict(pos_data)

                        token_amount = cached_amt
                        token_decimals = pos_data.get('decimals', 18) if pos_data else 18

                        migrated = await self.pancake_mgr.migrate_from_internal(
                            token, sym, token_amount, cost_bnb,
                            ai_score=pos_data.get('ai_score', 0) if pos_data else 0,
                            decimals=token_decimals)
                        if migrated:
                            graduated_notified = True
                            self.graduated_positions.add(token)
                            self.launch_positions.add(token)
                            await self._notify(
                                f"🎓 <b>毕业！迁移外盘</b> | {sym}\n"
                                f"合约: {_four_token_link(token)}\n"
                                f"曲线: <b>{curve_pct:.1f}%</b> (status=COMPLETED)\n"
                                f"💡 /ext 查看外盘状态")
                            await self._handoff_to_external(token)
                            return

                    if curve_pct > peak_curve_pct:
                        peak_curve_pct = curve_pct
                        if int(peak_curve_pct) % 10 == 0 or peak_curve_pct >= GRADUATION_PEAK_PCT_THRESHOLD:
                            try:
                                await self.state_mgr.update_peak_curve_pct(token, peak_curve_pct)
                            except Exception:
                                pass

                    # 死池 / 毕业检测
                    if cur_bnb <= 0.01:
                        now = time.time()
                        dead_pool_consecutive += 1
                        if dead_pool_since == 0:
                            dead_pool_since = now

                        dead_duration = now - dead_pool_since
                        monitor_peak = self.monitor.get_peak_pool_bnb(token)
                        monitor_peak_pct = min(100.0, (monitor_peak / graduate_thr) * 100.0) if graduate_thr > 0 else 0
                        effective_peak_pct = max(peak_curve_pct, monitor_peak_pct)
                        is_likely_graduated = (effective_peak_pct >= GRADUATION_PEAK_PCT_THRESHOLD)

                        if is_likely_graduated:
                            if dead_duration >= GRADUATION_CONFIRM_SEC and not graduated_notified:
                                can_migrate = await self._ensure_external_capacity(token, sym)
                                if not can_migrate:
                                    continue

                                pos_data = await self.state_mgr._run(
                                    lambda: self.state_mgr._row("SELECT * FROM positions WHERE mint=?", (token,)))
                                if pos_data and not isinstance(pos_data, dict):
                                    pos_data = dict(pos_data)
                                token_amount = cached_amt
                                token_decimals = pos_data.get('decimals', 18) if pos_data else 18

                                migrated = await self.pancake_mgr.migrate_from_internal(
                                    token, sym, token_amount, cost_bnb,
                                    ai_score=pos_data.get('ai_score', 0) if pos_data else 0,
                                    decimals=token_decimals)
                                if migrated:
                                    graduated_notified = True
                                    self.graduated_positions.add(token)
                                    self.launch_positions.add(token)
                                    await self._notify(
                                        f"🎓 <b>毕业！迁移外盘</b> | {sym}\n"
                                        f"合约: {_four_token_link(token)}\n"
                                        f"曲线峰值: <b>{effective_peak_pct:.1f}%</b>\n"
                                        f"💡 /ext 查看外盘状态")
                                    await self._handoff_to_external(token)
                                    return
                            continue

                        if dead_duration >= DEAD_POOL_TIMEOUT_SEC:
                            await self._do_sell_real(token, sym, cached_amt, cost_bnb)
                            await self._finalize_position(token, sym, is_orphan=True, trigger_reason="死池超时")
                            return
                        continue
                    else:
                        dead_pool_since = 0
                        dead_pool_consecutive = 0

                    mult = self.exec.calc_price_mult(float(init_bnb), cur_bnb)
                    if mult > peak_mult:
                        peak_mult = mult
                    if mult > 1.1:
                        ever_pumped = True

                    # 只在 TRADING(0) 阶段提示“即将发射”，避免已发射/已毕业时误报
                    if not launch_warned and curve_pct >= LAUNCH_WARNING_PCT and curve_status == 0:
                        launch_warned = True
                        await self._notify(
                            f"🚀🚀🚀 <b>即将发射!</b> | {sym}\n"
                            f"曲线: <b>{curve_pct:.1f}%</b> | 倍数: {mult:.2f}x")

                    # 博发射
                    if stage1_sold and stage2_sold:
                        if mult <= p["hard_sl"]:
                            sell_result = await self._do_sell_real(token, sym, cached_amt, cost_bnb)
                            await self._finalize_position(token, sym,
                                trigger_reason=f"硬止损 ({mult:.2f}x)", sell_mult=mult,
                                sell_tx=sell_result[1] if sell_result else None)
                            return
                        continue

                    # 第1级: 翻倍出本
                    if mult >= p["double_mult"] and not stage1_sold:
                        sell_amt = int(init_tok * p["double_pct"])
                        cost_slice = cost_bnb * p["double_pct"]
                        sell_result = await self._do_sell_real(token, sym, sell_amt, cost_slice)
                        if sell_result is not None:
                            realized, sell_tx_hash = sell_result
                            stage1_sold = True
                            cached_amt -= sell_amt
                            if cached_amt < 0:
                                cached_amt = 0
                            peak_mult = mult
                            await self.state_mgr.update_sell_stage(token, 1)
                            bnb_usd = self._get_bnb_usd()
                            await self._notify(
                                f"💰 <b>翻倍出本</b> | {sym}\n"
                                f"倍数: <b>{mult:.1f}x</b> | 本次回收: <b>{realized:.4f} BNB</b> (~${realized * bnb_usd:.2f})\n"
                                f"TX: {_tx_link(sell_tx_hash)}")

                    # 第2级: 出利
                    stage2_trigger = p["double_mult"] * 2
                    if stage1_sold and not stage2_sold and mult >= stage2_trigger:
                        sell_amt = int(cached_amt * 0.5)
                        cost_slice = cost_bnb * (1 - p["double_pct"]) * 0.5
                        sell_result = await self._do_sell_real(token, sym, sell_amt, cost_slice)
                        if sell_result is not None:
                            realized, sell_tx_hash = sell_result
                            stage2_sold = True
                            cached_amt -= sell_amt
                            if cached_amt < 0:
                                cached_amt = 0
                            peak_mult = mult
                            await self.state_mgr.update_sell_stage(token, 2)
                            self.launch_positions.add(token)
                            net_profit = realized - cost_slice
                            await self._notify(
                                f"🏆 <b>出利成功</b> | {sym}\n"
                                f"倍数: <b>{mult:.1f}x</b> | 净利: <b>{net_profit:+.4f} BNB</b>\n"
                                f"TX: {_tx_link(sell_tx_hash)}")

                    # 移动止盈
                    if peak_mult > 1.2:
                        trailing_trigger = peak_mult * p["trailing_sl"]
                        if mult <= trailing_trigger:
                            sell_result = await self._do_sell_real(token, sym, cached_amt, cost_bnb)
                            await self._finalize_position(token, sym,
                                trigger_reason=f"移动止盈 (峰{peak_mult:.2f}x→{mult:.2f}x)",
                                sell_mult=mult, sell_tx=sell_result[1] if sell_result else None)
                            return

                    # 时间衰减止损
                    if not stage1_sold and not ever_pumped:
                        age_sec = time.time() - entry_time
                        if age_sec >= TIME_DECAY_SL_SEC and mult <= TIME_DECAY_SL_MULT:
                            sell_result = await self._do_sell_real(token, sym, cached_amt, cost_bnb)
                            await self._finalize_position(token, sym,
                                trigger_reason=f"时间衰减 ({int(age_sec)}s, {mult:.2f}x)",
                                sell_mult=mult, sell_tx=sell_result[1] if sell_result else None)
                            return

                    # 横盘超时退出（不涨不跌长期占仓）
                    if not stage1_sold:
                        age_sec = time.time() - entry_time
                        sideways_sec = float(p.get("sideways_sec", 0) or 0)
                        if sideways_sec > 0 and age_sec >= sideways_sec:
                            smin = float(p.get("sideways_min", 0.95) or 0.95)
                            smax = float(p.get("sideways_max", 1.20) or 1.20)
                            low, high = (smin, smax) if smin <= smax else (smax, smin)
                            if low <= mult <= high:
                                sell_result = await self._do_sell_real(token, sym, cached_amt, cost_bnb)
                                await self._finalize_position(token, sym,
                                    trigger_reason=f"横盘超时 ({int(age_sec/60)}min, {mult:.2f}x)",
                                    sell_mult=mult, sell_tx=sell_result[1] if sell_result else None)
                                return

                    # 硬止损
                    if mult <= p["hard_sl"]:
                        sell_result = await self._do_sell_real(token, sym, cached_amt, cost_bnb)
                        await self._finalize_position(token, sym,
                            trigger_reason=f"硬止损 ({mult:.2f}x)",
                            sell_mult=mult, sell_tx=sell_result[1] if sell_result else None)
                        return

                    # 余额同步
                    if not self.dry_run and check_count % 5 == 0:
                        try:
                            real = await asyncio.wait_for(
                                self.exec.get_token_balance(token), timeout=BALANCE_CHECK_TIMEOUT)
                        except (asyncio.TimeoutError, Exception):
                            real = -1
                        if real > 0:
                            cached_amt = real
                            zero_balance_streak = 0
                            await self.state_mgr.update_amt(token, real)
                        elif real == 0 and cached_amt > 0:
                            zero_balance_streak += 1
                            if zero_balance_streak >= 2:
                                await asyncio.sleep(BALANCE_RECHECK_DELAY)
                                try:
                                    real2 = await asyncio.wait_for(
                                        self.exec.get_token_balance(token, use_paid=True), timeout=BALANCE_CHECK_TIMEOUT)
                                except (asyncio.TimeoutError, Exception):
                                    real2 = -1
                                if real2 == 0:
                                    cached_amt = 0

                    if cached_amt <= 0:
                        await self._finalize_position(token, sym, trigger_reason="代币余额归零")
                        return

                except asyncio.CancelledError:
                    raise
                except Exception as loop_err:
                    consecutive_loop_errors += 1
                    logger.warning(f"⚠️ [SM] {sym} 异常 ({consecutive_loop_errors}): {loop_err}")
                    if consecutive_loop_errors >= SELL_MACHINE_MAX_CONSECUTIVE_ERRORS:
                        raise
                    await asyncio.sleep(SELL_MACHINE_ERROR_SLEEP)

    # ================= 卖出 =================

    async def _do_sell_real(self, token, sym, amt, cost_slice):
        if amt <= 0:
            return (0.0, "")
        pool_bnb, pool_tok = await self.monitor.get_pool_state(token)
        if pool_bnb < 0:
            return None
        pool_bnb_wei = self.exec.w3.to_wei(max(pool_bnb, 0.001), 'ether')
        pool_tok_safe = max(pool_tok, 1)
        expected_bnb_wei = self.exec.calc_expected_bnb(amt, pool_bnb_wei, pool_tok_safe)
        expected_bnb = float(self.exec.w3.from_wei(expected_bnb_wei, 'ether')) if expected_bnb_wei > 0 else 0.0

        if self.dry_run:
            import random
            # ★ v15: 模拟盘卖出 — 模拟价格冲击和滑点
            sim_slippage = random.uniform(0.02, 0.08)  # 2-8% 滑点
            sim_realized = expected_bnb * (1.0 - sim_slippage)
            sim_gas = random.uniform(0.0003, 0.0008)
            sim_realized = max(0, sim_realized - sim_gas)
            self.virtual_bnb += sim_realized
            await self.state_mgr.realize(token, amt, sim_realized, cost_slice)
            logger.info(
                f"🧪 [模拟盘] 卖出 {sym} | 预期 {expected_bnb:.4f} → "
                f"实得 {sim_realized:.4f} BNB (滑点{sim_slippage*100:.1f}%)"
            )
            return (sim_realized, f"dry_sell_{int(time.time())}")

        pre_sell_balance = await self._query_token_balance_with_retry(token)
        if pre_sell_balance <= 0:
            return (0.0, "")

        try:
            tx = await asyncio.wait_for(
                self.exec.sell(token, amt, pool_bnb_wei, pool_tok_safe), timeout=SELL_OPERATION_TIMEOUT)
        except (asyncio.TimeoutError, Exception) as e:
            logger.error(f"❌ 内盘卖出失败: {sym} | {e}")
            tx = None

        if not tx:
            return None

        sell_gas = self.exec.last_gas_cost_bnb
        if sell_gas > 0:
            await self.state_mgr.add_gas_cost(token, sell_gas)

        await asyncio.sleep(1.5)
        try:
            remaining = await asyncio.wait_for(
                self.exec.get_token_balance(token), timeout=BALANCE_CHECK_TIMEOUT)
        except (asyncio.TimeoutError, Exception):
            remaining = -1

        if remaining == -1:
            await self.state_mgr.realize(token, amt, expected_bnb, cost_slice)
            return (expected_bnb, tx)

        if remaining == 0 or (pre_sell_balance > 0 and pre_sell_balance - remaining >= amt * 0.8):
            await self.state_mgr.realize(token, amt, expected_bnb, cost_slice)
            return (expected_bnb, tx)
        else:
            return None

    # ================= 清算 =================

    async def _finalize_position(self, token, sym="", is_orphan=False,
                                  trigger_reason="", sell_mult=0.0, sell_tx=None):
        pos = await self.state_mgr.finalize(token)
        self.active_snipes.discard(token)
        self._snipe_timestamps.pop(token, None)
        self.launch_positions.discard(token)
        self.graduated_positions.discard(token)
        self.sell_locks.pop(token, None)

        if pos:
            realized = pos.get('realized_bnb', 0)
            cost = pos.get('buy_bnb', 0)
            gas_cost = pos.get('gas_cost', 0) or 0
            pnl = realized - cost - gas_cost
            effective_cost = pos.get('effective_cost', 0) or cost

            risk_result = self.risk.record_trade(token, pnl, is_orphan=is_orphan)
            bnb_usd = self._get_bnb_usd()
            pnl_pct = (pnl / effective_cost * 100) if effective_cost > 0 else 0

            if not is_orphan:
                emoji = "📈" if pnl >= 0 else "📉"
                tx_text = f"\nTX: {_tx_link(sell_tx)}" if sell_tx else ""
                # ★ v15: 增加实时余额和日统计
                try:
                    cur_bal = await self.exec.get_bnb_balance() if not self.dry_run else self.virtual_bnb
                    daily = self.risk.get_daily_stats()
                    status_line = (
                        f"\n━━━ 实时 ━━━\n"
                        f"💰 {cur_bal:.4f} BNB | "
                        f"📊 今日 {daily['pnl_bnb']:+.4f} BNB ({daily['wins']}W/{daily['losses']}L)"
                    )
                except Exception:
                    status_line = ""
                await self._notify(
                    f"{emoji} <b>{'止盈' if pnl >= 0 else '止损'}</b> | {sym}\n"
                    f"原因: {trigger_reason}\n"
                    f"合约: {_addr_link(token)}\n"
                    f"成本: {cost:.4f} | 累计回收: <b>{realized:.4f} BNB</b>\n"
                    f"Gas: {gas_cost:.4f} BNB\n"
                    f"盈亏: <b>{pnl:+.4f} BNB</b> ({pnl_pct:+.1f}%){tx_text}{status_line}")

            if risk_result and risk_result.get("circuit_breaker_triggered"):
                await self._notify(
                    f"🚨 <b>风控熔断!</b>\n{risk_result['reason']}\n"
                    f"冷却 <b>{risk_result['cooldown_minutes']}</b> 分钟")

    # ================= 模拟盘切换 =================

    async def toggle_dry_run(self, enable: bool) -> str:
        """TG 切换模拟盘/实盘，同步到所有子模块和持久层。"""
        old = self.dry_run
        if old == enable:
            return f"{'🧪 模拟盘' if enable else '⚔️ 实盘'} 已经是当前模式"

        ok, reason = await self._assert_mode_switch_safe()
        if not ok:
            return reason

        old_paused = self.paused
        self.paused = True
        try:
            self.dry_run = enable
            self.exec.dry_run = enable
            os.environ["DRY_RUN"] = "true" if enable else "false"
            self.cfg.sys_cfg["DRY_RUN"] = enable
            self.cfg.save_sys()

            old_state_mgr = self.state_mgr
            old_buyer_profiler = self.buyer_profiler
            old_pancake_mgr = self.pancake_mgr

            self.state_mgr = SQLiteStateManager(db=self._state_db_path(enable))
            self.risk = self._new_risk_guard(enable)
            self.dev_screener = DevScreener(fleet=self.fleet, risk_guard=self.risk)
            self.buyer_profiler = BuyerProfiler(
                fleet=self.fleet,
                risk_guard=self.risk,
                db_path=self._buyer_history_db_path(enable),
                legacy_db_path=self._risk_db_path(enable),
            )
            pancake_db_path = self._ensure_pancake_db_layout(enable)
            self.pancake_mgr = PancakeManager(self.exec, self._notify, self.price_oracle, db_path=pancake_db_path)
            ext_restored = await self.pancake_mgr.restore_from_db()
            self.sync_config()

            with contextlib.suppress(Exception):
                if old_buyer_profiler:
                    old_buyer_profiler.shutdown()
            with contextlib.suppress(Exception):
                if old_pancake_mgr:
                    old_pancake_mgr.shutdown()
            with contextlib.suppress(Exception):
                if old_state_mgr:
                    old_state_mgr.shutdown()
        finally:
            self.paused = old_paused

        mode = "🧪 模拟盘" if enable else "⚔️ 实盘"
        bal = await self.exec.get_bnb_balance()
        bnb_usd = self._get_bnb_usd()

        if enable:
            self.virtual_bnb = 10.0
            msg = (f"🔄 <b>已切换到 {mode}</b>\n"
                   f"虚拟余额: 10.0 BNB\n"
                   f"实际余额: {bal:.4f} BNB (不会动)\n"
                   f"外盘恢复: {ext_restored}\n"
                   f"⚠️ 所有交易将模拟执行")
        else:
            msg = (f"🔄 <b>已切换到 {mode}</b>\n"
                   f"钱包余额: {bal:.4f} BNB (~${bal * bnb_usd:,.0f})\n"
                   f"外盘恢复: {ext_restored}\n"
                   f"⚠️ 交易将真实上链，请确认!")

        await self._notify(msg)
        return msg

    async def shutdown(self):
        logger.info("🛑 [Orch] 开始优雅关闭")
        self.paused = True

        tasks_to_cancel = []
        for attr in ("_wal_task", "_cleanup_task"):
            t = getattr(self, attr, None)
            if t and not t.done():
                t.cancel()
                tasks_to_cancel.append(t)
            setattr(self, attr, None)

        for t in list(self._bg_tasks):
            if not t.done():
                t.cancel()
                tasks_to_cancel.append(t)

        if tasks_to_cancel:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
        self._bg_tasks.clear()

        if self.batch_notifier:
            with contextlib.suppress(Exception):
                await self.batch_notifier.shutdown()
            self.batch_notifier = None

        if self.http_session and not self.http_session.closed:
            with contextlib.suppress(Exception):
                await self.http_session.close()
            self.http_session = None

        if self.price_oracle:
            with contextlib.suppress(Exception):
                await self.price_oracle.shutdown()
            self.price_oracle = None
        with contextlib.suppress(Exception):
            if self.pancake_mgr:
                self.pancake_mgr.shutdown()
        with contextlib.suppress(Exception):
            if self.buyer_profiler:
                self.buyer_profiler.shutdown()
        with contextlib.suppress(Exception):
            if self.fleet:
                self.fleet.shutdown()
        with contextlib.suppress(Exception):
            if self.state_mgr:
                self.state_mgr.shutdown()

        logger.info("🛑 [Orch] 优雅关闭完成")

    # ================= 紧急清仓 =================

    async def emergency_sell_all(self):
        active = await self.state_mgr.get_active()
        if not active:
            return await self._notify("📭 无持仓")

        self.paused = True
        self.emergency_mode = True
        await self._notify(f"🚨 <b>紧急清仓</b> | {len(active)} 个内盘持仓")

        for _ in range(10):
            if all(not lk.locked() for lk in self.sell_locks.values()):
                break
            await asyncio.sleep(0.5)

        active = await self.state_mgr.get_active()
        total_recovered = 0.0
        skipped_locked = []
        for mint, pos in active.items():
            lk = self.sell_locks.get(mint)
            if lk and lk.locked():
                skipped_locked.append((mint, pos.get('symbol', 'UNK')))
                continue

            amt = int(pos['amt'])
            if amt > 0:
                sell_result = await self._do_sell_real(mint, pos['symbol'], amt, pos.get('buy_bnb', 0))
                if sell_result:
                    total_recovered += sell_result[0]
            await self._finalize_position(mint, pos['symbol'], trigger_reason="紧急清仓")

        if skipped_locked:
            brief = " | ".join([f"{s}({m[:8]})" for m, s in skipped_locked[:5]])
            more = " ..." if len(skipped_locked) > 5 else ""
            await self._notify(
                f"⚠️ <b>紧急清仓跳过 {len(skipped_locked)} 个锁定仓位</b>\n"
                f"原因: 卖出协程仍在执行，避免重复卖出\n"
                f"样例: {brief}{more}"
            )

        self.emergency_mode = False
        current_bal = await self.exec.get_bnb_balance() if not self.dry_run else self.virtual_bnb
        await self._notify(
            f"🚨 <b>清仓完成</b>\n回收: <b>{total_recovered:.4f} BNB</b>\n"
            f"余额: {current_bal:.4f} BNB\n💡 /resume 恢复")

    # ================= TG 状态 =================

    async def get_full_status(self):
        active = await self.state_mgr.get_active()
        bal = await self.exec.get_bnb_balance()
        bnb_usd = self._get_bnb_usd()
        mode = "🧪 模拟" if self.dry_run else "⚔️ 实盘"
        paused = "⏸️" if self.paused else "▶️"
        real_count = await self._count_real_positions()
        ext_count = len(self.pancake_mgr.positions) if self.pancake_mgr else 0

        # v14: 漏斗统计
        pipeline_text = ""
        if self.dev_screener:
            ds = self.dev_screener
            bp = self.buyer_profiler
            pipeline_text = (
                f"\n━━━ 漏斗 ━━━\n"
                f"Dev审查: {ds.total_screened}→{ds.total_passed}过 | "
                f"Profiler: {bp.total_profiled}→{bp.total_passed}过\n"
                f"AI: {self.ai.total_calls}→{self.ai.total_passed}过")

        return (
            f"📊 <b>v{BOT_VERSION}</b> {mode} {paused} | BNB: ${bnb_usd:.2f}\n"
            f"余额: <b>{bal:.4f} BNB</b> (~${bal * bnb_usd:,.0f})\n"
            f"内盘: {real_count}/{self.cfg.risk_cfg['MAX_CONCURRENT_POSITIONS']}"
            f"{f' (+{len(self.launch_positions)}博发射)' if self.launch_positions else ''}"
            f" | 外盘: {ext_count}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{self.risk.get_status_text()}"
            f"{pipeline_text}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{self.ai.get_status_text()}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{self.detector.get_status_text()}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{self.monitor.get_status_text()}")

    async def get_positions_text(self):
        active = await self.state_mgr.get_active()
        if not active:
            return ""
        r = self.cfg.risk_cfg
        default_graduate_thr = r.get("GRADUATE_THRESHOLD_BNB", 24.0)
        real_count = await self._count_real_positions()
        ext_count = len(self.pancake_mgr.positions) if self.pancake_mgr else 0

        lines = [f"📋 <b>内盘持仓</b> ({len(active)}) 内:{real_count} 外:{ext_count}\n"]
        bnb_usd = self._get_bnb_usd()

        for mint, pos in active.items():
            cur_bnb, cur_tok = await self.monitor.get_pool_state(mint)
            if cur_bnb < 0:
                cur_bnb = 0.0
            init_bnb = float(pos.get('init_pool_bnb', 0))
            mult = self.exec.calc_price_mult(init_bnb, cur_bnb) if init_bnb > 0 else 0
            graduate_thr = self.monitor.get_graduate_bnb(mint, default_graduate_thr)
            curve_pct = min(100.0, (cur_bnb / graduate_thr) * 100.0) if graduate_thr > 0 else 0
            sell_stage = pos.get('sell_stage', 0) or 0
            stage_map = {0: "等出本", 1: "等出利", 2: "博发射🚀"}
            stage_text = stage_map.get(sell_stage, "未知")

            meta = self.monitor.get_curve_meta(mint) if self.monitor else {}
            curve_status = int(meta.get("status", 0) or 0) if meta else 0
            if mint in self.graduated_positions or curve_status == 3:
                stage_text = "🎓已毕业"

            emoji = "🟢" if mult >= 1.5 else ("🟡" if mult >= 1.0 else "🔴")
            age_min = int((time.time() - pos.get('ts', time.time())) / 60)

            # 默认口径（内盘）
            total_supply = self.monitor.get_initial_supply(mint)
            mc_text = await self._fmt_market_cap(cur_bnb, graduate_thr,
                                                  pool_tokens=cur_tok if cur_tok and cur_tok > 0 else 0,
                                                  total_supply=total_supply,
                                                  token_addr=mint)

            # 已毕业优先按外盘池子现价估算当前市值，避免固定 88.2 BNB 假象
            if curve_status == 3:
                try:
                    pair = await self.exec.get_pancake_pair(mint)
                    if pair:
                        token_res, wbnb_res = await self.exec.get_pancake_reserves(pair, mint)
                        if token_res > 0 and wbnb_res > 0 and total_supply > 0:
                            price_bnb = float(wbnb_res) / float(token_res)
                            mc_bnb = price_bnb * (float(total_supply) / 1e18)
                            mc_usd = mc_bnb * bnb_usd
                            if mc_usd >= 1_000_000:
                                mc_text = f"~{mc_bnb:.2f} BNB (~${mc_usd/1_000_000:.2f}M)"
                            elif mc_usd >= 1000:
                                mc_text = f"~{mc_bnb:.2f} BNB (~${mc_usd/1000:.1f}K)"
                            else:
                                mc_text = f"~{mc_bnb:.4f} BNB (~${mc_usd:.0f})"
                except Exception:
                    pass

            lines.append(
                f"{emoji} <b>{pos['symbol']}</b> ({age_min}min) [{stage_text}]\n"
                f"  {mult:.2f}x | 曲线:{curve_pct:.1f}% | MC:{mc_text}\n"
                f"  {_addr_link(mint)}")
        return "\n".join(lines)


# ================= 主入口 =================
async def main():
    TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID")

    if not TG_TOKEN:
        print("❌ 缺少 TELEGRAM_BOT_TOKEN")
        sys.exit(1)
    if not TG_CHAT:
        print("❌ 缺少 TELEGRAM_CHAT_ID")
        sys.exit(1)

    try:
        tg_chat_id = int(TG_CHAT)
    except ValueError:
        print("❌ TELEGRAM_CHAT_ID 必须是整数")
        sys.exit(1)

    cfg = ConfigManager()
    if not cfg.is_ready():
        print("❌ 缺少 BSC_PRIVATE_KEY")
        sys.exit(1)

    loop = asyncio.get_running_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=DEFAULT_THREAD_POOL_SIZE))
    stop_event = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop_event.set)

    tg = TelegramController(TG_TOKEN, tg_chat_id, cfg)
    polling_task = asyncio.create_task(tg.run_polling())

    async def _polling_watchdog():
        nonlocal polling_task
        retry_delay = 2
        while not stop_event.is_set():
            await asyncio.sleep(2)
            if polling_task.done() and not stop_event.is_set():
                err = None
                with contextlib.suppress(asyncio.CancelledError):
                    err = polling_task.exception()
                if err:
                    logger.error(f"❌ [Main] polling_task 异常退出: {type(err).__name__}: {err}")
                else:
                    logger.warning("⚠️ [Main] polling_task 意外结束，准备重启")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)
                polling_task = asyncio.create_task(tg.run_polling())
            else:
                retry_delay = 2

    watchdog_task = asyncio.create_task(_polling_watchdog())
    bot = None

    try:
        temp_w3 = Web3()
        try:
            wallet_addr = temp_w3.eth.account.from_key(cfg.pk).address
        except Exception as e:
            wallet_addr = f"解析失败: {e}"

        cfg_dry = cfg.sys_cfg.get("DRY_RUN")
        dry_run = cfg_dry if isinstance(cfg_dry, bool) else (os.environ.get("DRY_RUN", "false").lower() == "true")
        mode_text = "🧪 模拟盘" if dry_run else "⚔️ 实盘"

        await tg.send_alert(
            f"🤖 <b>v{BOT_VERSION} 就绪</b>\n"
            f"钱包: <code>{wallet_addr}</code>\n"
            f"模式: {mode_text}\n"
            f"👉 先发 <code>/set</code> 打开控制台，直接点 <b>🚀 一键点火</b>\n"
            f"备用命令：<code>/boot {tg.boot_token}</code>")

        boot_wait_task = asyncio.create_task(tg.boot_event.wait())
        stop_wait_task = asyncio.create_task(stop_event.wait())
        done, pending = await asyncio.wait(
            {boot_wait_task, stop_wait_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        if stop_wait_task in done and not tg.boot_event.is_set():
            logger.info("🛑 [Main] 收到停止信号，且尚未 /boot，直接收尾")
            return

        bot = BotOrchestrator(cfg, tg, dry_run=dry_run)
        tg.set_orch(bot)
        await bot.startup(cfg.pk)

        await stop_event.wait()
        logger.info("🛑 [Main] stop_event 已触发，开始收尾")
    except asyncio.CancelledError:
        logger.info("🛑 [Main] 收到取消信号，开始收尾")
    except KeyboardInterrupt:
        logger.info("🛑 [Main] 收到中断信号，开始收尾")
    finally:
        stop_event.set()
        tg.boot_event.clear()
        tg.set_orch(None)
        if bot:
            with contextlib.suppress(Exception):
                await bot.shutdown()
        if watchdog_task:
            watchdog_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await watchdog_task
        if polling_task:
            polling_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await polling_task
        with contextlib.suppress(Exception):
            await tg.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
