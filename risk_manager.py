"""
risk_manager.py — 风控引擎，黑名单，统计
"""

import sqlite3
import logging
from datetime import datetime

logger = logging.getLogger("RiskGuard")

# ================= 风控参数 (TG 可热更新) =================
DEFAULT_RISK_CONFIG = {
    "DAILY_LOSS_LIMIT_BNB": -0.5,
    "DAILY_TRADE_LIMIT": 10,
    "MAX_CONSECUTIVE_LOSSES": 3,
    "COOLDOWN_MINUTES": 30,
    "TRAILING_SL_PCT": 0.8,
    "HARD_SL_MULT": 0.7,
    "DOUBLE_SELL_PCT": 0.5,
    "GRADUATE_THRESHOLD_BNB": 24.0,
}


class RiskGuard:
    """
    钢铁风控引擎
    - can_buy(): 开仓前审核 (黑名单/日限额/连亏冷却)
    - record_buy(): 买入成功后记录日出手数 (v5.1 新增)
    - record_trade(): 平仓后记账 (盈亏统计/连亏计数/黑名单)
    - record_trade(is_orphan=True): 孤儿清理，不影响连败/连胜
    - is_dev_blacklisted(): v5.3 检查 Dev 地址黑名单
    - add_dev_to_blacklist(): v5.3 拉黑 Dev 地址
    """

    def __init__(self, db_path: str = "risk_stats.db", config: dict = None):
        self.db_path = db_path
        self.cfg = dict(DEFAULT_RISK_CONFIG)
        if config:
            self.cfg.update(config)
        self._init_db()
        logger.info("🛡️ [RiskGuard] ═══ 风控引擎就绪 ═══")
        logger.info(f"🛡️ [RiskGuard] 日亏损限额: {self.cfg['DAILY_LOSS_LIMIT_BNB']} BNB")
        logger.info(f"🛡️ [RiskGuard] 日出手上限: {self.cfg['DAILY_TRADE_LIMIT']}")
        logger.info(f"🛡️ [RiskGuard] 连亏熔断: {self.cfg['MAX_CONSECUTIVE_LOSSES']}次 → 冷却{self.cfg['COOLDOWN_MINUTES']}分钟")
        logger.info(f"🛡️ [RiskGuard] 止盈回撤: {self.cfg['TRAILING_SL_PCT']} | 硬止损: {self.cfg['HARD_SL_MULT']}")
        logger.info(f"🛡️ [RiskGuard] 翻倍出本比例: {self.cfg['DOUBLE_SELL_PCT']} | 毕业线: {self.cfg['GRADUATE_THRESHOLD_BNB']} BNB")

    def _get_conn(self):
        return sqlite3.connect(self.db_path, check_same_thread=False)

    def _init_db(self):
        with self._get_conn() as conn:
            c = conn.cursor()
            c.execute('''CREATE TABLE IF NOT EXISTS daily_stats (
                date TEXT PRIMARY KEY,
                trades INTEGER DEFAULT 0,
                pnl_bnb REAL DEFAULT 0.0
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS blacklist (
                token TEXT PRIMARY KEY,
                reason TEXT,
                add_time TEXT
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS system_state (
                key TEXT PRIMARY KEY,
                value TEXT
            )''')
            c.execute("INSERT OR IGNORE INTO system_state (key, value) VALUES ('consecutive_losses', '0')")
            c.execute("INSERT OR IGNORE INTO system_state (key, value) VALUES ('cooldown_until', '0')")
            c.execute("INSERT OR IGNORE INTO system_state (key, value) VALUES ('consecutive_wins', '0')")
            c.execute("INSERT OR IGNORE INTO system_state (key, value) VALUES ('total_wins', '0')")
            c.execute("INSERT OR IGNORE INTO system_state (key, value) VALUES ('total_losses', '0')")
            c.execute("INSERT OR IGNORE INTO system_state (key, value) VALUES ('total_orphans', '0')")
            c.execute("INSERT OR IGNORE INTO system_state (key, value) VALUES ('total_draws', '0')")

            for col_sql in [
                "ALTER TABLE daily_stats ADD COLUMN wins INTEGER DEFAULT 0",
                "ALTER TABLE daily_stats ADD COLUMN losses INTEGER DEFAULT 0",
                "ALTER TABLE daily_stats ADD COLUMN orphan_cleaned INTEGER DEFAULT 0",
                "ALTER TABLE daily_stats ADD COLUMN draws INTEGER DEFAULT 0",
                # v5.3: Dev 地址黑名单支持
                "ALTER TABLE blacklist ADD COLUMN addr_type TEXT DEFAULT 'token'",
            ]:
                try:
                    c.execute(col_sql)
                except Exception:
                    pass

            conn.commit()

    def _today(self):
        return datetime.now().strftime("%Y-%m-%d")

    def _get_state(self, conn, key, default='0'):
        c = conn.cursor()
        c.execute("SELECT value FROM system_state WHERE key = ?", (key,))
        row = c.fetchone()
        return row[0] if row else default

    # ================= 第一道防线：开仓前审核 =================

    def can_buy(self, token_addr: str) -> tuple:
        token_lower = token_addr.lower()
        today = self._today()
        now_ts = datetime.now().timestamp()

        logger.info(f"🛡️ [RiskGuard] ═══ 开仓审核: {token_addr[:10]}... ═══")

        with self._get_conn() as conn:
            c = conn.cursor()

            # 1. 黑名单 (v5.3: 只查 token 类型)
            c.execute("SELECT reason FROM blacklist WHERE token = ? AND (addr_type = 'token' OR addr_type IS NULL)", (token_lower,))
            row = c.fetchone()
            if row:
                logger.info(f"🛡️ [RiskGuard] ❌ 黑名单拦截: {row[0]}")
                return False, f"黑名单: {row[0]}"
            logger.info(f"🛡️ [RiskGuard] ✅ 黑名单检查通过")

            # 2. 连亏冷却
            cooldown_until = float(self._get_state(conn, 'cooldown_until', '0'))
            if now_ts < cooldown_until:
                remain = int((cooldown_until - now_ts) / 60)
                logger.info(f"🛡️ [RiskGuard] ❌ 连亏熔断中 (剩余 {remain} 分钟)")
                return False, f"连亏熔断中 (剩余 {remain} 分钟)"
            logger.info(f"🛡️ [RiskGuard] ✅ 连亏冷却检查通过")

            # 3. 日级断路器
            c.execute("SELECT trades, pnl_bnb FROM daily_stats WHERE date = ?", (today,))
            row = c.fetchone()
            if row:
                trades, pnl = row[0], row[1]
                logger.info(f"🛡️ [RiskGuard] 今日战绩: {trades}笔 / 盈亏 {pnl:+.4f} BNB")

                if trades >= self.cfg["DAILY_TRADE_LIMIT"]:
                    logger.info(f"🛡️ [RiskGuard] ❌ 日出手上限 ({trades}/{self.cfg['DAILY_TRADE_LIMIT']})")
                    return False, f"日出手上限 ({trades}/{self.cfg['DAILY_TRADE_LIMIT']})"
                if pnl <= self.cfg["DAILY_LOSS_LIMIT_BNB"]:
                    logger.info(f"🛡️ [RiskGuard] ❌ 日亏损达标 ({pnl:.4f}/{self.cfg['DAILY_LOSS_LIMIT_BNB']})")
                    return False, f"日亏损达标 ({pnl:.4f}/{self.cfg['DAILY_LOSS_LIMIT_BNB']})"
                logger.info(f"🛡️ [RiskGuard] ✅ 日限额检查通过 (出手: {trades}/{self.cfg['DAILY_TRADE_LIMIT']} | 盈亏: {pnl:+.4f}/{self.cfg['DAILY_LOSS_LIMIT_BNB']})")
            else:
                logger.info(f"🛡️ [RiskGuard] ✅ 今日首笔交易")

        logger.info(f"🛡️ [RiskGuard] 🟢 全部审核通过 → 放行")
        return True, "风控绿灯"

    # ================= v5.1 新增: 买入成功后记录出手 =================

    def record_buy(self, token_addr: str):
        today = self._today()
        with self._get_conn() as conn:
            c = conn.cursor()
            c.execute(
                "INSERT OR IGNORE INTO daily_stats (date, trades, pnl_bnb, wins, losses, orphan_cleaned, draws) VALUES (?, 0, 0.0, 0, 0, 0, 0)",
                (today,)
            )
            c.execute(
                "UPDATE daily_stats SET trades = trades + 1 WHERE date = ?",
                (today,)
            )
            conn.commit()

            c.execute("SELECT trades FROM daily_stats WHERE date = ?", (today,))
            row = c.fetchone()
            logger.info(f"🛡️ [RiskGuard] 📝 记录出手: {token_addr[:10]}... | 今日第 {row[0]} 笔")

    # ================= v5.2: 获取今日统计 =================

    def get_daily_stats(self) -> dict:
        today = self._today()
        with self._get_conn() as conn:
            c = conn.cursor()
            c.execute("SELECT trades, pnl_bnb, wins, losses, orphan_cleaned, draws FROM daily_stats WHERE date = ?", (today,))
            row = c.fetchone()
            if row:
                return {
                    "trades": row[0], "pnl_bnb": row[1],
                    "wins": row[2] or 0, "losses": row[3] or 0,
                    "orphans": row[4] or 0, "draws": row[5] or 0,
                }
            return {"trades": 0, "pnl_bnb": 0.0, "wins": 0, "losses": 0, "orphans": 0, "draws": 0}

    # ================= 第二道防线：平仓后记账 =================

    def record_trade(self, token_addr: str, realized_pnl_bnb: float, is_orphan: bool = False) -> dict:
        token_lower = token_addr.lower()
        today = self._today()
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        result = {
            "circuit_breaker_triggered": False,
            "reason": "",
            "consecutive_losses": 0,
            "cooldown_minutes": 0,
            "daily_pnl": 0.0,
            "daily_trades": 0,
        }

        if is_orphan:
            logger.info(f"🧹 [RiskGuard] ═══ 孤儿清理记账 (不计连败): {token_addr[:10]}... ═══")
        else:
            logger.info(f"🛡️ [RiskGuard] ═══ 平仓记账: {token_addr[:10]}... ═══")
        logger.info(f"🛡️ [RiskGuard] 实现盈亏: {realized_pnl_bnb:+.4f} BNB | 孤儿: {is_orphan}")

        with self._get_conn() as conn:
            c = conn.cursor()

            # 1. 拉入黑名单 (保持 addr_type='token')
            c.execute(
                "INSERT OR REPLACE INTO blacklist (token, reason, add_time, addr_type) VALUES (?, ?, ?, 'token')",
                (token_lower, "孤儿清理" if is_orphan else "已交易完毕", now_str)
            )
            logger.info(f"🛡️ [RiskGuard] ✅ 已拉入黑名单: {token_addr[:10]}...")

            # 2. 更新日盈亏
            c.execute(
                "INSERT OR IGNORE INTO daily_stats (date, trades, pnl_bnb, wins, losses, orphan_cleaned, draws) VALUES (?, 0, 0.0, 0, 0, 0, 0)",
                (today,)
            )

            if is_orphan:
                c.execute(
                    "UPDATE daily_stats SET pnl_bnb = pnl_bnb + ?, orphan_cleaned = orphan_cleaned + 1 WHERE date = ?",
                    (realized_pnl_bnb, today)
                )
                total_orphans = int(self._get_state(conn, 'total_orphans', '0'))
                c.execute("INSERT OR REPLACE INTO system_state (key, value) VALUES ('total_orphans', ?)",
                          (str(total_orphans + 1),))
                logger.info(f"🧹 [RiskGuard] 孤儿清理完成，不影响连败/连胜 (累计孤儿: {total_orphans + 1})")
            else:
                if realized_pnl_bnb > 0:
                    c.execute(
                        "UPDATE daily_stats SET pnl_bnb = pnl_bnb + ?, wins = wins + 1 WHERE date = ?",
                        (realized_pnl_bnb, today)
                    )
                elif realized_pnl_bnb < 0:
                    c.execute(
                        "UPDATE daily_stats SET pnl_bnb = pnl_bnb + ?, losses = losses + 1 WHERE date = ?",
                        (realized_pnl_bnb, today)
                    )
                else:
                    c.execute(
                        "UPDATE daily_stats SET pnl_bnb = pnl_bnb + ?, draws = draws + 1 WHERE date = ?",
                        (realized_pnl_bnb, today)
                    )

            c.execute("SELECT trades, pnl_bnb FROM daily_stats WHERE date = ?", (today,))
            row = c.fetchone()
            if row:
                logger.info(f"🛡️ [RiskGuard] 今日更新: {row[0]}笔出手 / 盈亏 {row[1]:+.4f} BNB")
                result["daily_pnl"] = row[1]
                result["daily_trades"] = row[0]

            # 3. 连亏/连胜处理
            if not is_orphan:
                consecutive_losses = int(self._get_state(conn, 'consecutive_losses', '0'))
                consecutive_wins = int(self._get_state(conn, 'consecutive_wins', '0'))
                total_wins = int(self._get_state(conn, 'total_wins', '0'))
                total_losses = int(self._get_state(conn, 'total_losses', '0'))
                total_draws = int(self._get_state(conn, 'total_draws', '0'))

                if realized_pnl_bnb > 0:
                    old_losses = consecutive_losses
                    consecutive_wins += 1
                    total_wins += 1
                    c.execute("INSERT OR REPLACE INTO system_state (key, value) VALUES ('consecutive_losses', '0')")
                    c.execute("INSERT OR REPLACE INTO system_state (key, value) VALUES ('consecutive_wins', ?)",
                              (str(consecutive_wins),))
                    c.execute("INSERT OR REPLACE INTO system_state (key, value) VALUES ('total_wins', ?)",
                              (str(total_wins),))
                    logger.info(
                        f"🏆 [RiskGuard] 盈利 +{realized_pnl_bnb:.4f} BNB | "
                        f"连亏清零 ({old_losses}→0) | 连胜: {consecutive_wins} | "
                        f"总战绩: {total_wins}W/{total_losses}L/{total_draws}D"
                    )
                elif realized_pnl_bnb < 0:
                    old_wins = consecutive_wins
                    consecutive_losses += 1
                    total_losses += 1
                    c.execute("INSERT OR REPLACE INTO system_state (key, value) VALUES ('consecutive_wins', '0')")
                    c.execute("INSERT OR REPLACE INTO system_state (key, value) VALUES ('consecutive_losses', ?)",
                              (str(consecutive_losses),))
                    c.execute("INSERT OR REPLACE INTO system_state (key, value) VALUES ('total_losses', ?)",
                              (str(total_losses),))
                    logger.warning(
                        f"🩸 [RiskGuard] 亏损 {realized_pnl_bnb:.4f} BNB | "
                        f"连胜清零 ({old_wins}→0) | "
                        f"连亏: {consecutive_losses}/{self.cfg['MAX_CONSECUTIVE_LOSSES']} | "
                        f"总战绩: {total_wins}W/{total_losses}L/{total_draws}D"
                    )

                    result["consecutive_losses"] = consecutive_losses

                    if consecutive_losses >= self.cfg["MAX_CONSECUTIVE_LOSSES"]:
                        cooldown_until = datetime.now().timestamp() + (self.cfg["COOLDOWN_MINUTES"] * 60)
                        c.execute(
                            "INSERT OR REPLACE INTO system_state (key, value) VALUES ('cooldown_until', ?)",
                            (str(cooldown_until),)
                        )
                        c.execute(
                            "INSERT OR REPLACE INTO system_state (key, value) VALUES ('consecutive_losses', '0')"
                        )
                        logger.error(
                            f"🛑 [RiskGuard] ═══ {consecutive_losses} 连亏触发熔断! ═══\n"
                            f"  强制静默 {self.cfg['COOLDOWN_MINUTES']} 分钟\n"
                            f"  连败计数已重置 (冷却后重新开始)"
                        )
                        result["circuit_breaker_triggered"] = True
                        result["reason"] = f"连续 {consecutive_losses} 次亏损"
                        result["cooldown_minutes"] = self.cfg["COOLDOWN_MINUTES"]
                else:
                    total_draws += 1
                    c.execute("INSERT OR REPLACE INTO system_state (key, value) VALUES ('total_draws', ?)",
                              (str(total_draws),))
                    logger.info(
                        f"➖ [RiskGuard] 平局 (盈亏=0) | "
                        f"连胜/连亏不变 | 总战绩: {total_wins}W/{total_losses}L/{total_draws}D"
                    )

            conn.commit()

        return result

    # ================= 拉黑接口 =================

    def add_to_blacklist(self, token_addr: str, reason: str):
        token_lower = token_addr.lower()
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._get_conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO blacklist (token, reason, add_time, addr_type) VALUES (?, ?, ?, 'token')",
                (token_lower, reason, now_str)
            )
            conn.commit()
        logger.info(f"🚫 [RiskGuard] 拉黑: {token_addr[:8]} | {reason}")

    # ================= v5.3: Dev 地址黑名单 =================

    def is_dev_blacklisted(self, dev_addr: str) -> bool:
        """检查 dev 地址是否在黑名单中"""
        addr_lower = dev_addr.lower()
        with self._get_conn() as conn:
            c = conn.cursor()
            c.execute(
                "SELECT reason FROM blacklist WHERE token = ? AND addr_type = 'dev_address'",
                (addr_lower,)
            )
            return c.fetchone() is not None

    def add_dev_to_blacklist(self, dev_addr: str, reason: str):
        """将 dev 地址加入黑名单"""
        addr_lower = dev_addr.lower()
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._get_conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO blacklist (token, reason, add_time, addr_type) VALUES (?, ?, ?, 'dev_address')",
                (addr_lower, reason, now_str)
            )
            conn.commit()
        logger.info(f"🚫 [RiskGuard] Dev拉黑: {dev_addr[:12]}... | {reason}")

    def get_dev_blacklist_count(self) -> int:
        """获取 dev 黑名单数量"""
        with self._get_conn() as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM blacklist WHERE addr_type = 'dev_address'")
            return c.fetchone()[0]

    # ================= TG 热更新 =================

    def update_config(self, **kwargs):
        updated = []
        for k, v in kwargs.items():
            if k in self.cfg and v is not None:
                old = self.cfg[k]
                self.cfg[k] = v
                updated.append(f"{k}: {old}→{v}")
                logger.info(f"⚙️ [RiskGuard] 参数更新: {k} = {old} → {v}")
        return updated

    # ================= TG 状态面板 =================

    def get_status_text(self) -> str:
        today = self._today()
        with self._get_conn() as conn:
            c = conn.cursor()

            c.execute("SELECT trades, pnl_bnb, wins, losses, orphan_cleaned, draws FROM daily_stats WHERE date = ?", (today,))
            row = c.fetchone()
            if row:
                trades, pnl = row[0], row[1]
                today_wins = row[2] if row[2] else 0
                today_losses = row[3] if row[3] else 0
                today_orphans = row[4] if row[4] else 0
                today_draws = row[5] if row[5] else 0
            else:
                trades, pnl, today_wins, today_losses, today_orphans, today_draws = 0, 0.0, 0, 0, 0, 0

            consecutive_losses = int(self._get_state(conn, 'consecutive_losses', '0'))
            consecutive_wins = int(self._get_state(conn, 'consecutive_wins', '0'))
            total_wins = int(self._get_state(conn, 'total_wins', '0'))
            total_losses = int(self._get_state(conn, 'total_losses', '0'))
            total_orphans = int(self._get_state(conn, 'total_orphans', '0'))
            total_draws = int(self._get_state(conn, 'total_draws', '0'))
            cooldown = float(self._get_state(conn, 'cooldown_until', '0'))

            c.execute("SELECT COUNT(*) FROM blacklist WHERE addr_type = 'token' OR addr_type IS NULL")
            bl_count = c.fetchone()[0]

            # v5.3: Dev 黑名单计数
            c.execute("SELECT COUNT(*) FROM blacklist WHERE addr_type = 'dev_address'")
            dev_bl_count = c.fetchone()[0]

        now_ts = datetime.now().timestamp()
        if now_ts < cooldown:
            remain = int((cooldown - now_ts) / 60)
            status = f"🔴 熔断中 (剩余 {remain} 分钟)"
        else:
            status = "🟢 正常"

        pnl_emoji = "📈" if pnl >= 0 else "📉"

        total_decided = total_wins + total_losses
        win_rate = (total_wins / total_decided * 100) if total_decided > 0 else 0.0
        today_decided = today_wins + today_losses
        today_win_rate = (today_wins / today_decided * 100) if today_decided > 0 else 0.0

        if consecutive_wins > 0:
            streak_text = f"🔥 连胜: {consecutive_wins}"
        elif consecutive_losses > 0:
            streak_text = f"💔 连亏: {consecutive_losses}/{self.cfg['MAX_CONSECUTIVE_LOSSES']}"
        else:
            streak_text = "➖ 无连续"

        return (
            f"🛡️ <b>风控引擎状态</b>\n"
            f"系统: {status}\n"
            f"━━━ 今日战绩 ━━━\n"
            f"出手: {trades}/{self.cfg['DAILY_TRADE_LIMIT']}\n"
            f"{pnl_emoji} 盈亏: {pnl:+.4f} BNB\n"
            f"胜/负/平: {today_wins}W/{today_losses}L/{today_draws}D"
            f"{f' (孤儿{today_orphans})' if today_orphans > 0 else ''}\n"
            f"今日胜率: {today_win_rate:.0f}%\n"
            f"━━━ 累计战绩 ━━━\n"
            f"总战绩: {total_wins}W/{total_losses}L/{total_draws}D"
            f"{f' (孤儿{total_orphans})' if total_orphans > 0 else ''}\n"
            f"总胜率: <b>{win_rate:.0f}%</b>\n"
            f"{streak_text}\n"
            f"━━━ 风控参数 ━━━\n"
            f"日亏损限额: {self.cfg['DAILY_LOSS_LIMIT_BNB']} BNB\n"
            f"日出手上限: {self.cfg['DAILY_TRADE_LIMIT']}\n"
            f"连亏熔断: {self.cfg['MAX_CONSECUTIVE_LOSSES']}次/{self.cfg['COOLDOWN_MINUTES']}分钟\n"
            f"止盈回撤: {self.cfg['TRAILING_SL_PCT']} | 硬止损: {self.cfg['HARD_SL_MULT']}\n"
            f"翻倍出本: {self.cfg['DOUBLE_SELL_PCT']} | 毕业线: {self.cfg['GRADUATE_THRESHOLD_BNB']} BNB\n"
            f"黑名单: 代币 {bl_count} | Dev {dev_bl_count}"
        )
