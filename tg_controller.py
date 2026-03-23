"""
tg_controller.py — Telegram 控制面板
"""

import asyncio
import logging
import time
import secrets
import contextlib
from typing import Any, Awaitable, Callable, Dict

from aiogram import Bot, Dispatcher, Router, types, F, BaseMiddleware
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

logger = logging.getLogger("TG_Controller")

# ================= 发送限速 =================
TG_SEND_INTERVAL = 1.0          # 两条消息之间最少间隔 (秒)
TG_SEND_QUEUE_MAX = 100         # 队列最大长度，超出丢弃最早的
TG_RETRY_AFTER_DEFAULT = 30     # 429 时默认等待秒数


class ChatAuthMiddleware(BaseMiddleware):
    """只允许配置的 chat 控制 bot。"""

    def __init__(self, allowed_chat_id: int):
        self.allowed_chat_id = int(allowed_chat_id)

    @staticmethod
    def _extract_chat_id(event: Any):
        chat = getattr(event, "chat", None)
        if chat is not None and getattr(chat, "id", None) is not None:
            return chat.id
        msg = getattr(event, "message", None)
        if msg is not None:
            msg_chat = getattr(msg, "chat", None)
            if msg_chat is not None and getattr(msg_chat, "id", None) is not None:
                return msg_chat.id
        return None

    async def __call__(self, handler: Callable[[Any, Dict[str, Any]], Awaitable[Any]], event: Any, data: Dict[str, Any]):
        chat_id = self._extract_chat_id(event)
        if chat_id != self.allowed_chat_id:
            user_id = getattr(getattr(event, "from_user", None), "id", None)
            logger.warning(f"⛔ [TG] 拒绝未授权控制 | chat={chat_id} user={user_id}")
            if isinstance(event, types.CallbackQuery):
                try:
                    await event.answer("⛔️ 未授权", show_alert=True)
                except Exception:
                    pass
            return None
        return await handler(event, data)


# ================= 全参数映射表 (去重后) =================
PARAM_HELP = {
    "MONITOR_DURATION_SEC":             ("动量观察期(秒)", "int"),
    "MOMENTUM_BNB_THRESHOLD":           ("动量 BNB 阈值", "float"),
    "UP_TICKS_REQUIRED":                ("上涨 tick 要求", "int"),
    "MIN_UNIQUE_BUYERS":                ("最少独立买家", "int"),
    "MAX_DEV_HOLDING_PCT":              ("最大控盘比例(%)", "float"),
    "SELL_PRESSURE_RATIO_PCT":          ("卖压占比阈值(%)", "float"),
    "SELL_PRESSURE_DEV_OUTFLOW_PCT":    ("Dev净流出阈值(%)", "float"),
    "SELL_PRESSURE_LARGE_SELL_COUNT":   ("大额卖出次数阈值", "int"),
    "SELL_PRESSURE_MIN_VOLUME_TOKENS":  ("卖压启用最小成交量(token)", "int"),
    "MOMENTUM_CONFIRM_DELAY_SEC":       ("动量确认等待(秒)", "int"),
    "MOMENTUM_CONFIRM_PULLBACK_BNB":    ("动量确认回撤阈值(BNB)", "float"),
    "AI_SCORE_THRESHOLD":               ("AI 拦截评分阈值", "float"),
    "AI_MULTIPLIER_HIGH":               ("AI 高分(≥80)乘数", "float"),
    "AI_MULTIPLIER_MID":                ("AI 中分(60-79)乘数", "float"),
    "AI_MULTIPLIER_LOW":                ("AI 低分(<60)乘数", "float"),
    "BUY_BNB_AMT":                      ("底仓(BNB)", "float"),
    "HARD_CAP_BNB":                     ("仓位硬上限(BNB)", "float"),
    "DOUBLE_SELL_MULT":                 ("出本触发倍数", "float"),
    "DOUBLE_SELL_PCT":                  ("出本卖出比例", "float"),
    "SIDEWAYS_EXIT_SEC":                ("横盘超时(秒)", "int"),
    "SIDEWAYS_MIN_MULT":                ("横盘下限倍数", "float"),
    "SIDEWAYS_MAX_MULT":                ("横盘上限倍数", "float"),
    "TRAILING_SL_PCT":                  ("移动止盈回撤比", "float"),
    "HARD_SL_MULT":                     ("硬止损倍数", "float"),
    "GRADUATE_THRESHOLD_BNB":           ("毕业阈值(BNB)", "float"),
    "MAX_CONCURRENT_POSITIONS":         ("最大持仓数", "int"),
    "MAX_EXTERNAL_TRACKING":            ("外盘最大追踪数", "int"),
    "MAX_CONCURRENT_MONITORS":          ("最大监控并发", "int"),
    "DAILY_TRADE_LIMIT":                ("日出手上限", "int"),
    "DAILY_LOSS_LIMIT_BNB":             ("日最大亏损(BNB)", "float"),
    "MAX_CONSECUTIVE_LOSSES":           ("连亏熔断次数", "int"),
    "COOLDOWN_MINUTES":                 ("连亏冷却(分钟)", "int"),
    # Phase 1: Dev 画像
    "DEV_SCREEN_ENABLED":               ("Dev画像开关", "bool"),
    "DEV_SCREEN_TIMEOUT":               ("Dev画像总预算(秒)", "float"),
    "DEV_FAST_TIMEOUT":                 ("快审预算(秒)", "float"),
    "DEV_HEAD_TIMEOUT":                 ("块高查询预算(秒)", "float"),
    "DEV_QUERY_1H_TIMEOUT":             ("1h发币查询预算(秒)", "float"),
    "DEV_QUERY_6H_TIMEOUT":             ("6h发币查询预算(秒)", "float"),
    "DEV_QUERY_12H_TIMEOUT":            ("12h发币查询预算(秒)", "float"),
    "DEV_MIN_NONCE":                    ("C级手套阈值: nonce≤此值", "int"),
    "DEV_MIN_BALANCE_BNB":              ("C级手套阈值: 余额<此值", "float"),
    "DEV_A_MIN_NONCE":                  ("A级门槛: nonce≥此值", "int"),
    "DEV_A_MIN_BALANCE_BNB":            ("A级门槛: 余额≥此值", "float"),
    "DEV_A_MAX_DEPLOYS_1H":             ("A级门槛: 1h发币≤此值", "int"),
    "DEV_A_MAX_DEPLOYS_6H":             ("A级门槛: 6h发币≤此值", "int"),
    "DEV_A_MAX_DEPLOYS_12H":            ("A级门槛: 12h发币≤此值", "int"),
    "DEV_C_MAX_DEPLOYS_1H":             ("C级踢出: 1h发币≥此值", "int"),
    "DEV_C_MAX_DEPLOYS_6H":             ("C级踢出: 6h发币≥此值", "int"),
    "DEV_C_MAX_DEPLOYS_12H":            ("C级踢出: 12h发币≥此值", "int"),
    # Phase 2: 买家侧写
    "PROFILER_ENABLED":                 ("侧写雷达开关", "bool"),
    "PROFILER_SAMPLE_SIZE":             ("侧写抽样买家数", "int"),
    "PROFILER_TIMEOUT":                 ("侧写超时(秒)", "float"),
    "PROFILER_DANGER_THRESHOLD":        ("单人危险阈值(分)", "int"),
    "PROFILER_CABAL_RATIO":             ("Cabal判定人数", "int"),
    "SCORE_DISPOSABLE_WALLET":          ("一次性钱包权重", "int"),
    "SCORE_SNIPER_BOT":                 ("狙击机器人权重", "int"),
    "SCORE_CROSS_LAUNCH":               ("跨盘串联权重", "int"),
    "SCORE_SAME_BLOCK":                 ("同块聚集权重", "int"),
    "SCORE_NONCE_UNIFORMITY":           ("nonce一致性权重", "int"),
}


class TelegramController:
    def __init__(self, token, chat_id, config_mgr):
        self.bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        self.dp = Dispatcher()
        self.router = Router()
        self.chat_id = int(chat_id)
        self.cfg = config_mgr
        self.orch = None
        self.boot_event = asyncio.Event()
        self.boot_token = secrets.token_hex(4)
        self.dp.include_router(self.router)
        self.router.message.outer_middleware(ChatAuthMiddleware(self.chat_id))
        self.router.callback_query.outer_middleware(ChatAuthMiddleware(self.chat_id))
        self.user_states = {}

        # ★ v14.1: 发送队列
        self._send_queue: asyncio.Queue = None  # 在 run_polling 中初始化
        self._send_task = None
        self._last_send_time = 0
        self._boot_cutoff_ts = time.time()
        self._total_sent = 0
        self._total_dropped = 0
        self._total_throttled = 0
        self._polling_stop = False

        self._register_commands()
        self._register_callbacks()

    def set_orch(self, o):
        self.orch = o

    def _kb(self, rows):
        return InlineKeyboardMarkup(inline_keyboard=rows)

    def _set_home_text(self):
        if not self.orch:
            return (
                f"📊 <b>总览</b>\n"
                f"状态: 🟡 待点火\n"
                f"模式: 🧪 模拟盘\n"
                f"操作: 直接点下面的 <b>🚀 一键点火</b>\n\n"
                f"备用命令: <code>/boot {self.boot_token}</code>"
            )

        mode = "🧪 模拟盘" if self.orch.dry_run else "⚔️ 实盘"
        paused = "⏸️ 已暂停" if self.orch.paused else "▶️ 运行中"
        active = len(self.orch.active_snipes) if hasattr(self.orch, 'active_snipes') else 0
        return (
            f"📊 <b>总览</b>\n"
            f"状态: 🟢 已点火 | {paused}\n"
            f"模式: {mode}\n"
            f"监控中目标: {active}\n\n"
            f"下面点按钮看详情或改参数。"
        )

    def _set_home_kb(self):
        rows = [
            [InlineKeyboardButton(text="📊 总览", callback_data="set_status_menu"),
             InlineKeyboardButton(text="⚙️ 参数", callback_data="set_config_menu")],
            [InlineKeyboardButton(text="🧩 模块", callback_data="set_modules_menu"),
             InlineKeyboardButton(text="🎛️ 动作", callback_data="set_actions_menu")],
        ]
        if self.orch:
            rows.append([InlineKeyboardButton(text="💼 仓位", callback_data="set_positions_menu"),
                         InlineKeyboardButton(text="🌐 RPC", callback_data="set_rpc_menu")])
        else:
            rows.append([InlineKeyboardButton(text="🚀 一键点火", callback_data=f"act_boot_{self.boot_token}")])
        rows.append([InlineKeyboardButton(text="❌ 关闭", callback_data="close_menu")])
        return self._kb(rows)

    def _back_main_kb(self):
        return self._kb([[InlineKeyboardButton(text="🔙 返回主页", callback_data="set_home")]])

    def _set_config_menu_kb(self):
        return self._kb([
            [InlineKeyboardButton(text="🛡️ 风控", callback_data="set_risk"),
             InlineKeyboardButton(text="📊 交易", callback_data="set_trade")],
            [InlineKeyboardButton(text="🤖 AI", callback_data="set_ai"),
             InlineKeyboardButton(text="⚡ 监控", callback_data="set_monitor")],
            [InlineKeyboardButton(text="🔍 Dev审查", callback_data="set_dev"),
             InlineKeyboardButton(text="🕵️ 买家侧写", callback_data="set_profiler")],
            [InlineKeyboardButton(text="🔙 返回主页", callback_data="set_home")]
        ])

    def _set_status_menu_kb(self):
        return self._kb([
            [InlineKeyboardButton(text="总览", callback_data="view_status"),
             InlineKeyboardButton(text="今日统计", callback_data="view_stats")],
            [InlineKeyboardButton(text="风控漏斗", callback_data="view_pipeline"),
             InlineKeyboardButton(text="风控状态", callback_data="view_risk")],
            [InlineKeyboardButton(text="余额", callback_data="view_balance"),
             InlineKeyboardButton(text="内盘仓位", callback_data="view_positions")],
            [InlineKeyboardButton(text="🔙 返回主页", callback_data="set_home")]
        ])

    def _set_modules_menu_kb(self):
        return self._kb([
            [InlineKeyboardButton(text="AI", callback_data="view_ai"),
             InlineKeyboardButton(text="Dev", callback_data="view_dev")],
            [InlineKeyboardButton(text="Profiler", callback_data="view_profiler"),
             InlineKeyboardButton(text="Monitor", callback_data="view_monitor")],
            [InlineKeyboardButton(text="Radar", callback_data="view_radar"),
             InlineKeyboardButton(text="Price", callback_data="view_price")],
            [InlineKeyboardButton(text="Nonce", callback_data="view_nonce"),
             InlineKeyboardButton(text="Executor", callback_data="view_executor")],
            [InlineKeyboardButton(text="🔙 返回主页", callback_data="set_home")]
        ])

    def _set_actions_menu_kb(self):
        rows = []
        if not self.orch:
            rows.append([InlineKeyboardButton(text="🚀 一键点火", callback_data=f"act_boot_{self.boot_token}")])
        else:
            rows.extend([
                [InlineKeyboardButton(text="⏸️ 暂停", callback_data="act_pause"),
                 InlineKeyboardButton(text="▶️ 恢复", callback_data="act_resume")],
                [InlineKeyboardButton(text="🧪 模拟盘", callback_data="act_dry_on"),
                 InlineKeyboardButton(text="⚔️ 实盘", callback_data="act_dry_off")],
                [InlineKeyboardButton(text="🧹 清孤儿", callback_data="act_clean_orphans")],
            ])
        rows.append([InlineKeyboardButton(text="🔙 返回主页", callback_data="set_home")])
        return self._kb(rows)

    def _set_positions_menu_kb(self):
        return self._kb([
            [InlineKeyboardButton(text="内盘仓位", callback_data="view_positions"),
             InlineKeyboardButton(text="外盘仓位", callback_data="view_ext")],
            [InlineKeyboardButton(text="余额", callback_data="view_balance"),
             InlineKeyboardButton(text="孤儿仓位", callback_data="view_orphans")],
            [InlineKeyboardButton(text="🔙 返回主页", callback_data="set_home")]
        ])

    def _set_rpc_menu_kb(self):
        return self._kb([
            [InlineKeyboardButton(text="RPC状态", callback_data="view_rpc"),
             InlineKeyboardButton(text="Trace", callback_data="view_trace")],
            [InlineKeyboardButton(text="重置Trace", callback_data="act_reset_trace")],
            [InlineKeyboardButton(text="🔙 返回主页", callback_data="set_home")]
        ])

    # ================= ★ 限速发送队列 =================

    async def _send_worker(self):
        """串行消息发送工人 — 严格限速 1条/秒"""
        while True:
            try:
                text = await self._send_queue.get()

                # 限速
                now = time.time()
                elapsed = now - self._last_send_time
                if elapsed < TG_SEND_INTERVAL:
                    await asyncio.sleep(TG_SEND_INTERVAL - elapsed)

                # 发送
                try:
                    await self.bot.send_message(self.chat_id, text)
                    self._last_send_time = time.time()
                    self._total_sent += 1
                except Exception as e:
                    err_str = str(e).lower()
                    # 处理 429 Retry-After
                    if "retry" in err_str or "429" in err_str or "flood" in err_str:
                        wait = TG_RETRY_AFTER_DEFAULT
                        # 尝试从异常中提取 retry_after 秒数
                        try:
                            if hasattr(e, 'retry_after'):
                                wait = e.retry_after
                            else:
                                import re
                                match = re.search(r'(\d+)', err_str)
                                if match:
                                    val = int(match.group(1))
                                    if 1 <= val <= 600:
                                        wait = val
                        except Exception:
                            pass
                        self._total_throttled += 1
                        logger.warning(f"⚠️ [TG] 限流! 等待 {wait}s")
                        await asyncio.sleep(wait)
                        # 重试一次
                        try:
                            await self.bot.send_message(self.chat_id, text)
                            self._last_send_time = time.time()
                            self._total_sent += 1
                        except Exception:
                            logger.debug(f"[TG] 重试失败，丢弃消息")
                            self._total_dropped += 1
                    else:
                        logger.debug(f"[TG] 发送失败: {e}")
                        self._total_dropped += 1

                self._send_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[TG] send_worker 异常: {e}")
                await asyncio.sleep(1)

    async def send_alert(self, text):
        """入队发送 — 不阻塞调用方"""
        if self._send_queue is None:
            # 队列还没初始化，直接发
            try:
                await self.bot.send_message(self.chat_id, text)
            except Exception:
                pass
            return

        # 队列满了丢弃最早的
        if self._send_queue.qsize() >= TG_SEND_QUEUE_MAX:
            try:
                self._send_queue.get_nowait()
                self._total_dropped += 1
            except asyncio.QueueEmpty:
                pass

        await self._send_queue.put(text)

    async def send_alert_priority(self, text):
        """优先发送 — 直接发，不排队 (用于命令响应)"""
        try:
            await self.bot.send_message(self.chat_id, text)
            self._last_send_time = time.time()
            self._total_sent += 1
        except Exception as e:
            logger.debug(f"[TG] 优先发送失败: {e}")

    # ================= 命令注册 =================

    def _register_commands(self):
        r = self.router

        @r.message(Command("boot"))
        async def cmd_boot(m: types.Message):
            msg_ts = m.date.timestamp() if getattr(m, 'date', None) else time.time()
            text = (m.text or "").strip()
            parts = text.split(maxsplit=1)
            supplied = parts[1].strip() if len(parts) > 1 else ""
            if msg_ts < self._boot_cutoff_ts:
                logger.warning(f"⛔ [TG] 拒绝陈旧 /boot | msg_ts={msg_ts:.3f} cutoff={self._boot_cutoff_ts:.3f}")
                return await m.reply("⛔️ 这条 /boot 是旧消息，已拒绝。请使用本次启动口令重新点火")
            if supplied != self.boot_token:
                logger.warning(f"⛔ [TG] 拒绝无效 /boot 口令 | chat={m.chat.id} user={getattr(m.from_user, 'id', None)}")
                return await m.reply(f"⛔️ /boot 口令无效。请发送：<code>/boot {self.boot_token}</code>")
            self.boot_event.set()
            logger.info(f"🔥 [TG] /boot accepted | chat={m.chat.id} user={getattr(m.from_user, 'id', None)} ts={msg_ts:.3f}")
            await m.reply("🔥 引擎点火！雷达全面开启！")

        @r.message(Command("status"))
        async def cmd_status(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            text = await self.orch.get_full_status()
            # 追加 TG 队列状态
            text += (f"\n━━━ TG 发送 ━━━\n"
                     f"队列: {self._send_queue.qsize() if self._send_queue else 0} | "
                     f"已发: {self._total_sent} | 丢弃: {self._total_dropped} | 限流: {self._total_throttled}")
            await m.reply(text)

        @r.message(Command("rpc"))
        async def cmd_rpc(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            await m.reply(self.orch.fleet.get_stats_text())

        @r.message(Command("risk"))
        async def cmd_risk(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            await m.reply(self.orch.risk.get_status_text())

        @r.message(Command("ai"))
        async def cmd_ai(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            await m.reply(self.orch.ai.get_status_text())

        @r.message(Command("dev"))
        async def cmd_dev(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            if hasattr(self.orch, 'dev_screener') and self.orch.dev_screener:
                await m.reply(self.orch.dev_screener.get_status_text())
            else:
                await m.reply("🔍 Dev 审查模块未加载")

        @r.message(Command("profiler"))
        async def cmd_profiler(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            if hasattr(self.orch, 'buyer_profiler') and self.orch.buyer_profiler:
                await m.reply(self.orch.buyer_profiler.get_status_text())
            else:
                await m.reply("🕵️ 侧写模块未加载")

        @r.message(Command("pipeline"))
        async def cmd_pipeline(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            sections = [f"🏭 <b>风控流水线 v14</b>\n"]
            if hasattr(self.orch, 'dev_screener') and self.orch.dev_screener:
                ds = self.orch.dev_screener
                rate = (ds.total_blocked / ds.total_screened * 100) if ds.total_screened > 0 else 0
                sections.append(f"<b>Phase 1 Dev审查</b> {'🟢' if ds.cfg.get('DEV_SCREEN_ENABLED') else '🔴'}\n  审查:{ds.total_screened} 拦截:{ds.total_blocked}({rate:.0f}%)")
            if hasattr(self.orch, 'buyer_profiler') and self.orch.buyer_profiler:
                bp = self.orch.buyer_profiler
                rate = (bp.total_cabal_blocked / bp.total_profiled * 100) if bp.total_profiled > 0 else 0
                sections.append(f"<b>Phase 2 买家侧写</b> {'🟢' if bp.cfg.get('PROFILER_ENABLED') else '🔴'}\n  侧写:{bp.total_profiled} Cabal拦截:{bp.total_cabal_blocked}({rate:.0f}%)")
            sections.append(f"<b>Phase 3 AI+动量</b>\n  AI:{self.orch.ai.total_calls}→{self.orch.ai.total_passed}过 | Monitor:{self.orch.monitor.total_confirmed}确认/{self.orch.monitor.total_timeout}超时")
            await m.reply("\n\n".join(sections))

        @r.message(Command("radar"))
        async def cmd_radar(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            await m.reply(self.orch.detector.get_status_text())

        @r.message(Command("monitor"))
        async def cmd_monitor(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            await m.reply(self.orch.monitor.get_status_text())

        @r.message(Command("positions"))
        async def cmd_positions(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            text = await self.orch.get_positions_text()
            await m.reply(text or "📭 当前无持仓")

        @r.message(Command("pause"))
        async def cmd_pause(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            self.orch.paused = True
            await m.reply("⏸️ 已暂停接受新币，现有持仓继续监控")

        @r.message(Command("resume"))
        async def cmd_resume(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            self.orch.paused = False
            await m.reply("▶️ 已恢复新币监控")

        @r.message(Command("kill"))
        async def cmd_kill(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            await m.reply("🚨 收到紧急清仓指令，正在执行...")
            await self.orch.emergency_sell_all()

        @r.message(Command("balance"))
        async def cmd_balance(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            bal = await self.orch.exec.get_bnb_balance()
            vbal = f"\n🧪 虚拟余额: <b>{self.orch.virtual_bnb:.4f} BNB</b>" if self.orch.dry_run else ""
            await m.reply(f"💰 钱包余额: <b>{bal:.4f} BNB</b>{vbal}")

        @r.message(Command("orphans"))
        async def cmd_orphans(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            orphans = await self.orch.detect_orphan_positions()
            if not orphans: return await m.reply("📭 无孤儿仓位")
            lines = [f"👻 <b>孤儿仓位</b> ({len(orphans)})\n"]
            for mint, info in orphans.items():
                lines.append(f"  💀 <b>{info['symbol']}</b> ({info['age_min']}min) | {info['reason']}\n    <code>{mint[:20]}...</code>")
            lines.append("\n💡 /clean_orphans 清理")
            await m.reply("\n".join(lines))

        @r.message(Command("clean_orphans"))
        async def cmd_clean_orphans(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            count = await self.orch.clean_orphan_positions()
            await m.reply(f"🧹 已清理 <b>{count}</b> 个孤儿仓位")

        # ===== 外盘 =====

        @r.message(Command("ext"))
        async def cmd_ext(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            await m.reply(self.orch.pancake_mgr.get_positions_text())

        @r.message(Command("graduated"))
        async def cmd_graduated(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            await m.reply(self.orch.pancake_mgr.get_positions_text())

        @r.message(Command("sell_ext"))
        async def cmd_sell_ext(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            parts = m.text.strip().split(maxsplit=1)
            if len(parts) < 2: return await m.reply("❌ 格式: <code>/sell_ext 代币地址</code>")
            matched = self._match_ext_addr(parts[1].strip())
            if not matched: return await m.reply(f"❌ 未找到匹配的外盘仓位")
            await m.reply(f"🚨 强平: <code>{matched}</code>...")
            result = await self.orch.pancake_mgr.force_sell(matched)
            await m.reply(result)

        @r.message(Command("ignore_ext"))
        async def cmd_ignore_ext(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            parts = m.text.strip().split(maxsplit=1)
            if len(parts) < 2: return await m.reply("❌ 格式: <code>/ignore_ext 代币地址</code>")
            matched = self._match_ext_addr(parts[1].strip())
            if not matched: return await m.reply(f"❌ 未找到匹配的外盘仓位")
            result = await self.orch.pancake_mgr.ignore_position(matched)
            await m.reply(result)

        # ===== 状态 =====

        @r.message(Command("price"))
        async def cmd_price(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            await m.reply(self.orch.price_oracle.get_status_text())

        @r.message(Command("nonce"))
        async def cmd_nonce(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            await m.reply(self.orch.exec.nonce_mgr.get_status_text())

        @r.message(Command("executor"))
        async def cmd_executor(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            await m.reply(self.orch.exec.get_executor_stats())

        # ===== RPC 管理 =====

        @r.message(Command("add_rpc"))
        async def cmd_add_rpc(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            parts = m.text.strip().split(maxsplit=1)
            if len(parts) < 2: return await m.reply("❌ 格式: <code>/add_rpc URL [tier]</code>")
            args = parts[1].strip().split()
            url = args[0]
            tier = int(args[1]) if len(args) > 1 else 1
            if not url.startswith("http"): return await m.reply("❌ URL 必须以 http 开头")
            result = self.orch.fleet.add_node(url, tier=tier)
            if "✅" in result:
                self.cfg.sys_cfg.setdefault("EXTRA_RPCS", []).append(url)
                self.cfg.save_sys()
            await m.reply(result)

        @r.message(Command("del_rpc"))
        async def cmd_del_rpc(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            parts = m.text.strip().split(maxsplit=1)
            if len(parts) < 2: return await m.reply("❌ 格式: <code>/del_rpc URL</code>")
            result = self.orch.fleet.remove_node(parts[1].strip())
            await m.reply(result)

        @r.message(Command("logs_slots"))
        async def cmd_logs_slots(m: types.Message):
            if not self.orch:
                return await m.reply("⚠️ 系统尚未启动")
            parts = (m.text or "").strip().split(maxsplit=1)
            if len(parts) < 2:
                cur = getattr(self.orch.fleet, 'heavy_slots', 8)
                return await m.reply(f"当前 get_logs 并发槽: <b>{cur}</b>\n用法: <code>/logs_slots 5</code>")
            try:
                slots = int(parts[1].strip())
            except Exception:
                return await m.reply("❌ 槽位必须是整数，用法: <code>/logs_slots 5</code>")

            result = self.orch.fleet.set_heavy_slots(slots)
            if result.startswith("✅") or result.startswith("ℹ️"):
                self.cfg.sys_cfg["GET_LOGS_SLOTS"] = int(getattr(self.orch.fleet, 'heavy_slots', slots))
                self.cfg.save_sys()
            await m.reply(result)

        @r.message(Command("trace"))
        async def cmd_trace(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            from rpc_fleet import get_logs_tracer
            await m.reply(get_logs_tracer.get_report())

        @r.message(Command("reset_trace"))
        async def cmd_reset_trace(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            from rpc_fleet import get_logs_tracer
            get_logs_tracer.reset()
            await m.reply("✅ 追踪已重置")

        # ===== ★ v14.1: 模拟盘切换 =====

        @r.message(Command("dry"))
        async def cmd_dry(m: types.Message):
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            parts = m.text.strip().split()
            if len(parts) < 2 or parts[1].lower() not in ("on", "off"):
                current = "🧪 模拟盘" if self.orch.dry_run else "⚔️ 实盘"
                return await m.reply(
                    f"当前: <b>{current}</b>\n\n"
                    f"<code>/dry on</code> 开启模拟盘\n"
                    f"<code>/dry off</code> 切换实盘")
            enable = parts[1].lower() == "on"
            result = await self.orch.toggle_dry_run(enable)
            await m.reply(result)

        # ===== 配置管理 =====

        @r.message(Command("set"))
        async def cmd_set(m: types.Message):
            await m.reply(self._set_home_text(), reply_markup=self._set_home_kb())

        @r.message(Command("stats"))
        async def cmd_stats(m: types.Message):
            """v15: 今日统计摘要"""
            if not self.orch: return await m.reply("⚠️ 系统尚未启动")
            daily = self.orch.risk.get_daily_stats()
            bnb_usd = self.orch._get_bnb_usd()
            bal = await self.orch.exec.get_bnb_balance()
            mode = "🧪 模拟" if self.orch.dry_run else "⚔️ 实盘"
            
            # 漏斗数据
            ds = self.orch.dev_screener
            bp = self.orch.buyer_profiler
            total_in = ds.total_screened if ds else 0
            dev_pass = ds.total_passed if ds else 0
            profiler_pass = bp.total_passed if bp else 0
            ai_pass = self.orch.ai.total_passed
            
            pnl = daily['pnl_bnb']
            pnl_usd = pnl * bnb_usd
            emoji = "📈" if pnl >= 0 else "📉"
            
            text = (
                f"📊 <b>今日统计</b> {mode}\n"
                f"━━━ 战绩 ━━━\n"
                f"出手: {daily['trades']}笔\n"
                f"胜/负/平: {daily['wins']}W / {daily['losses']}L / {daily.get('draws',0)}D\n"
                f"{emoji} 盈亏: <b>{pnl:+.4f} BNB</b> (~${pnl_usd:+,.2f})\n"
                f"━━━ 漏斗 ━━━\n"
                f"新币发现: {total_in}\n"
                f"→ Dev审查通过: {dev_pass}\n"
                f"→ 侧写通过: {profiler_pass}\n"
                f"→ AI通过: {ai_pass}\n"
                f"→ 买入执行: {daily['trades']}\n"
                f"过滤率: {((total_in - daily['trades']) / max(total_in,1) * 100):.0f}%\n"
                f"━━━ 账户 ━━━\n"
                f"💰 余额: <b>{bal:.4f} BNB</b> (~${bal * bnb_usd:,.0f})\n"
                f"BNB价格: ${bnb_usd:.2f}"
            )
            await m.reply(text)

        @r.message(Command("help"))
        async def cmd_help(m: types.Message):
            await m.reply(
                "📋 <b>使用说明</b>\n\n"
                "主要入口：<code>/set</code>\n"
                "你常用的状态、参数、模式、仓位、RPC、点火/暂停，都尽量整合到 /set 里了。\n\n"
                "保留的进阶命令：\n"
                "<code>/boot token</code> 点火\n"
                "<code>/sell_ext 地址</code> 强平外盘\n"
                "<code>/ignore_ext 地址</code> 放生外盘\n"
                "<code>/add_rpc URL [tier]</code> / <code>/del_rpc URL</code>\n"
                "<code>/logs_slots N</code> 热更新 get_logs 并发槽\n\n"
                "如果忘了，直接发 <code>/set</code>。")

    def _match_ext_addr(self, addr_fragment: str) -> str:
        if not self.orch: return None
        addr_lower = addr_fragment.lower()
        for full_addr in self.orch.pancake_mgr.positions:
            if full_addr.lower().startswith(addr_lower) or addr_lower in full_addr.lower():
                return full_addr
        return None

    # ================= Callback 处理 =================

    def _register_callbacks(self):
        r = self.router

        @r.callback_query(F.data == "set_home")
        async def cb_set_home(query: types.CallbackQuery):
            await query.message.edit_text(self._set_home_text(), reply_markup=self._set_home_kb())
            await query.answer()

        @r.callback_query(F.data == "set_menu")
        async def cb_set_menu(query: types.CallbackQuery):
            await query.message.edit_text(self._set_home_text(), reply_markup=self._set_home_kb())
            await query.answer()

        @r.callback_query(F.data == "set_config_menu")
        async def cb_set_config_menu(query: types.CallbackQuery):
            await query.message.edit_text("⚙️ <b>参数设置</b>", reply_markup=self._set_config_menu_kb())
            await query.answer()

        @r.callback_query(F.data == "set_status_menu")
        async def cb_set_status_menu(query: types.CallbackQuery):
            if not self.orch:
                await query.message.edit_text(self._set_home_text(), reply_markup=self._set_home_kb())
            else:
                await query.message.edit_text(self._set_home_text(), reply_markup=self._set_status_menu_kb())
            await query.answer()

        @r.callback_query(F.data == "set_modules_menu")
        async def cb_set_modules_menu(query: types.CallbackQuery):
            await query.message.edit_text("🧩 <b>模块状态</b>", reply_markup=self._set_modules_menu_kb())
            await query.answer()

        @r.callback_query(F.data == "set_actions_menu")
        async def cb_set_actions_menu(query: types.CallbackQuery):
            await query.message.edit_text("🎛️ <b>控制动作</b>", reply_markup=self._set_actions_menu_kb())
            await query.answer()

        @r.callback_query(F.data == "set_positions_menu")
        async def cb_set_positions_menu(query: types.CallbackQuery):
            await query.message.edit_text("💼 <b>仓位与账户</b>", reply_markup=self._set_positions_menu_kb())
            await query.answer()

        @r.callback_query(F.data == "set_rpc_menu")
        async def cb_set_rpc_menu(query: types.CallbackQuery):
            await query.message.edit_text("🌐 <b>RPC 工具</b>", reply_markup=self._set_rpc_menu_kb())
            await query.answer()

        @r.callback_query(F.data.startswith("act_boot_"))
        async def cb_act_boot(query: types.CallbackQuery):
            token = query.data.replace("act_boot_", "", 1)
            if self.orch:
                return await query.answer("系统已经点火", show_alert=True)
            if token != self.boot_token:
                return await query.answer("点火口令已过期，请重新 /set", show_alert=True)
            self.boot_event.set()
            await query.message.edit_text("🔥 <b>已点火，正在启动主引擎…</b>", reply_markup=self._set_home_kb())
            await query.answer("已点火")

        @r.callback_query(F.data == "act_pause")
        async def cb_act_pause(query: types.CallbackQuery):
            if not self.orch:
                return await query.answer("系统尚未启动", show_alert=True)
            self.orch.paused = True
            await query.answer("已暂停")
            await query.message.edit_text("⏸️ <b>已暂停</b>", reply_markup=self._set_actions_menu_kb())

        @r.callback_query(F.data == "act_resume")
        async def cb_act_resume(query: types.CallbackQuery):
            if not self.orch:
                return await query.answer("系统尚未启动", show_alert=True)
            self.orch.paused = False
            await query.answer("已恢复")
            await query.message.edit_text("▶️ <b>已恢复</b>", reply_markup=self._set_actions_menu_kb())

        @r.callback_query(F.data == "act_dry_on")
        async def cb_act_dry_on(query: types.CallbackQuery):
            if not self.orch:
                return await query.answer("系统尚未启动", show_alert=True)
            result = await self.orch.toggle_dry_run(True)
            await query.message.edit_text(result, reply_markup=self._set_actions_menu_kb())
            await query.answer("已切到模拟盘")

        @r.callback_query(F.data == "act_dry_off")
        async def cb_act_dry_off(query: types.CallbackQuery):
            if not self.orch:
                return await query.answer("系统尚未启动", show_alert=True)
            result = await self.orch.toggle_dry_run(False)
            await query.message.edit_text(result, reply_markup=self._set_actions_menu_kb())
            await query.answer("已切到实盘")

        @r.callback_query(F.data == "act_clean_orphans")
        async def cb_act_clean_orphans(query: types.CallbackQuery):
            if not self.orch:
                return await query.answer("系统尚未启动", show_alert=True)
            count = await self.orch.clean_orphan_positions()
            await query.message.edit_text(f"🧹 已清理 <b>{count}</b> 个孤儿仓位", reply_markup=self._set_actions_menu_kb())
            await query.answer("已清理")

        @r.callback_query(F.data == "act_reset_trace")
        async def cb_act_reset_trace(query: types.CallbackQuery):
            from rpc_fleet import get_logs_tracer
            get_logs_tracer.reset()
            await query.message.edit_text("✅ Trace 已重置", reply_markup=self._set_rpc_menu_kb())
            await query.answer("已重置")

        @r.callback_query(F.data.startswith("view_"))
        async def cb_view_panels(query: types.CallbackQuery):
            key = query.data.replace("view_", "", 1)
            if not self.orch and key not in {"rpc", "trace"}:
                return await query.answer("系统尚未启动", show_alert=True)

            if key == "status":
                text, kb = self.orch.get_status_text(), self._set_status_menu_kb()
            elif key == "stats":
                daily = self.orch.risk.get_daily_stats()
                bnb_usd = self.orch._get_bnb_usd()
                bal = await self.orch.exec.get_bnb_balance()
                mode = "🧪 模拟" if self.orch.dry_run else "⚔️ 实盘"
                ds = self.orch.dev_screener
                bp = self.orch.buyer_profiler
                total_in = ds.total_screened if ds else 0
                dev_pass = (ds.total_grade_a + ds.total_grade_b) if ds else 0
                profiler_pass = bp.total_passed if bp else 0
                ai_pass = self.orch.ai.total_passed
                pnl = daily['pnl_bnb']
                pnl_usd = pnl * bnb_usd
                emoji = "📈" if pnl >= 0 else "📉"
                text = (
                    f"📊 <b>今日统计</b> {mode}\n"
                    f"出手: {daily['trades']} | 胜/负/平: {daily['wins']}W/{daily['losses']}L/{daily.get('draws',0)}D\n"
                    f"{emoji} 盈亏: <b>{pnl:+.4f} BNB</b> (~${pnl_usd:+,.2f})\n"
                    f"新币: {total_in} → Dev放行: {dev_pass} → 侧写: {profiler_pass} → AI: {ai_pass} → 买入: {daily['trades']}\n"
                    f"余额: <b>{bal:.4f} BNB</b> (~${bal * bnb_usd:,.0f})"
                )
                kb = self._set_status_menu_kb()
            elif key == "pipeline":
                sections = [f"🏭 <b>风控流水线</b>\n"]
                if hasattr(self.orch, 'dev_screener') and self.orch.dev_screener:
                    ds = self.orch.dev_screener
                    total_dev = ds.total_grade_a + ds.total_grade_b + ds.total_grade_c
                    rate = (ds.total_grade_c / total_dev * 100) if total_dev > 0 else 0
                    sections.append(f"<b>Phase 1 Dev画像</b> {'🟢' if ds.cfg.get('DEV_SCREEN_ENABLED') else '🔴'}\n  A/B/C: {ds.total_grade_a}/{ds.total_grade_b}/{ds.total_grade_c} | C拦截率:{rate:.0f}%")
                if hasattr(self.orch, 'buyer_profiler') and self.orch.buyer_profiler:
                    bp = self.orch.buyer_profiler
                    rate = (bp.total_cabal_blocked / bp.total_profiled * 100) if bp.total_profiled > 0 else 0
                    sections.append(f"<b>Phase 2 买家侧写</b> {'🟢' if bp.cfg.get('PROFILER_ENABLED') else '🔴'}\n  侧写:{bp.total_profiled} Cabal拦截:{bp.total_cabal_blocked}({rate:.0f}%)")
                sections.append(f"<b>Phase 3 AI+动量</b>\n  AI:{self.orch.ai.total_calls}→{self.orch.ai.total_passed}过 | Monitor:{self.orch.monitor.total_confirmed}确认/{self.orch.monitor.total_timeout}超时")
                text, kb = "\n\n".join(sections), self._set_status_menu_kb()
            elif key == "risk":
                text, kb = self.orch.risk.get_status_text(), self._set_status_menu_kb()
            elif key == "balance":
                bal = await self.orch.exec.get_bnb_balance()
                bnb_usd = self.orch._get_bnb_usd()
                mode = "🧪 模拟盘" if self.orch.dry_run else "⚔️ 实盘"
                text = f"💰 <b>账户余额</b>\n模式: {mode}\n余额: <b>{bal:.4f} BNB</b> (~${bal * bnb_usd:,.0f})"
                kb = self._set_positions_menu_kb()
            elif key == "positions":
                text, kb = await self.orch.get_positions_text(), self._set_positions_menu_kb()
            elif key == "ext":
                text, kb = self.orch.pancake_mgr.get_status_text(), self._set_positions_menu_kb()
            elif key == "orphans":
                orphans = await self.orch.detect_orphan_positions()
                if not orphans:
                    text = "📭 无孤儿仓位"
                else:
                    lines = [f"👻 <b>孤儿仓位</b> ({len(orphans)})\n"]
                    for mint, info in list(orphans.items())[:15]:
                        lines.append(f"• <b>{info['symbol']}</b> ({info['age_min']}min) | {info['reason']}\n  <code>{mint[:20]}...</code>")
                    text = "\n".join(lines)
                kb = self._set_positions_menu_kb()
            elif key == "rpc":
                text, kb = (self.orch.fleet.get_stats_text() if self.orch else "⚠️ 系统尚未启动"), self._set_rpc_menu_kb()
            elif key == "trace":
                from rpc_fleet import get_logs_tracer
                text, kb = get_logs_tracer.get_report(), self._set_rpc_menu_kb()
            elif key == "ai":
                text, kb = self.orch.ai.get_status_text(), self._set_modules_menu_kb()
            elif key == "dev":
                text, kb = self.orch.dev_screener.get_status_text(), self._set_modules_menu_kb()
            elif key == "profiler":
                text, kb = self.orch.buyer_profiler.get_status_text(), self._set_modules_menu_kb()
            elif key == "monitor":
                text, kb = self.orch.monitor.get_status_text(), self._set_modules_menu_kb()
            elif key == "radar":
                text, kb = self.orch.detector.get_status_text(), self._set_modules_menu_kb()
            elif key == "price":
                text, kb = self.orch.price_oracle.get_status_text(), self._set_modules_menu_kb()
            elif key == "nonce":
                text, kb = self.orch.exec.nonce_mgr.get_status_text(), self._set_modules_menu_kb()
            elif key == "executor":
                text, kb = self.orch.exec.get_executor_stats(), self._set_modules_menu_kb()
            else:
                return await query.answer("未知面板", show_alert=True)
            await query.message.edit_text(text, reply_markup=kb)
            await query.answer()

        @r.callback_query(F.data == "set_risk")
        async def cb_set_risk(query: types.CallbackQuery):
            rc = self.cfg.risk_cfg
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"日限额: {rc.get('DAILY_TRADE_LIMIT')}笔", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_DAILY_TRADE_LIMIT")],
                [InlineKeyboardButton(text=f"日止损: {rc.get('DAILY_LOSS_LIMIT_BNB')} BNB", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_DAILY_LOSS_LIMIT_BNB")],
                [InlineKeyboardButton(text=f"连亏熔断: {rc.get('MAX_CONSECUTIVE_LOSSES')}次", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_MAX_CONSECUTIVE_LOSSES")],
                [InlineKeyboardButton(text=f"冷却: {rc.get('COOLDOWN_MINUTES')}分", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_COOLDOWN_MINUTES")],
                [InlineKeyboardButton(text="🔙 返回", callback_data="set_menu")]
            ])
            await query.message.edit_text("🛡️ <b>风控设置</b>", reply_markup=keyboard)
            await query.answer()

        @r.callback_query(F.data == "set_trade")
        async def cb_set_trade(query: types.CallbackQuery):
            rc = self.cfg.risk_cfg
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"底仓: {rc.get('BUY_BNB_AMT')} BNB", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_BUY_BNB_AMT")],
                [InlineKeyboardButton(text=f"上限: {rc.get('HARD_CAP_BNB')} BNB", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_HARD_CAP_BNB")],
                [InlineKeyboardButton(text=f"持仓上限: {rc.get('MAX_CONCURRENT_POSITIONS')}", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_MAX_CONCURRENT_POSITIONS")],
                [InlineKeyboardButton(text=f"外盘追踪上限: {rc.get('MAX_EXTERNAL_TRACKING', 10)}", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_MAX_EXTERNAL_TRACKING")],
                [InlineKeyboardButton(text=f"出本: {rc.get('DOUBLE_SELL_MULT')}x", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_DOUBLE_SELL_MULT")],
                [InlineKeyboardButton(text=f"横盘超时: {rc.get('SIDEWAYS_EXIT_SEC', 3600)}s", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_SIDEWAYS_EXIT_SEC")],
                [InlineKeyboardButton(text=f"横盘区间: {rc.get('SIDEWAYS_MIN_MULT', 0.95)}x~{rc.get('SIDEWAYS_MAX_MULT', 1.20)}x", callback_data="noop"),
                 InlineKeyboardButton(text="改下限", callback_data="input_SIDEWAYS_MIN_MULT")],
                [InlineKeyboardButton(text="横盘区间继续", callback_data="noop"),
                 InlineKeyboardButton(text="改上限", callback_data="input_SIDEWAYS_MAX_MULT")],
                [InlineKeyboardButton(text=f"止盈: {rc.get('TRAILING_SL_PCT')}", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_TRAILING_SL_PCT")],
                [InlineKeyboardButton(text=f"止损: {rc.get('HARD_SL_MULT')}x", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_HARD_SL_MULT")],
                [InlineKeyboardButton(text="🔙 返回", callback_data="set_menu")]
            ])
            await query.message.edit_text("📊 <b>交易设置</b>", reply_markup=keyboard)
            await query.answer()

        @r.callback_query(F.data == "set_ai")
        async def cb_set_ai(query: types.CallbackQuery):
            rc = self.cfg.risk_cfg
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"阈值: {rc.get('AI_SCORE_THRESHOLD')}", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_AI_SCORE_THRESHOLD")],
                [InlineKeyboardButton(text=f"高分: {rc.get('AI_MULTIPLIER_HIGH')}x", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_AI_MULTIPLIER_HIGH")],
                [InlineKeyboardButton(text=f"中分: {rc.get('AI_MULTIPLIER_MID')}x", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_AI_MULTIPLIER_MID")],
                [InlineKeyboardButton(text=f"低分: {rc.get('AI_MULTIPLIER_LOW')}x", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_AI_MULTIPLIER_LOW")],
                [InlineKeyboardButton(text="🔙 返回", callback_data="set_menu")]
            ])
            await query.message.edit_text("🤖 <b>AI设置</b>", reply_markup=keyboard)
            await query.answer()

        @r.callback_query(F.data == "set_monitor")
        async def cb_set_monitor(query: types.CallbackQuery):
            rc = self.cfg.risk_cfg
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"观察: {rc.get('MONITOR_DURATION_SEC')}s", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_MONITOR_DURATION_SEC")],
                [InlineKeyboardButton(text=f"动量: {rc.get('MOMENTUM_BNB_THRESHOLD')} BNB", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_MOMENTUM_BNB_THRESHOLD")],
                [InlineKeyboardButton(text=f"tick: {rc.get('UP_TICKS_REQUIRED')}", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_UP_TICKS_REQUIRED")],
                [InlineKeyboardButton(text=f"买家: {rc.get('MIN_UNIQUE_BUYERS')}", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_MIN_UNIQUE_BUYERS")],
                [InlineKeyboardButton(text=f"控盘: {rc.get('MAX_DEV_HOLDING_PCT')}%", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_MAX_DEV_HOLDING_PCT")],
                [InlineKeyboardButton(text=f"卖压占比: {rc.get('SELL_PRESSURE_RATIO_PCT', 40.0)}%", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_SELL_PRESSURE_RATIO_PCT")],
                [InlineKeyboardButton(text=f"Dev净流出: {rc.get('SELL_PRESSURE_DEV_OUTFLOW_PCT', 5.0)}%", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_SELL_PRESSURE_DEV_OUTFLOW_PCT")],
                [InlineKeyboardButton(text=f"大卖次数: {rc.get('SELL_PRESSURE_LARGE_SELL_COUNT', 2)}", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_SELL_PRESSURE_LARGE_SELL_COUNT")],
                [InlineKeyboardButton(text=f"卖压最小量: {rc.get('SELL_PRESSURE_MIN_VOLUME_TOKENS', 0)}", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_SELL_PRESSURE_MIN_VOLUME_TOKENS")],
                [InlineKeyboardButton(text="🔙 返回", callback_data="set_menu")]
            ])
            await query.message.edit_text("⚡ <b>监控设置</b>", reply_markup=keyboard)
            await query.answer()

        @r.callback_query(F.data == "set_dev")
        async def cb_set_dev(query: types.CallbackQuery):
            if not self.orch or not hasattr(self.orch, 'dev_screener') or not self.orch.dev_screener:
                return await query.answer("❌ Dev画像模块未加载")
            cfg = self.orch.dev_screener.cfg
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"开关: {'开' if cfg.get('DEV_SCREEN_ENABLED') else '关'}", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_DEV_SCREEN_ENABLED")],
                [InlineKeyboardButton(text=f"总预算: {cfg.get('DEV_SCREEN_TIMEOUT')}s", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_DEV_SCREEN_TIMEOUT")],
                [InlineKeyboardButton(text=f"快审: {cfg.get('DEV_FAST_TIMEOUT')}s", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_DEV_FAST_TIMEOUT")],
                [InlineKeyboardButton(text=f"块高: {cfg.get('DEV_HEAD_TIMEOUT')}s", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_DEV_HEAD_TIMEOUT")],
                [InlineKeyboardButton(text=f"1h查询: {cfg.get('DEV_QUERY_1H_TIMEOUT')}s", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_DEV_QUERY_1H_TIMEOUT")],
                [InlineKeyboardButton(text=f"6h查询: {cfg.get('DEV_QUERY_6H_TIMEOUT')}s", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_DEV_QUERY_6H_TIMEOUT")],
                [InlineKeyboardButton(text=f"12h查询: {cfg.get('DEV_QUERY_12H_TIMEOUT')}s", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_DEV_QUERY_12H_TIMEOUT")],
                [InlineKeyboardButton(text=f"C手套 nonce≤{cfg.get('DEV_MIN_NONCE')}", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_DEV_MIN_NONCE")],
                [InlineKeyboardButton(text=f"C手套 余额<{cfg.get('DEV_MIN_BALANCE_BNB')}", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_DEV_MIN_BALANCE_BNB")],
                [InlineKeyboardButton(text=f"A级 nonce≥{cfg.get('DEV_A_MIN_NONCE')}", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_DEV_A_MIN_NONCE")],
                [InlineKeyboardButton(text=f"A级 余额≥{cfg.get('DEV_A_MIN_BALANCE_BNB')}", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_DEV_A_MIN_BALANCE_BNB")],
                [InlineKeyboardButton(text=f"A级发币 1/6/12h≤{cfg.get('DEV_A_MAX_DEPLOYS_1H')}/{cfg.get('DEV_A_MAX_DEPLOYS_6H')}/{cfg.get('DEV_A_MAX_DEPLOYS_12H')}", callback_data="noop"),
                 InlineKeyboardButton(text="改1h", callback_data="input_DEV_A_MAX_DEPLOYS_1H")],
                [InlineKeyboardButton(text="A级发币 继续", callback_data="noop"),
                 InlineKeyboardButton(text="改6h", callback_data="input_DEV_A_MAX_DEPLOYS_6H")],
                [InlineKeyboardButton(text="A级发币 继续", callback_data="noop"),
                 InlineKeyboardButton(text="改12h", callback_data="input_DEV_A_MAX_DEPLOYS_12H")],
                [InlineKeyboardButton(text=f"C级踢出 1/6/12h≥{cfg.get('DEV_C_MAX_DEPLOYS_1H')}/{cfg.get('DEV_C_MAX_DEPLOYS_6H')}/{cfg.get('DEV_C_MAX_DEPLOYS_12H')}", callback_data="noop"),
                 InlineKeyboardButton(text="改1h", callback_data="input_DEV_C_MAX_DEPLOYS_1H")],
                [InlineKeyboardButton(text="C级踢出 继续", callback_data="noop"),
                 InlineKeyboardButton(text="改6h", callback_data="input_DEV_C_MAX_DEPLOYS_6H")],
                [InlineKeyboardButton(text="C级踢出 继续", callback_data="noop"),
                 InlineKeyboardButton(text="改12h", callback_data="input_DEV_C_MAX_DEPLOYS_12H")],
                [InlineKeyboardButton(text="🔙 返回参数菜单", callback_data="set_config_menu")]
            ])
            await query.message.edit_text("🔍 <b>Dev画像设置</b>\nA=直买 / B=进动量 / C=踢出拉黑", reply_markup=keyboard)
            await query.answer()

        @r.callback_query(F.data == "set_profiler")
        async def cb_set_profiler(query: types.CallbackQuery):
            if not self.orch or not hasattr(self.orch, 'buyer_profiler') or not self.orch.buyer_profiler:
                return await query.answer("❌ 侧写模块未加载")
            cfg = self.orch.buyer_profiler.cfg
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"开关: {'开' if cfg.get('PROFILER_ENABLED') else '关'}", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_PROFILER_ENABLED")],
                [InlineKeyboardButton(text=f"抽样: {cfg.get('PROFILER_SAMPLE_SIZE')}", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_PROFILER_SAMPLE_SIZE")],
                [InlineKeyboardButton(text=f"危险: {cfg.get('PROFILER_DANGER_THRESHOLD')}分", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_PROFILER_DANGER_THRESHOLD")],
                [InlineKeyboardButton(text=f"Cabal: {cfg.get('PROFILER_CABAL_RATIO')}人", callback_data="noop"),
                 InlineKeyboardButton(text="改", callback_data="input_PROFILER_CABAL_RATIO")],
                [InlineKeyboardButton(text="🔙 返回", callback_data="set_menu")]
            ])
            await query.message.edit_text("🕵️ <b>买家侧写设置</b>", reply_markup=keyboard)
            await query.answer()

        @r.callback_query(F.data == "noop")
        async def cb_noop(query: types.CallbackQuery):
            await query.answer()

        @r.callback_query(F.data.startswith("input_"))
        async def cb_input_value(query: types.CallbackQuery):
            param = query.data.replace("input_", "")
            if param not in PARAM_HELP:
                return await query.answer("❌ 未知参数")
            desc, vtype = PARAM_HELP[param]
            # 从对应模块读实时值
            current = "N/A"
            if param.startswith("DEV_") and self.orch and hasattr(self.orch, 'dev_screener') and self.orch.dev_screener:
                current = self.orch.dev_screener.cfg.get(param, "N/A")
            elif (param.startswith("PROFILER_") or param.startswith("SCORE_")) and self.orch and hasattr(self.orch, 'buyer_profiler') and self.orch.buyer_profiler:
                current = self.orch.buyer_profiler.cfg.get(param, "N/A")
            else:
                current = self.cfg.risk_cfg.get(param, "N/A")

            self.user_states[query.from_user.id] = {"action": "set_value", "param": param}
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ 取消", callback_data="cancel_input")]
            ])
            example = 0.5 if vtype == 'float' else (10 if vtype == 'int' else 'true')
            await query.message.edit_text(
                f"✏️ <b>修改参数</b>\n\n"
                f"参数: <code>{param}</code>\n"
                f"说明: {desc}\n类型: {vtype}\n当前: <b>{current}</b>\n\n"
                f"请发送新值 (如: <code>{example}</code>)",
                reply_markup=keyboard)
            await query.answer("请输入新值")

        @r.callback_query(F.data == "cancel_input")
        async def cb_cancel_input(query: types.CallbackQuery):
            self.user_states.pop(query.from_user.id, None)
            await query.message.delete()
            await query.answer("已取消")

        @r.callback_query(F.data == "close_menu")
        async def cb_close_menu(query: types.CallbackQuery):
            await query.message.delete()
            await query.answer()

        # ===== 参数值输入处理 =====
        # ★ v15: 只匹配非命令文本 + 用户处于输入状态
        @r.message(F.text & ~F.text.startswith("/"))
        async def handle_value_input(m: types.Message):
            user_id = m.from_user.id
            if user_id not in self.user_states:
                return
            state = self.user_states[user_id]
            if state.get("action") != "set_value":
                return
            param = state["param"]
            raw = m.text.strip()
            desc, vtype = PARAM_HELP[param]
            try:
                if vtype == "int": value = int(raw)
                elif vtype == "float": value = float(raw)
                elif vtype == "bool": value = raw.lower() in ("true", "1", "on", "yes")
                else: value = raw
            except ValueError:
                return await m.reply(f"❌ 需要 {vtype} 类型，请重新输入")

            old = self.cfg.risk_cfg.get(param, "N/A")
            self.cfg.risk_cfg[param] = value
            # 路由到对应模块
            if param.startswith("DEV_") and self.orch and hasattr(self.orch, 'dev_screener') and self.orch.dev_screener:
                self.orch.dev_screener.update_config(**{param: value})
            elif (param.startswith("PROFILER_") or param.startswith("SCORE_")) and self.orch and hasattr(self.orch, 'buyer_profiler') and self.orch.buyer_profiler:
                self.orch.buyer_profiler.update_config(**{param: value})
            if self.orch:
                self.orch.sync_config()
            self.cfg.save_risk()
            del self.user_states[user_id]
            try: await m.delete()
            except: pass
            await self.bot.send_message(
                m.chat.id,
                f"✅ <code>{param}</code>\n{desc}\n{old} → <b>{value}</b> 💾")

    # ================= 启动 =================

    async def run_polling(self):
        # ★ v14.2: 增强 polling 韧性，网络抖动时在进程内自愈重连
        self.boot_event.clear()
        self.user_states.clear()
        self.boot_token = secrets.token_hex(4)
        self._boot_cutoff_ts = time.time()
        self._polling_stop = False
        self._send_queue = asyncio.Queue(maxsize=TG_SEND_QUEUE_MAX)
        self._send_task = asyncio.create_task(self._send_worker())

        try:
            # 避免服务重启后吃到旧 /boot 等积压更新
            await self.bot.delete_webhook(drop_pending_updates=True)
        except Exception as e:
            logger.debug(f"[TG] 清理 pending updates 失败: {e}")

        retry_delay = 1
        try:
            while not self._polling_stop:
                try:
                    await self.dp.start_polling(
                        self.bot,
                        allowed_updates=self.dp.resolve_used_update_types(),
                        handle_signals=False,
                    )
                    if self._polling_stop:
                        break
                    logger.warning("[TG] polling 异常退出，将在 2s 后重启 polling")
                    retry_delay = 2
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    if self._polling_stop:
                        break
                    logger.warning(f"[TG] polling 异常: {type(e).__name__}: {e}，{retry_delay}s 后重试")

                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)
        finally:
            if self._send_task:
                self._send_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._send_task
                self._send_task = None

    async def shutdown(self):
        self._polling_stop = True
        self.boot_event.clear()
        self.user_states.clear()
        if self._send_task:
            self._send_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._send_task
            self._send_task = None
        try:
            await self.bot.session.close()
        except Exception:
            pass
