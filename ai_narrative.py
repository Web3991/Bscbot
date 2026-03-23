"""
ai_narrative.py — Grok AI 叙事引擎 + 动态仓位计算
"""

import asyncio
import json
import logging
import os
import re
import time

logger = logging.getLogger("AI_Narrative")

# ================= 默认配置 =================
DEFAULT_MODEL = "grok-4-1-fast-non-reasoning"
DEFAULT_API_URL = "https://api.x.ai/v1/chat/completions"
DEFAULT_BASE_BUY = 0.05
DEFAULT_HARD_CAP = 0.08

DEFAULT_MULTIPLIER_HIGH = 1.5
DEFAULT_MULTIPLIER_MID = 1.0
DEFAULT_MULTIPLIER_LOW = 0.5
DEFAULT_AI_SCORE_THRESHOLD = 40

# v7: 极简提示词 — 禁止X搜索 + few-shot + BSC专属
GROK_SYSTEM_PROMPT = """Score this BSC meme token 0-100 by name/symbol ONLY.
DO NOT search X/Twitter. DO NOT penalize zero tweets. Judge the name's inherent narrative power.

SCORING:
- CZ/何一/Binance/BNB/Four.meme/PancakeSwap references → 70-95
- Elon/Trump/major crypto events/trending topics → 50-75
- Chinese memes (谐音梗/抖音热梗), gaming refs (帕鲁/原神), viral culture → 45-65
- Creative funny names, animal memes with twist → 35-55
- Random meaningless names → 10-30
- Scam clones (SafeXX, V2, "1000x", "rugproof") → 0-15

Examples:
"CZ的猫" ($CZCAT) → {{"score":85,"type":"BINANCE_ECOSYSTEM","reason":"Direct CZ reference, peak BSC viral"}}
"龙虾帕鲁" ($LP) → {{"score":52,"type":"MEME_CULTURE","reason":"Palworld gaming meme plus animal combo"}}
"SafeMoonV3" ($SMV3) → {{"score":8,"type":"SCAM_PATTERN","reason":"Obvious copycat scam name"}}

Token: {name} (${symbol})
Respond in Chinese (中文).
JSON only, no markdown:
{{"score":<int>,"type":"<BINANCE_ECOSYSTEM|CRYPTO_NARRATIVE|MEME_CULTURE|GENERIC|SCAM_PATTERN>","reason":"<max 20 words>"}}"""


class ProxyRotator:
    """
    代理池轮换器
    - 多代理自动切换
    - 失败标记冷却 (60s)
    - 全部失败时尝试直连
    - 打印脱敏代理信息
    """

    def __init__(self, proxies_str: str = ""):
        self.proxies = []
        self.proxy_stats = {}
        self._parse_proxies(proxies_str)

    def _parse_proxies(self, proxies_str: str):
        if not proxies_str:
            return
        for p in proxies_str.split("|"):
            p = p.strip()
            if p:
                self.proxies.append(p)
                self.proxy_stats[p] = {"fail": 0, "cooldown_until": 0, "success": 0}

    def _mask_proxy(self, proxy: str) -> str:
        """脱敏显示代理地址"""
        if not proxy:
            return "直连"
        if '@' in proxy:
            return proxy.split('@')[-1][:20]
        return proxy.split('//')[-1][:20] if '//' in proxy else proxy[:20]

    def add_proxy(self, proxy: str):
        proxy = proxy.strip()
        if proxy and proxy not in self.proxy_stats:
            self.proxies.append(proxy)
            self.proxy_stats[proxy] = {"fail": 0, "cooldown_until": 0, "success": 0}

    def get_best_proxy(self) -> str:
        if not self.proxies:
            return None
        now = time.time()
        available = [
            p for p in self.proxies
            if self.proxy_stats[p]["cooldown_until"] < now
        ]
        if not available:
            return None
        available.sort(
            key=lambda p: self.proxy_stats[p]["success"] - self.proxy_stats[p]["fail"],
            reverse=True
        )
        return available[0]

    def mark_success(self, proxy: str):
        if proxy and proxy in self.proxy_stats:
            self.proxy_stats[proxy]["success"] += 1
            self.proxy_stats[proxy]["fail"] = max(0, self.proxy_stats[proxy]["fail"] - 1)

    def mark_fail(self, proxy: str):
        if proxy and proxy in self.proxy_stats:
            stats = self.proxy_stats[proxy]
            stats["fail"] += 1
            if stats["fail"] >= 3:
                stats["cooldown_until"] = time.time() + 60
                stats["fail"] = 0
                logger.warning(f"🌐 [Proxy] {self._mask_proxy(proxy)} 连续失败，冷却60s")

    def get_status(self) -> str:
        if not self.proxies:
            return "直连 (无代理)"
        now = time.time()
        active = sum(1 for p in self.proxies if self.proxy_stats[p]["cooldown_until"] < now)
        return f"{active}/{len(self.proxies)} 可用"


class AINarrativeEngine:
    """
    Grok AI 叙事引擎 v7
    - analyze_token: 评估代币叙事强度
    - 熔断保护 / 动态仓位 / TG 热更新
    - v7: 极简提示词 + 禁止X搜索 + few-shot
    """

    def __init__(self, api_key: str = None, base_buy: float = None, hard_cap: float = None):
        self.api_key = api_key or os.getenv("GROK_API_KEY", "")
        self.api_url = DEFAULT_API_URL
        self.model = DEFAULT_MODEL
        self.base_buy = base_buy or DEFAULT_BASE_BUY
        self.hard_cap = hard_cap or DEFAULT_HARD_CAP
        self.fail_count = 0
        self.silent_until = 0

        proxy_str = os.getenv("GROK_PROXIES", "").strip() or os.getenv("GROK_PROXY", "").strip()
        self.proxy_rotator = ProxyRotator(proxy_str)
        self.proxy = self.proxy_rotator.get_best_proxy()

        self.multiplier_high = DEFAULT_MULTIPLIER_HIGH
        self.multiplier_mid = DEFAULT_MULTIPLIER_MID
        self.multiplier_low = DEFAULT_MULTIPLIER_LOW
        self.score_threshold = DEFAULT_AI_SCORE_THRESHOLD

        self.total_calls = 0
        self.total_success = 0
        self.total_fallback = 0
        self.total_rejected = 0
        self.total_passed = 0

        # v7: 初始超时降至6s (提示词更短，响应更快)
        self.api_timeout = 6.0
        self.timeout_min = 4.0
        self.timeout_max = 12.0

        if self.api_key:
            logger.info(f"🧠 [AI] Grok 叙事引擎就绪，模型: {self.model}")
            logger.info(f"🧠 [AI] 底仓: {self.base_buy} BNB | 上限: {self.hard_cap} BNB")
            logger.info(f"🧠 [AI] 乘数: HIGH={self.multiplier_high} MID={self.multiplier_mid} LOW={self.multiplier_low}")
            logger.info(f"🧠 [AI] 拦截阈值: {self.score_threshold} 分")
            logger.info(f"🧠 [AI] 代理池: {self.proxy_rotator.get_status()}")
            logger.info(f"🧠 [AI] 初始超时: {self.api_timeout}s (自适应: {self.timeout_min}-{self.timeout_max}s)")
        else:
            logger.warning("⚠️ [AI] 未配置 GROK_API_KEY，将使用默认评分")

    async def analyze_token(self, name: str, symbol: str, session=None) -> tuple:
        now = time.time()
        self.total_calls += 1
        logger.info(f"🧠 [AI] ═══ 开始分析 #{self.total_calls}: {name} ({symbol}) ═══")

        if not self.api_key:
            self.total_fallback += 1
            return self.base_buy, 60.0, "NO_API_KEY", "未配置 API Key，使用默认评分"

        if now < self.silent_until:
            remain = int((self.silent_until - now))
            self.total_fallback += 1
            return self.base_buy, 60.0, "AI_SILENT", f"AI 熔断保护中，剩余 {remain}s"

        clean_name = re.sub(r"[^\w\s\-]", "", name)[:30].strip()
        clean_sym = re.sub(r"[^\w\s\-]", "", symbol)[:15].strip()

        prompt = GROK_SYSTEM_PROMPT.format(name=clean_name, symbol=clean_sym)

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.model,
            "messages": [{"role": "system", "content": prompt}],
            "temperature": 0.7,
            "stream": False,
        }

        max_proxy_attempts = 3
        last_error = None

        for proxy_attempt in range(max_proxy_attempts):
            current_proxy = self.proxy_rotator.get_best_proxy() if proxy_attempt < max_proxy_attempts - 1 else None
            proxy_tag = self.proxy_rotator._mask_proxy(current_proxy) if hasattr(self.proxy_rotator, '_mask_proxy') else (current_proxy or '直连')[:20]
            current_timeout = self.api_timeout
            logger.info(f"🧠 [AI] API请求 → {self.model} (超时:{current_timeout:.0f}s | 代理:{proxy_tag}) [{proxy_attempt+1}/{max_proxy_attempts}]")
            api_start = time.time()

            try:
                if session:
                    import aiohttp
                    async with session.post(
                        self.api_url, json=payload, headers=headers,
                        timeout=aiohttp.ClientTimeout(total=current_timeout),
                        proxy=current_proxy,
                    ) as resp:
                        api_latency = time.time() - api_start
                        logger.info(f"🧠 [AI] API 响应: HTTP {resp.status} | 延迟: {api_latency:.2f}s")
                        if resp.status != 200:
                            raise Exception(f"API_STATUS_{resp.status}")
                        data = await resp.json()
                else:
                    import httpx
                    async with httpx.AsyncClient(
                        timeout=current_timeout,
                        proxy=current_proxy,
                    ) as client:
                        resp = await client.post(self.api_url, json=payload, headers=headers)
                        api_latency = time.time() - api_start
                        logger.info(f"🧠 [AI] API 响应: HTTP {resp.status_code} | 延迟: {api_latency:.2f}s")
                        if resp.status_code != 200:
                            raise Exception(f"API_STATUS_{resp.status_code}")
                        data = resp.json()

                # 成功: 标记代理+缩短超时
                if current_proxy:
                    self.proxy_rotator.mark_success(current_proxy)
                self.api_timeout = max(self.timeout_min, self.api_timeout * 0.9)

                content = data['choices'][0]['message']['content'].strip()
                logger.info(f"🧠 [AI] 原始响应: {content[:200]}")

                if "```json" in content:
                    content = content.split("```json")[1].split("```")[0].strip()
                elif "```" in content:
                    content = content.split("```")[1].split("```")[0].strip()

                res = json.loads(content)
                score = float(res.get("score", 60))
                narrative_type = res.get("type", "Unknown")
                reason = res.get("reason", "No reason provided")

                self.fail_count = 0
                self.total_success += 1

                final_buy = self._calculate_position(score)

                if score < self.score_threshold:
                    self.total_rejected += 1
                else:
                    self.total_passed += 1

                logger.info(f"🧠 [AI] ┌─ 解析结果:")
                logger.info(f"🧠 [AI] │ 评分: {score:.0f}/100")
                logger.info(f"🧠 [AI] │ 类型: {narrative_type}")
                logger.info(f"🧠 [AI] │ 理由: {reason[:100]}")
                logger.info(f"🧠 [AI] │ 仓位: {final_buy} BNB | {'✅通过' if score >= self.score_threshold else '❌拦截'}")
                logger.info(f"🧠 [AI] └─ 拦截率: {self.get_reject_rate_text()}")

                return final_buy, score, narrative_type, reason

            except Exception as e:
                last_error = e
                api_latency = time.time() - api_start
                if current_proxy:
                    self.proxy_rotator.mark_fail(current_proxy)
                # 超时自适应延长
                if "timeout" in str(e).lower() or api_latency >= current_timeout * 0.9:
                    self.api_timeout = min(self.timeout_max, self.api_timeout * 1.3)
                    logger.info(f"🧠 [AI] 超时自适应: 调整为 {self.api_timeout:.0f}s")
                logger.warning(
                    f"⚠️ [AI] 尝试 {proxy_attempt+1}/{max_proxy_attempts} 失败: {e} | "
                    f"耗时: {api_latency:.2f}s | 代理: {proxy_tag}"
                )
                if proxy_attempt < max_proxy_attempts - 1:
                    await asyncio.sleep(0.5)
                continue

        self.fail_count += 1
        self.total_fallback += 1
        logger.warning(f"⚠️ [AI] 全部尝试失败 ({self.fail_count}/3): {last_error}")

        if self.fail_count >= 3:
            self.silent_until = now + 300
            logger.error("🛑 [AI] 3 连跪触发熔断，静默 5 分钟!")

        return self.base_buy, 60.0, "FALLBACK_TIMEOUT", f"API 异常: {str(last_error)[:80]}"

    def _calculate_position(self, score: float) -> float:
        if score >= 80:
            multiplier = self.multiplier_high
            tier = "HIGH"
        elif score >= 60:
            multiplier = self.multiplier_mid
            tier = "MID"
        else:
            multiplier = self.multiplier_low
            tier = "LOW"

        calc_buy = self.base_buy * multiplier
        final_buy = min(calc_buy, self.hard_cap)
        logger.info(
            f"🧠 [AI] 仓位: {self.base_buy} × {multiplier} ({tier}) = {calc_buy:.4f} → "
            f"min({calc_buy:.4f}, {self.hard_cap}) = {final_buy:.4f} BNB"
        )
        return round(final_buy, 4)

    def get_reject_rate_text(self) -> str:
        total_decided = self.total_rejected + self.total_passed
        if total_decided == 0:
            return "N/A (无数据)"
        rate = self.total_rejected / total_decided * 100
        return f"{rate:.0f}% ({self.total_rejected}拦/{self.total_passed}过)"

    def update_config(self, **kwargs):
        updated = []
        if 'base_buy' in kwargs and kwargs['base_buy'] is not None:
            self.base_buy = float(kwargs['base_buy'])
            updated.append(f"底仓={self.base_buy}")
        if 'hard_cap' in kwargs and kwargs['hard_cap'] is not None:
            self.hard_cap = float(kwargs['hard_cap'])
            updated.append(f"上限={self.hard_cap}")
        if 'api_key' in kwargs and kwargs['api_key'] is not None:
            self.api_key = kwargs['api_key']
            updated.append("API_KEY=已更新")
        if 'multiplier_high' in kwargs and kwargs['multiplier_high'] is not None:
            self.multiplier_high = float(kwargs['multiplier_high'])
            updated.append(f"乘数HIGH={self.multiplier_high}")
        if 'multiplier_mid' in kwargs and kwargs['multiplier_mid'] is not None:
            self.multiplier_mid = float(kwargs['multiplier_mid'])
            updated.append(f"乘数MID={self.multiplier_mid}")
        if 'multiplier_low' in kwargs and kwargs['multiplier_low'] is not None:
            self.multiplier_low = float(kwargs['multiplier_low'])
            updated.append(f"乘数LOW={self.multiplier_low}")
        if 'score_threshold' in kwargs and kwargs['score_threshold'] is not None:
            self.score_threshold = float(kwargs['score_threshold'])
            updated.append(f"拦截阈值={self.score_threshold}")
        if 'proxy' in kwargs and kwargs['proxy'] is not None:
            proxy_str = kwargs['proxy'].strip()
            self.proxy_rotator = ProxyRotator(proxy_str)
            updated.append(f"代理池={'已更新 ' + self.proxy_rotator.get_status() if proxy_str else '已清除'}")

        if updated:
            logger.info(f"⚙️ [AI] 配置热更新: {', '.join(updated)}")
        return updated

    def get_status_text(self) -> str:
        now = time.time()
        if now < self.silent_until:
            remain = int((self.silent_until - now) / 60)
            status = f"🔴 熔断中 (剩余 {remain} 分钟)"
        else:
            status = "🟢 正常"

        success_rate = (self.total_success / self.total_calls * 100) if self.total_calls > 0 else 0

        return (
            f"🧠 <b>AI 叙事引擎状态</b>\n"
            f"模型: {self.model}\n"
            f"状态: {status}\n"
            f"代理池: {self.proxy_rotator.get_status()}\n"
            f"超时: {self.api_timeout:.0f}s (自适应)\n"
            f"连跪: {self.fail_count}/3\n"
            f"统计: {self.total_success}成功/{self.total_fallback}降级/{self.total_calls}总计 ({success_rate:.0f}%)\n"
            f"拦截率: {self.get_reject_rate_text()}\n"
            f"━━━ 仓位参数 ━━━\n"
            f"底仓: {self.base_buy} BNB | 上限: {self.hard_cap} BNB\n"
            f"乘数: HIGH={self.multiplier_high} MID={self.multiplier_mid} LOW={self.multiplier_low}\n"
            f"拦截阈值: {self.score_threshold} 分"
        )