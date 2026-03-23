# BSC Four.meme 自动交易机器人

基于 Python 的 [Four.meme](https://four.meme)（BSC）自动交易机器人，个人项目，仍在持续迭代中。

> 免责声明：本项目仅供学习和研究使用。土狗代币交易风险极高，可能导致本金全部损失。使用本软件的一切风险由用户自行承担，作者不对任何经济损失负责。

---

## 设计理念

1. **低成本、稳定、务实** — 运营成本控制在 $10/月以内，无任何付费数据 API 依赖，全部直连链上 RPC。
2. **全链路自动化** — 「发现 → 评估 → 决策 → 执行」完整闭环，24 小时无人值守运行，不需要手动扫链。
3. **策略优先，不卷基础设施** — 不抢 0 区块，不堆 Gas，通过 Dev 画像、买家侧写、AI 叙事评分等多层筛选来提高胜率，策略逻辑更接近真人操盘节奏。

---

## 功能概览

- **新币监听** — WebSocket 订阅 Four.meme 链上新币创建事件，零轮询，RPC 压力极低
- **Dev 画像分级** — 根据链上历史（nonce、余额、1h/6h/24h 部署频率）对开发者评 A/B/C 级，C 级直接拦截并加黑名单
- **买家侧写** — 对开盘后前 N 个买家做多维评分：一次性钱包、狙击机器人、跨盘串联、同块聚集；检测 Cabal 控盘
- **动量观察** — 买入前并行追踪池子活跃度、独立买家数、连涨 tick、卖压比等多项指标
- **AI 叙事评分** — 调用 Grok API 对代币名称/符号的梗文化潜力评分（0–100），动态调整买入仓位；不配置 API Key 也能正常运行
- **外盘管理** — 持续跟踪内盘打满毕业到 PancakeSwap 的仓位
- **风控系统** — 日亏损限额、日出手上限、连亏冷却、追踪止损、硬止损，参数可运行时热更新
- **Telegram 控制面板** — 运行时全功能控制、参数热更新、状态看板、批量通知
- **Dry-run 模式** — 完整流程模拟，不发真实交易，独立状态数据库，建议先跑模拟再上实盘

---

## 项目结构

```
bsc_main.py          — 主控 / 流水线调度 (v14)
bsc_detector.py      — WS 事件监听 (Four.meme CreateToken topic)
dev_screener.py      — 链上 Dev 画像，A/B/C 分级
bsc_monitor.py       — 动量监控 + 买家追踪
buyer_profiler.py    — 多维买家评分，Cabal 检测
ai_narrative.py      — Grok API 叙事评分，仓位计算
bsc_executor.py      — 链上交易执行，RBF 重发，Nonce 管理
bsc_pancake.py       — PancakeSwap 外盘跟踪
rpc_fleet.py         — 三层 RPC 管理（T0 付费 / T1 免费 / T2 公益兜底）
risk_manager.py      — 风控引擎，黑名单，统计
tg_controller.py     — Telegram 机器人界面
price_oracle.py      — BNB 价格源
```

---

## 设计亮点

- **零付费数据 API** — 全部直连链上 RPC，无订阅费
- **只用 SQLite** — 不依赖 Redis 或外部数据库，单文件持久化，部署极简
- **事件驱动** — WS 订阅新币创建事件，不轮询，一个付费节点 $10/月可用很久
- **三层 RPC 舰队** — T0 付费节点处理关键调用，T1/T2 免费节点承担其余；舰队熔断防止全网拥堵时疯狂重试
- **并行流水线** — v14 重构后总决策耗时从 ~106s 压缩到 ~70s
- **Dry-run 模式** — 完整流程模拟，独立数据库，不影响实盘状态

---

## 运营成本估算

| 项目 | 费用 |
|---|---|
| 服务器 | ~$5/月（1 核 1G，推荐阿里云/腾讯云海外实例）|
| RPC | 一个任意供应商付费节点（WS 监听用）+ 免费公共节点 |
| Grok API | 极低（每个通过风控的新币调用一次，约 150 tokens）|
| 其他 | 无 |

---

## 环境要求

- Python 3.10+
- BSC 钱包私钥
- Telegram Bot Token + Chat ID
- 一个付费 BSC RPC 节点（需支持 WebSocket）
- Grok API Key（xAI）— 可选，不配置时 AI 评分模块自动跳过

---

## 快速开始

```bash
pip install -r requirements.txt

cp bsc_risk_config.example.json bsc_risk_config.json
cp bsc_sys_config.example.json bsc_sys_config.json

# 编辑配置文件，填入私钥、RPC、Telegram Token 等
nano bsc_sys_config.json
nano bsc_risk_config.json

# 先跑模拟模式，确认流水线正常
python bsc_main.py --dry-run

# 确认无误后上实盘
python bsc_main.py
```

---

## 风控参数说明

以下参数均可通过 Telegram 控制面板运行时热更新，无需重启。

| 参数 | 默认值 | 说明 |
|---|---|---|
| BUY_BNB_AMT | 0.05 | 基础买入金额（BNB）|
| HARD_CAP_BNB | 0.08 | 单笔买入上限（BNB）|
| DAILY_LOSS_LIMIT_BNB | -0.5 | 日亏损上限（BNB），触达后停止出手 |
| DAILY_TRADE_LIMIT | 10 | 每日最大出手次数 |
| MAX_CONSECUTIVE_LOSSES | 3 | 连亏熔断触发次数 |
| COOLDOWN_MINUTES | 30 | 熔断冷却时间（分钟）|
| TRAILING_SL_PCT | 0.8 | 追踪止损（峰值 × 系数）|
| HARD_SL_MULT | 0.7 | 硬止损（买入价 × 系数）|
| MOMENTUM_BNB_THRESHOLD | 2.5 | 动量确认池子 BNB 阈值 |
| MIN_UNIQUE_BUYERS | 5 | 最低独立买家数 |
| AI_SCORE_THRESHOLD | 40 | AI 评分低于此值跳过（仅在配置 Grok Key 时生效）|

完整参数见 `bsc_risk_config.example.json`。

---

## 注意事项

- 私钥、API Key 等敏感信息只存在本地配置文件中，请勿上传到公开仓库
- 建议先在 dry-run 模式下观察至少 1–2 天，再切换实盘
- 项目使用相对路径写入日志和数据库，建议固定在同一目录下运行
- 土狗市场风险极高，任何参数配置都不能保证盈利

---

交流反馈：Telegram [@Bscbot1](https://t.me/Bscbot1)
