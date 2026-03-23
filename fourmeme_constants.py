"""
fourmeme_constants.py — Four.meme 平台常量定义
"""

# ================= Four.meme 平台常量 =================

# 代币总供应量 (所有 Four.meme 代币统一)
FOURMEME_TOTAL_SUPPLY = 1_000_000_000          # 1B tokens (无 decimals)
FOURMEME_TOTAL_SUPPLY_WEI = 10 ** 27           # 1B × 10^18

# LP 预留代币 (毕业时转给 PancakeSwap 建池)
FOURMEME_LP_RESERVE = 200_000_000              # 200M tokens
FOURMEME_LP_RESERVE_WEI = 200_000_000 * 10**18

# 可售代币 (bonding curve 上可交易)
FOURMEME_SALEABLE = 800_000_000                # 800M tokens
FOURMEME_SALEABLE_WEI = 800_000_000 * 10**18

# 毕业阈值 (BNB)
FOURMEME_GRADUATE_BNB = 24.0

# 市值乘数 = total_supply / lp_reserve = 1B / 200M = 5
FOURMEME_MC_MULTIPLIER = 5.0

# Four.meme Router (Proxy) 合约地址
FOURMEME_ROUTER = "0x5c952063c7fc8610FFDB798152D69F0B9550762b"

# 交易费率
FOURMEME_TRADING_FEE = 0.005  # 0.5%
FOURMEME_MIN_FEE_BNB = 0.001  # 最低 0.001 BNB

# 创建费
FOURMEME_CREATE_FEE_BNB = 0.005

# 靓号后缀 (Four.meme 合约地址特征)
FOURMEME_VANITY_SUFFIXES = ["4444", "7777", "ffff"]


# ================= 计算工具函数 =================

def calc_bonding_curve_progress(router_balance: int, total_supply: int = None) -> float:
    """
    计算 bonding curve 进度百分比
    
    Args:
        router_balance: Router 合约中的代币余额 (含 decimals)
        total_supply: 总供应量 (含 decimals), 默认 1B × 10^18
    
    Returns:
        0.0 ~ 100.0 的进度百分比
    
    公式: progress = 100 - (leftTokens × 100 / initialRealTokenReserves)
    其中: leftTokens = router_balance - lp_reserve
          initialRealTokenReserves = saleable (800M)
    """
    if total_supply is None:
        total_supply = FOURMEME_TOTAL_SUPPLY_WEI
    
    lp_reserve = int(total_supply * 0.2)
    saleable = int(total_supply * 0.8)
    
    left_tokens = max(0, router_balance - lp_reserve)
    
    if saleable <= 0:
        return 0.0
    
    progress = (1.0 - float(left_tokens) / float(saleable)) * 100.0
    return round(max(0.0, min(100.0, progress)), 2)


def calc_estimated_bnb_raised(router_balance: int, total_supply: int = None,
                                graduate_bnb: float = FOURMEME_GRADUATE_BNB) -> float:
    """
    估算已募集的 BNB 数量 (线性近似)
    
    Args:
        router_balance: Router 合约中的代币余额 (含 decimals)
        total_supply: 总供应量, 默认 1B × 10^18
        graduate_bnb: 毕业 BNB 阈值, 默认 24.0
    
    Returns:
        估算的 BNB 数量
    """
    progress = calc_bonding_curve_progress(router_balance, total_supply)
    return round(progress / 100.0 * graduate_bnb, 6)


def calc_market_cap_bnb(pool_bnb: float, total_supply: int = None) -> float:
    """
    计算 Four.meme 代币的市值 (BNB 计价)
    
    核心公式: MC = pool_bnb × total_supply / lp_reserve = pool_bnb × 5
    
    原理: 毕业时 200M tokens + 24 BNB 建 PancakeSwap LP
          LP 价格 = 24 / 200M, MC = (24/200M) × 1B = 120 BNB
          比例: MC / raised = 1B / 200M = 5
    
    Args:
        pool_bnb: 估算的池子 BNB 数量
        total_supply: 总供应量 (无 decimals), 默认 1B
    
    Returns:
        市值 (BNB)
    """
    if pool_bnb <= 0:
        return 0.0
    
    if total_supply is None:
        multiplier = FOURMEME_MC_MULTIPLIER
    else:
        lp_reserve = int(total_supply * 0.2)
        multiplier = float(total_supply) / float(max(lp_reserve, 1))
    
    return round(pool_bnb * multiplier, 4)


def calc_market_cap_usd(pool_bnb: float, bnb_price_usd: float,
                          total_supply: int = None) -> float:
    """市值 USD 计价"""
    mc_bnb = calc_market_cap_bnb(pool_bnb, total_supply)
    return round(mc_bnb * bnb_price_usd, 2)


def calc_price_per_token_bnb(pool_bnb: float, total_supply: int = None) -> float:
    """
    计算单个代币价格 (BNB)
    
    price = MC / total_supply = pool_bnb × 5 / 1B = pool_bnb / 200M
    """
    if pool_bnb <= 0:
        return 0.0
    
    ts = total_supply if total_supply else FOURMEME_TOTAL_SUPPLY
    lp_reserve = int(ts * 0.2)
    return pool_bnb / float(max(lp_reserve, 1))


def format_market_cap(mc_usd: float) -> str:
    """格式化市值显示"""
    if mc_usd >= 1_000_000:
        return f"${mc_usd / 1_000_000:.2f}M"
    elif mc_usd >= 1_000:
        return f"${mc_usd / 1_000:.1f}K"
    return f"${mc_usd:.0f}"


# ================= 验证 =================
if __name__ == "__main__":
    # 验证毕业时的市值是否与官方一致
    bnb_price = 600.0  # 假设 BNB 价格
    
    print("═══ Four.meme 常量验证 ═══")
    print(f"Total Supply: {FOURMEME_TOTAL_SUPPLY:,}")
    print(f"LP Reserve:   {FOURMEME_LP_RESERVE:,} ({FOURMEME_LP_RESERVE/FOURMEME_TOTAL_SUPPLY*100:.0f}%)")
    print(f"Saleable:     {FOURMEME_SALEABLE:,} ({FOURMEME_SALEABLE/FOURMEME_TOTAL_SUPPLY*100:.0f}%)")
    print(f"MC Multiplier: {FOURMEME_MC_MULTIPLIER}×")
    print()
    
    # 毕业时 MC
    grad_mc_bnb = calc_market_cap_bnb(FOURMEME_GRADUATE_BNB)
    grad_mc_usd = grad_mc_bnb * bnb_price
    print(f"毕业 ({FOURMEME_GRADUATE_BNB} BNB raised):")
    print(f"  MC = {grad_mc_bnb} BNB = {format_market_cap(grad_mc_usd)}")
    print(f"  价格/token = {calc_price_per_token_bnb(FOURMEME_GRADUATE_BNB):.12f} BNB")
    print(f"  曲线进度 = 100%")
    print()
    
    # 中间点 MC
    for raised_bnb in [1.0, 5.0, 12.0, 20.0]:
        mc = calc_market_cap_bnb(raised_bnb)
        mc_usd = mc * bnb_price
        progress = raised_bnb / FOURMEME_GRADUATE_BNB * 100
        print(f"  {raised_bnb:5.1f} BNB raised → MC={mc:7.2f} BNB ({format_market_cap(mc_usd):>10}) | 曲线 {progress:.1f}%")
