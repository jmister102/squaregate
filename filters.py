"""Filter logic for Bloomberg ticker screening."""

import pandas as pd


def apply_filters(df: pd.DataFrame) -> dict:
    """
    Apply all screening filter tests and return a dict of {test_name: bool_Series}.
    True = PASS, False = FAIL.
    """
    filters = {}

    # Filter 1: Float market cap < $75M in last 60 days
    filters['test_float_lt_75M'] = df['FloatValue_last60d'] < 75_000_000

    # Filter 2: ADV > $50k (100-day OR 20-day)
    filters['test_adv_gt_50k'] = (df['AVAT_100d'] > 50_000) | (df['AVAT_20d'] > 50_000)

    # Filter 3: < 24 months cash runway (quarterly or TTM burn)
    df['Burn_Q']   = pd.to_numeric(df['Burn_Q'],   errors='coerce')
    df['Burn_TTM'] = pd.to_numeric(df['Burn_TTM'], errors='coerce')
    filters['test_lt_24m_runway'] = (
        df['Burn_Q'].between(0, 24,   inclusive='neither') |
        df['Burn_TTM'].between(0, 24, inclusive='neither')
    )

    # Filter 4: ADV / daily burn < 35%  (PASS if cash-flow positive OR ratio < 35%)
    daily_burn = df['CFO_Q'] / 63
    filters['test_adv_div_dailyburn_lt_35pct'] = (
        (df['CFO_Q'] >= 0) |                                  # not burning cash
        (df['AVAT_100d'] / daily_burn < 0.35)                 # manageable ratio
    )

    return filters


def create_fail_reason(df: pd.DataFrame, test_cols: list) -> list:
    """Return a list of comma-joined failed test names for each row."""
    failed = ~df[test_cols]
    return [
        ', '.join(col for col in test_cols if failed.loc[idx, col])
        for idx in df.index
    ]
