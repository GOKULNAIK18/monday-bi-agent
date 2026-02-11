# import pandas as pd


# def analyze_deals_quality(df: pd.DataFrame):
#     issues = []

#     if df["deal_value"].isna().sum() > 0:
#         issues.append("Some deals have missing deal values.")

#     if df["closure_probability"].isna().sum() > 0:
#         issues.append("Some deals have missing closure probabilities.")

#     if df["sector"].isna().sum() > 0:
#         issues.append("Some deals are missing sector classification.")

#     return issues


# def analyze_work_orders_quality(df: pd.DataFrame):
#     issues = []

#     if df["execution_status"].isna().sum() > 0:
#         issues.append("Some projects have missing execution status.")

#     if df["execution_value"].isna().sum() > 0:
#         issues.append("Some projects have missing execution value.")

#     return issues

import pandas as pd


def analyze_deals_quality(df: pd.DataFrame):
    issues = []

    # Deal Value
    if "deal_value" in df.columns:
        if df["deal_value"].isna().sum() > 0:
            issues.append("Some deals have missing deal values.")
    else:
        issues.append("Deal value column missing from deals board.")

    # Closure Probability
    if "closure_probability" in df.columns:
        if df["closure_probability"].isna().sum() > 0:
            issues.append("Some deals have missing closure probabilities.")
    else:
        issues.append("Closure probability column missing from deals board.")

    # Sector
    if "sector" in df.columns:
        if df["sector"].isna().sum() > 0:
            issues.append("Some deals are missing sector classification.")
    else:
        issues.append("Sector column missing from deals board.")

    return issues


def analyze_work_orders_quality(df: pd.DataFrame):
    issues = []

    # Execution Status
    if "execution_status" in df.columns:
        if df["execution_status"].isna().sum() > 0:
            issues.append("Some projects have missing execution status.")
    else:
        issues.append("Execution status column missing from work orders board.")

    # Execution Value
    if "execution_value" in df.columns:
        if df["execution_value"].isna().sum() > 0:
            issues.append("Some projects have missing execution value.")
    else:
        issues.append("Execution value column missing from work orders board.")

    return issues

