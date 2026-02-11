import pandas as pd


def analyze_deals_quality(df: pd.DataFrame):
    issues = []

    if df["deal_value"].isna().sum() > 0:
        issues.append("Some deals have missing deal values.")

    if df["closure_probability"].isna().sum() > 0:
        issues.append("Some deals have missing closure probabilities.")

    if df["sector"].isna().sum() > 0:
        issues.append("Some deals are missing sector classification.")

    return issues


def analyze_work_orders_quality(df: pd.DataFrame):
    issues = []

    if df["execution_status"].isna().sum() > 0:
        issues.append("Some projects have missing execution status.")

    if df["execution_value"].isna().sum() > 0:
        issues.append("Some projects have missing execution value.")

    return issues
