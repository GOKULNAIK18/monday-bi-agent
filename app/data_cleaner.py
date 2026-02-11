import pandas as pd
import numpy as np


def clean_text_column(series):
    return (
        series.fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
    )


def clean_numeric_column(series):
    return pd.to_numeric(series, errors="coerce").fillna(0)


def clean_date_column(series):
    return pd.to_datetime(series, errors="coerce")


def extract_quarter(date_series):
    return date_series.dt.to_period("Q").astype(str)


# -------------------------------
# CLEAN DEALS DATA
# -------------------------------

def clean_deals(df: pd.DataFrame):

    df = df.copy()

    # Rename columns for clarity
    df = df.rename(columns={
        "numeric_mm0f5eer": "deal_value",
        "date_mm0fc15j": "close_date",
        "color_mm0f774r": "deal_stage",
        "color_mm0fj241": "closure_probability",
        "color_mm0fhm4g": "sector",
        "date_mm0ftrg2": "tentative_close_date",
        "date_mm0fznfw": "created_date"
    })

    # Clean text fields
    text_cols = ["deal_stage", "closure_probability", "sector"]
    for col in text_cols:
        if col in df:
            df[col] = clean_text_column(df[col])

    # Clean numeric
    if "deal_value" in df:
        df["deal_value"] = clean_numeric_column(df["deal_value"])

    # Clean dates
    date_cols = ["close_date", "tentative_close_date", "created_date"]
    for col in date_cols:
        if col in df:
            df[col] = clean_date_column(df[col])

    # Extract quarter from close date
    if "close_date" in df:
        df["quarter"] = extract_quarter(df["close_date"])

    # ------------------------------
    # Map Closure Probability to %
    # ------------------------------

    probability_map = {
        "low": 25,
        "medium": 50,
        "high": 75,
        "very high": 90,
        "won": 100,
        "lost": 0
    }

    if "closure_probability" in df:
        df["probability_pct"] = df["closure_probability"].map(probability_map).fillna(50)

    # Weighted pipeline
    if "deal_value" in df and "probability_pct" in df:
        df["weighted_value"] = (
            df["deal_value"] * df["probability_pct"] / 100
        )

    return df


# -------------------------------
# CLEAN WORK ORDERS DATA
# -------------------------------

def clean_work_orders(df: pd.DataFrame):

    df = df.copy()

    df = df.rename(columns={
        "numeric_mm0frrpc": "project_value",
        "date_mm0fbrv4": "start_date",
        "date_mm0fkxej": "end_date",
        "color_mm0ft9td": "execution_status",
        "color_mm0fe6kb": "sector"
    })

    # Clean text
    text_cols = ["execution_status", "sector"]
    for col in text_cols:
        if col in df:
            df[col] = clean_text_column(df[col])

    # Clean numeric
    if "project_value" in df:
        df["project_value"] = clean_numeric_column(df["project_value"])

    # Clean dates
    for col in ["start_date", "end_date"]:
        if col in df:
            df[col] = clean_date_column(df[col])

    # Project duration
    if "start_date" in df and "end_date" in df:
        df["project_duration_days"] = (
            df["end_date"] - df["start_date"]
        ).dt.days

    return df
