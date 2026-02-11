import pandas as pd


def pipeline_summary(df: pd.DataFrame):

    total_pipeline = df["deal_value"].sum()
    weighted_pipeline = df["weighted_value"].sum()

    avg_deal_size = df["deal_value"].mean()

    return {
        "total_pipeline": float(total_pipeline),
        "weighted_pipeline": float(weighted_pipeline),
        "average_deal_size": float(avg_deal_size)
    }


def pipeline_by_sector(df: pd.DataFrame):

    grouped = (
        df.groupby("sector")
        .agg(
            total_value=("deal_value", "sum"),
            weighted_value=("weighted_value", "sum"),
            deal_count=("deal_value", "count")
        )
        .reset_index()
    )

    return grouped.to_dict(orient="records")


def win_rate(df: pd.DataFrame):

    if "deal_stage" not in df:
        return {"win_rate": None}

    won = df[df["deal_stage"].str.contains("won", na=False)]
    closed = df[
        df["deal_stage"].str.contains("won|lost", na=False)
    ]

    if len(closed) == 0:
        return {"win_rate": 0}

    win_rate_value = len(won) / len(closed)

    return {"win_rate": round(win_rate_value * 100, 2)}


def stage_distribution(df: pd.DataFrame):

    distribution = (
        df["deal_stage"]
        .value_counts()
        .reset_index()
    )

    distribution.columns = ["stage", "count"]

    return distribution.to_dict(orient="records")

def operational_summary(df: pd.DataFrame):

    total_execution_value = df["project_value"].sum()

    completed_projects = df[
    df["execution_status"].str.contains("completed|closed", na=False)
    ]
    
    active_projects = df[
    ~df["execution_status"].str.contains("completed|closed", na=False)
    ]

    avg_duration = df["project_duration_days"].mean()

    return {
        "total_execution_value": float(total_execution_value),
        "active_projects": int(len(active_projects)),
        "completed_projects": int(len(completed_projects)),
        "average_project_duration_days": float(avg_duration)
    }

def sales_vs_execution(deals_df, work_df):

    pipeline = pipeline_summary(deals_df)
    operations = operational_summary(work_df)

    execution_ratio = (
        pipeline["weighted_pipeline"] / operations["total_execution_value"]
        if operations["total_execution_value"] > 0 else 0
    )

    return {
        "weighted_pipeline": pipeline["weighted_pipeline"],
        "execution_value": operations["total_execution_value"],
        "pipeline_to_execution_ratio": execution_ratio
    }

