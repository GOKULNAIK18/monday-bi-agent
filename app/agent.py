# from openai import OpenAI
# from app.config import OPENAI_API_KEY
# from app.bi_engine import pipeline_summary, pipeline_by_sector
# from app.data_cleaner import clean_deals
# from app.monday_client import fetch_board_items
# from app.config import DEALS_BOARD_ID
# import pandas as pd

# client = OpenAI(api_key=OPENAI_API_KEY)


# def interpret_query(user_query: str):

#     system_prompt = """
#     You are a business intelligence assistant.

#     Extract:
#     - intent (pipeline_summary, pipeline_by_sector)
#     - sector (if mentioned, else null)

#     Return JSON only.
#     """

#     response = client.chat.completions.create(
#         model="gpt-4o-mini",
#         messages=[
#             {"role": "system", "content": system_prompt},
#             {"role": "user", "content": user_query}
#         ],
#         temperature=0
#     )

#     return response.choices[0].message.content


# def run_agent(user_query: str):

#     interpretation = interpret_query(user_query)

#     # Convert JSON string to dict
#     import json
#     parsed = json.loads(interpretation)

#     df = fetch_board_items(DEALS_BOARD_ID)
#     df = clean_deals(df)
#     df = df.where(pd.notnull(df), None)

#     if parsed["intent"] == "pipeline_summary":
#         data = pipeline_summary(df)

#     elif parsed["intent"] == "pipeline_by_sector":
#         data = pipeline_by_sector(df)

#         if parsed.get("sector"):
#             data = [d for d in data if d["sector"] == parsed["sector"]]

#     else:
#         return "I could not understand the request."

#     # Generate executive explanation
#     explanation_prompt = f"""
#     Founder asked: {user_query}

#     Data:
#     {data}

#     Provide a short executive-level insight summary.
#     """

#     response = client.chat.completions.create(
#         model="gpt-4o-mini",
#         messages=[{"role": "user", "content": explanation_prompt}],
#         temperature=0.3
#     )

#     return response.choices[0].message.content

# from app.bi_engine import pipeline_summary, pipeline_by_sector
# from app.data_cleaner import clean_deals
# from app.monday_client import fetch_board_items
# from app.config import DEALS_BOARD_ID
# import pandas as pd


# def run_agent(user_query: str):

#     user_query = user_query.lower()

#     df = fetch_board_items(DEALS_BOARD_ID)
#     df = clean_deals(df)
#     df = df.where(pd.notnull(df), None)

#     # Intent detection (simple rules)
#     if "sector" in user_query:
#         data = pipeline_by_sector(df)

#         for d in data:
#             if d["sector"] and d["sector"] in user_query:
#                 return f"""
# Sector: {d['sector']}

# Total Pipeline: {round(d['total_value'], 2)}
# Weighted Pipeline: {round(d['weighted_value'], 2)}
# Active Deals: {d['deal_count']}

# This sector represents a significant portion of overall pipeline.
# """

#         return "Sector not found."

#     else:
#         data = pipeline_summary(df)

#         return f"""
# Overall Pipeline Overview:

# Total Pipeline Value: {round(data['total_pipeline'], 2)}
# Weighted Pipeline: {round(data['weighted_pipeline'], 2)}
# Average Deal Size: {round(data['average_deal_size'], 2)}

# Pipeline remains strong with diversified exposure across sectors.
# """

from datetime import datetime
import pandas as pd

from app.data_quality import analyze_deals_quality, analyze_work_orders_quality
from app.bi_engine import (
    pipeline_summary,
    pipeline_by_sector,
    operational_summary,
    sales_vs_execution,
)
from app.data_cleaner import clean_deals, clean_work_orders
from app.monday_client import fetch_board_items
from app.config import DEALS_BOARD_ID, WORK_ORDERS_BOARD_ID


# ----------------------------------------------------
# 🔹 Time Filter Utility
# ----------------------------------------------------
def filter_by_time(df, user_query, date_column="created_date"):

    if date_column not in df.columns:
        return df

    df = df.copy()
    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")

    now = datetime.now()

    # This Quarter
    if "this quarter" in user_query or "current quarter" in user_query:
        quarter = (now.month - 1) // 3 + 1
        start_month = 3 * (quarter - 1) + 1
        start_date = datetime(now.year, start_month, 1)
        return df[df[date_column] >= start_date]

    # This Month
    if "this month" in user_query:
        start_date = datetime(now.year, now.month, 1)
        return df[df[date_column] >= start_date]

    # This Year
    if "this year" in user_query:
        start_date = datetime(now.year, 1, 1)
        return df[df[date_column] >= start_date]

    return df


# ----------------------------------------------------
# 🔹 Main Agent
# ----------------------------------------------------
def run_agent(user_query: str):

    user_query = user_query.lower().strip()

    # ------------------------------------------------
    # 1️⃣ Fetch + Clean Data
    # ------------------------------------------------
    try:
        deals_df = fetch_board_items(DEALS_BOARD_ID)
        deals_df = clean_deals(deals_df)
        deals_df = deals_df.where(pd.notnull(deals_df), None)

        work_df = fetch_board_items(WORK_ORDERS_BOARD_ID)
        work_df = clean_work_orders(work_df)
        work_df = work_df.where(pd.notnull(work_df), None)

    except Exception:
        return "⚠ Unable to fetch data from Monday.com. Please try again later."

    # ------------------------------------------------
    # 2️⃣ Apply Time Filters
    # ------------------------------------------------
    deals_df = filter_by_time(deals_df, user_query, "created_date")
    work_df = filter_by_time(work_df, user_query, "created_date")

    # ------------------------------------------------
    # 3️⃣ Data Quality Analysis
    # ------------------------------------------------
    deals_issues = analyze_deals_quality(deals_df)
    work_issues = analyze_work_orders_quality(work_df)

    quality_notes = deals_issues + work_issues

    notes_section = ""
    if quality_notes:
        notes_section = "\n\n⚠ Data Quality Notes:\n" + "\n".join(
            f"- {n}" for n in quality_notes
        )

    # ------------------------------------------------
    # 4️⃣ Intent Routing
    # ------------------------------------------------

    # ---------------------------
    # Sector-specific pipeline
    # ---------------------------
    if "sector" in user_query:

        data = pipeline_by_sector(deals_df)

        for d in data:
            if d["sector"] and d["sector"].lower() in user_query:

                return f"""
📊 Sector Performance: {d['sector'].title()}

Total Pipeline: {round(d['total_value'], 2)}
Weighted Pipeline: {round(d['weighted_value'], 2)}
Active Deals: {d['deal_count']}

This sector contributes meaningfully to overall pipeline strength.
{notes_section}
"""

        return "Sector not found. Please specify a valid sector."

    # ---------------------------
    # Execution / Operations
    # ---------------------------
    if any(word in user_query for word in ["execution", "operations", "delivery"]):

        data = operational_summary(work_df)

        return f"""
⚙ Operational Overview

Total Execution Value: {round(data['total_execution_value'], 2)}
Active Projects: {data['active_projects']}
Completed Projects: {data['completed_projects']}
Average Project Duration (days): {round(data['average_project_duration_days'], 2)}

Execution performance reflects current delivery capacity.
{notes_section}
"""

    # ---------------------------
    # Sales vs Execution
    # ---------------------------
    if any(word in user_query for word in ["capacity", "ratio", "pressure"]):

        data = sales_vs_execution(deals_df, work_df)

        return f"""
📈 Sales vs Execution Overview

Weighted Pipeline: {round(data['weighted_pipeline'], 2)}
Execution Value: {round(data['execution_value'], 2)}
Pipeline-to-Execution Ratio: {round(data['pipeline_to_execution_ratio'], 2)}

A higher ratio may indicate potential future delivery pressure.
{notes_section}
"""

    # ---------------------------
    # Leadership Snapshot
    # ---------------------------
    if any(word in user_query for word in ["leadership", "update", "snapshot", "summary"]):

        pipeline = pipeline_summary(deals_df)
        execution = operational_summary(work_df)
        ratio = sales_vs_execution(deals_df, work_df)

        return f"""
📊 Leadership Snapshot

Pipeline:
- Total: {round(pipeline['total_pipeline'], 2)}
- Weighted: {round(pipeline['weighted_pipeline'], 2)}

Execution:
- Active Projects: {execution['active_projects']}
- Completed Projects: {execution['completed_projects']}

Sales vs Execution Ratio: {round(ratio['pipeline_to_execution_ratio'], 2)}

Overall, the business shows strong pipeline momentum with current execution capacity.
{notes_section}
"""

    # ---------------------------
    # Default: Overall Pipeline
    # ---------------------------
    data = pipeline_summary(deals_df)

    return f"""
📊 Overall Pipeline Overview

Total Pipeline Value: {round(data['total_pipeline'], 2)}
Weighted Pipeline: {round(data['weighted_pipeline'], 2)}
Average Deal Size: {round(data['average_deal_size'], 2)}

Pipeline remains diversified and financially significant.
{notes_section}
"""
