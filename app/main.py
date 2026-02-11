import pandas as pd
from fastapi import FastAPI
from app.monday_client import fetch_board_items
from app.config import DEALS_BOARD_ID
from app.data_cleaner import clean_deals
from app.bi_engine import pipeline_summary, pipeline_by_sector
from app.agent import run_agent
from pydantic import BaseModel
from app.config import WORK_ORDERS_BOARD_ID
from app.monday_client import fetch_board_columns
from app.config import WORK_ORDERS_BOARD_ID
from app.data_cleaner import clean_work_orders
from app.bi_engine import operational_summary
from app.config import WORK_ORDERS_BOARD_ID
from app.bi_engine import sales_vs_execution



class QueryRequest(BaseModel):
    question: str

app = FastAPI()

@app.get("/inspect-work-orders")
def inspect_work_orders():
    df = fetch_board_items(WORK_ORDERS_BOARD_ID)
    return {"columns": list(df.columns)}

@app.post("/ask")
def ask_agent(request: QueryRequest):
    return {"answer": run_agent(request.question)}


@app.get("/")
def root():
    return {"message": "Monday BI Agent Running"}


@app.get("/inspect-deals")
def inspect_deals():
    df = fetch_board_items(DEALS_BOARD_ID)
    df = clean_deals(df)
    df = df.replace({float("nan"): None})
    df = df.where(pd.notnull(df), None)

    return df.head().to_dict(orient="records")


@app.get("/test-deals")
def test_deals():
    df = fetch_board_items(DEALS_BOARD_ID)
    return {"rows": len(df)}


@app.get("/pipeline-summary")
def get_pipeline_summary():
    df = fetch_board_items(DEALS_BOARD_ID)
    df = clean_deals(df)
    df = df.where(pd.notnull(df), None)

    return pipeline_summary(df)


@app.get("/pipeline-by-sector")
def get_pipeline_by_sector():
    df = fetch_board_items(DEALS_BOARD_ID)
    df = clean_deals(df)
    df = df.where(pd.notnull(df), None)

    return pipeline_by_sector(df)


from app.monday_client import fetch_board_columns

@app.get("/work-orders-metadata")
def work_orders_metadata():
    return fetch_board_columns(WORK_ORDERS_BOARD_ID)


@app.get("/operational-summary")
def get_operational_summary():
    df = fetch_board_items(WORK_ORDERS_BOARD_ID)
    df = clean_work_orders(df)
    df = df.where(pd.notnull(df), None)

    return operational_summary(df)

@app.get("/sales-vs-execution")
def get_sales_vs_execution():
    deals_df = clean_deals(fetch_board_items(DEALS_BOARD_ID))
    work_df = clean_work_orders(fetch_board_items(WORK_ORDERS_BOARD_ID))

    return sales_vs_execution(deals_df, work_df)

@app.get("/data-quality-report")
def get_data_quality():
    deals_df = clean_deals(fetch_board_items(DEALS_BOARD_ID))
    work_df = clean_work_orders(fetch_board_items(WORK_ORDERS_BOARD_ID))

    deals_issues = analyze_deals_quality(deals_df)
    work_issues = analyze_work_orders_quality(work_df)

    return {
        "deals_issues": deals_issues,
        "work_orders_issues": work_issues,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000)
