import requests
import pandas as pd
from app.config import MONDAY_API_KEY, MONDAY_URL

headers = {
    "Authorization": MONDAY_API_KEY,
    "Content-Type": "application/json"
}

def fetch_board_items(board_id):
    query = """
    query ($board_id: ID!) {
      boards(ids: [$board_id]) {
        items_page(limit: 500) {
          items {
            id
            name
            column_values {
              id
              text
              value
            }
          }
        }
      }
    }
    """

    response = requests.post(
        MONDAY_URL,
        json={"query": query, "variables": {"board_id": board_id}},
        headers=headers
    )

    if response.status_code != 200:
        raise Exception(response.text)

    data = response.json()

    items = data["data"]["boards"][0]["items_page"]["items"]

    rows = []

    for item in items:
        row = {"item_name": item["name"]}
        for col in item["column_values"]:
            row[col["id"]] = col["text"]
        rows.append(row)

    return pd.DataFrame(rows)



def convert_to_dataframe(items):
    rows = []

    for item in items:
        row = {"item_name": item["name"]}

        for col in item["column_values"]:
            row[col["id"]] = col["text"]

        rows.append(row)

    return pd.DataFrame(rows)

def fetch_board_columns(board_id):
    query = """
    query ($board_id: ID!) {
      boards(ids: [$board_id]) {
        columns {
          id
          title
          type
        }
      }
    }
    """

    response = requests.post(
        MONDAY_URL,
        json={"query": query, "variables": {"board_id": board_id}},
        headers=headers
    )

    if response.status_code != 200:
        raise Exception(response.text)

    return response.json()
