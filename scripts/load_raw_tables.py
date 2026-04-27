import pandas as pd
import pandas_gbq
import os

project_id = "titanbay-494310"
dataset = "titanbay_raw"

tables = [
    "platform_partners",
    "platform_relationship_managers", 
    "platform_entities",
    "platform_investors",
    "platform_fund_closes",
    "freshdesk_tickets"
]
downloads = os.path.expanduser("~/Downloads")

for table in tables:
    filename = os.path.join(downloads, f"{table}.csv")
    print(f"Loading {table}...")
    df = pd.read_csv(filename)
    print(f"  Columns: {list(df.columns)}")
    pandas_gbq.to_gbq(df, f"{dataset}.{table}", project_id=project_id, if_exists="replace")
    print(f"  Done: {len(df)} rows")
