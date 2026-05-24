import sqlite3

import pandas as pd

from src import config

db_main = config.OUTPUT_ROOT / "clasification" / "main.db"
conn = sqlite3.connect(db_main)

df = pd.read_sql(
    'select year, description, uid_group from unique_groups where uid_group != ""',
    con=conn,
)

result = df.drop_duplicates(["uid_group", "year"])
years_sorted = sorted(result["year"].unique(), reverse=True)
years_cols_l = [f"desc_{y}" for y in years_sorted]
years_cols = {y: f"desc_{y}" for y in years_sorted}

result["year_col"] = result["year"].map(years_cols)


pivoted = (
    result.pivot_table(
        index="uid_group", columns="year_col", values="description", aggfunc="first"
    )
    .reset_index()[["uid_group"] + years_cols_l]
    .fillna("")
)

pivoted.to_sql("pivot_description", con=conn, index=False, if_exists="replace")
