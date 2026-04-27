import sqlite3

import pandas as pd

from src import config

db_main = config.OUTPUT_ROOT / "clasification" / "main.db"
conn = sqlite3.connect(db_main)
COL_DESC = "description"

df = pd.read_sql(
    """
               select id, year, description from clean_dict
               where column not in ('', 'vfi')
               """,
    con=conn,
)

df[COL_DESC] = (
    df[COL_DESC]
    .astype(str)
    .str.lower()
    .str.replace(r"\s+", " ", regex=True)
    .str.strip()
)

unique_desc = df[[COL_DESC]].drop_duplicates().reset_index(drop=True)
unique_desc.index.name = "uid_desc"
unique_desc = unique_desc.reset_index()


unique_desc["uid_desc"] = unique_desc["uid_desc"].apply(lambda x: f"D{x:05d}")


df = df.merge(unique_desc, on=COL_DESC, how="left").drop_duplicates(
    ["year", "description", "uid_desc"]
)

df_25 = df.query("year==2025")
df_25["uid_group"] = df_25["uid_desc"]
df_ref = df_25[["uid_desc", "uid_group"]].drop_duplicates()

df_all = df.merge(df_ref, how="outer").fillna("")

df_all.to_sql("unique_groups", con=conn, index=False, if_exists="replace")
