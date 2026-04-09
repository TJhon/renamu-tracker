import re
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from src import config

db_main = config.OUTPUT_ROOT / "clasification" / "main.db"
with sqlite3.connect(db_main) as c:
    df_parquets = pd.read_sql("SELECT DISTINCT * FROM cuadros", con=c)


df_parquets["b_letter"] = df_parquets["column"].apply(lambda x: str(x)[0])

# drop todo
# referencia a los nombres de los departamentos, numero de ubigeo etc
# df_parquets.query("b_letter == 'n'").value_counts('column')
# merge: _merge
# df_parquets.query("b_letter == '_'").value_counts('column')
# orden
# df_parquets.query("b_letter == 'o'").value_counts('column')

# # fvi_p22: confucion deberia ser vfi_p22: no relevante
# df_parquets.query("b_letter == 'f'").value_counts("column")
# # solo valido lo que tenga "descr" al principio
# df_parquets.query("b_letter == 'd'").value_counts("column")
# # dropear reg y ruc lo demas queda
# df_parquets.query("b_letter == 'r'")#.value_counts("column")
# # ids: solo id_original e id_jk
# df_parquets.query("b_letter == 'i'").value_counts("column")

drop_bl = ["n", "_", "o", "u", "a", "f"]
drop_words = ["reg", "ruc", ""]

df_parquets = df_parquets.query("b_letter not in @drop_bl")
df_parquets = df_parquets.query("column not in @drop_words")

drop_d = df_parquets.query("b_letter == 'd'")["column"]
drop_d = drop_d[~drop_d.str.startswith("descr")].tolist()
df_parquets = df_parquets.query("not (b_letter == 'd' and column in @drop_d)")

# ids: solo id_original e id_jk
keep_ids = ["id_original", "id_jk"]
df_parquets = df_parquets.query("not (b_letter == 'i' and column not in @keep_ids)")

df_parquets["len_col"] = df_parquets["column"].str.len()
MAX_COL_LEN = df_parquets["len_col"].max()

ref_no_cols_parquets = ["i", "y"]  # id year
inicios_columns = df_parquets.drop_duplicates(["year", "b_letter"])[
    ["year", "b_letter"]
].query("b_letter not in @ref_no_cols_parquets")


def get_b_l(year):
    l1 = inicios_columns.query("year == @year")["b_letter"]
    return list(l1)


################
# generar la columna de para unirlo con los cuadros
###############


def file_metadata(full_path: str) -> dict | None:
    name = Path(full_path).name

    match = re.search(r"(\d{4})(?:_(\d+))?_page_(\d+)\.csv", name)

    if match:
        year = int(match.group(1))
        modulo = match.group(2)  # puede ser None
        page = int(match.group(3))

        return {"year": year, "modulo": modulo, "page": page}

    return None


csv_paths = config.OUTPUT_ROOT / "llm_table_parser" / "df_parser"

rcsv = list(csv_paths.rglob("*.csv"))

dfs = []
for file_csv in rcsv:
    try:
        _df = pd.read_csv(file_csv).reset_index(names="row_page")
        _metadata = file_metadata(file_csv)
        _df = _df.assign(
            year=_metadata.get("year"),
            modulo=_metadata.get("modulo"),
            page=_metadata.get("page"),
        )
        dfs.append(_df)
    except Exception:
        print(file_csv)

df_raw: pd.DataFrame = pd.concat(dfs, ignore_index=True)
col_ids = ["year", "modulo", "page", "row_page"]
df_raw[col_ids] = df_raw[col_ids].apply(
    lambda col: pd.to_numeric(col, errors="coerce").astype("Int64")
)
df_raw = df_raw.sort_values(col_ids)
df_raw = df_raw.reset_index(names="id")

str_cols = df_raw.select_dtypes(include="object").columns
df_raw[str_cols] = df_raw[str_cols].fillna("")

with sqlite3.connect(db_main) as c:
    df_raw.to_sql("pdf_raw_info", con=c, index=False, if_exists="replace")

df_1: pd.DataFrame = df_raw.copy()

########## objetivo 1 obteer los nombres de columnas dentro de la base
# supuestos
#   - no importa value_pos, modulo, page, row_page, values
#   - la columna no puede tener espacios
#   - el tamanio maximo es el que se tiene es lo que esta dentro de las columnas dentro de las bases de datos  MAX_COL_LEN
#   - la columna no puede empezar por 1 numero (mejor se toma las b_letter )
#   - por anio se tienen la inicial de la palabra que puede empezar la columna

conn = sqlite3.connect(db_main)

df_col = df_1.copy().drop(columns=["row_page", "value_pos", "modulo", "page", "values"])

str_cols = df_col.select_dtypes(include="object").columns

df_col1 = df_col.copy()

valid_starts = inicios_columns["b_letter"].tolist()

for col in str_cols:
    s = df_col1[col].astype(str).str.lower()

    mask = (
        ~s.str.contains(" ", na=False)  # no contiene espacios
        & ~s.str.contains("cuadro", na=False)  # no contiene "cuadro"
        & ~s.str.contains("tasas", na=False)  # no contiene "cuadro"
        & ~s.str.contains("distrito", na=False)  # no contiene "cuadro"
        & ~s.str.contains("canon", na=False)  # no contiene "cuadro"
        & ~s.str.contains("cdir", na=False)  # no contiene "cuadro"
        & ~s.str.contains("predial", na=False)  # no contiene "cuadro"
        & ~s.str.contains("denuncias", na=False)  # no contiene "cuadro"
        & ~s.str.contains("tiene", na=False)  # no contiene "cuadro"
        & ~s.str.contains("costo", na=False)  # no contiene "cuadro"
        & (s.str.len() <= MAX_COL_LEN)  # longitud válida
        & (s.str[0].isin(valid_starts))  # empieza con letra válida
    )

    df_col1[col] = s.where(mask, "")

df_col1["column"] = df_col1["col"].fillna("")
df_col1["n"] = np.where(df_col1["column"] != "", 1, 0)

cols = ["desc_col", "h2", "n_q", "cuadro", "desc_q"]

for col in cols:
    val = df_col1[col].fillna("")

    mask_val_not_empty = val != ""
    mask_c_empty = df_col1["column"] == ""

    # caso 1: c está vacío → reemplaza
    df_col1["column"] = np.where(
        mask_c_empty & mask_val_not_empty, val, df_col1["column"]
    )

    # caso 2: c NO está vacío → elegir el más largo
    mask_c_not_empty = ~mask_c_empty

    mask_val_longer = val.str.len() > df_col1["column"].str.len()

    df_col1["column"] = np.where(
        mask_c_not_empty & mask_val_not_empty & mask_val_longer, val, df_col1["column"]
    )

    # actualizar contador:
    # suma solo cuando:
    # - c estaba vacío y se llenó
    # - o cuando val reemplazó por ser más largo
    df_col1["n"] += (
        (mask_c_empty & mask_val_not_empty)
        | (mask_c_not_empty & mask_val_not_empty & mask_val_longer)
    ).astype(int)


# 755 missing de column
id_columnas = df_col1[["id", "column"]]
id_columnas.to_sql("00_columns", conn, if_exists="replace")

df_desc = df_1[["id", "col", "desc_col", "desc_q", "h2", "cuadro"]].merge(
    id_columnas, on="id"
)
df_desc["cccc"] = df_desc["cuadro"]

cols_eval = ["col", "desc_col", "desc_q", "h2"]

for c in cols_eval:
    df_desc[c] = df_desc.apply(
        lambda row: (
            ""
            if row["column"] != "" and str(row["column"]).lower() in str(row[c]).lower()
            else row[c]
        ),
        axis=1,
    )

    df_desc[c] = df_desc.apply(
        lambda row: "" if " " not in str(row[c]).lower() else row[c],
        axis=1,
    )

    df_desc["cuadro1"] = df_desc.apply(
        lambda row: str(row[c]) if "cuadro" in str(row[c]).lower() else "",
        axis=1,
    )

# 3.1k
df_desc.query("desc_q == desc_col").query("desc_q != ''")


def complete_and_replace(col1, col2, df=df_desc):
    #  primero completamos los valores si el valor es vacio
    df[col1] = df.apply(
        lambda row: (
            row[col2] if row[col1] == "" and len(row[col2]) > MAX_COL_LEN else row[col1]
        ),
        axis=1,
    )
    # segundo: de donde sacamos la informacion eliminamos el valor para evitar repeticiones
    df[col2] = df.apply(
        lambda row: (
            ""
            if str(row[col1]).lower().strip() == str(row[col2]).lower().strip()
            else row[col2]
        ),
        axis=1,
    )
    pass


complete_and_replace("desc_col", "col")
complete_and_replace("desc_col", "desc_q")


df_desc.query("desc_q == desc_col").query("desc_q != ''")


cuadro_pattern = r"CUADRO_\s*\w+\d(?:\s+[A-Z])?"
df_desc["c1"] = df_desc["cuadro1"].str.extract(
    f"({cuadro_pattern})", expand=False, flags=re.IGNORECASE
)
df_desc["c2"] = df_desc["cuadro"].str.extract(
    f"({cuadro_pattern})", expand=False, flags=re.IGNORECASE
)
df_desc["cuadro"] = df_desc["c1"].combine_first(df_desc["c2"])

df_desc["h2"] = df_desc["cuadro1"].str.replace(cuadro_pattern, "", regex=True)

df_desc[["cuadro", "c1", "c2", "h2"]] = df_desc[["cuadro", "c1", "c2", "h2"]].fillna("")

complete_and_replace("desc_q", "h2")

df_desc = df_desc.drop(columns=["col", "h2"])  # , "cuadro1", "c1", "c2"])


def _extract_cuadro_n(stem: str) -> int:
    """Extrae el número del cuadro del nombre de archivo. Si no hay número → 1."""
    _NUM_RE = re.compile(r"(\d+)([a-zA-Z]*)")
    stem = stem.replace(r" ", "")
    matches = _NUM_RE.findall(stem)  # lista de tuplas (numero, sufijo)
    if not matches:
        return "1"
    num, suffix = matches[-1]
    return str(int(num)) + suffix


df_desc["cuadro"] = df_desc["cuadro"].apply(_extract_cuadro_n)

df_id = df_1[["id", "row_page", "year", "modulo", "page", "values", "value_pos"]]
df_result1 = df_id.merge(df_desc, on="id")

df_result1 = df_result1.drop(columns=["cccc", "cuadro1", "c1", "c2"])

df_result1.to_sql("00_0_description", conn, if_exists="replace", index=False)

for xtable in ["00_0_description", "00_columns"]:
    conn.execute(f"drop table if exists '{xtable}'")
    conn.commit()


def merge_rows_interrows(df, text_col, col_ref="col"):
    #  mejorar cuando para la regla 2 si epieza por mayuscul pero debe omtir preguntas signos de adminracion numeros et
    rows = df.to_dict("records")  # interrows

    n = len(rows)

    # -------------------------
    # WHILE 1 → REGLA 1: si la 2 filas empiezan con minusculas se unen a la primera fila
    # -------------------------
    i = 0
    while i < n - 1:
        curr = str(rows[i][text_col]).strip()
        nxt = str(rows[i + 1][text_col]).strip()

        curr_col = rows[i][col_ref]
        next_col = rows[i + 1][col_ref]

        if curr == "" or nxt == "":
            i += 1
            continue

        if len(curr) >= 2 and curr[0].islower() and nxt[0].islower() and next_col == "":
            rows[i][text_col] = curr + " " + nxt
            rows[i + 1][text_col] = ""

        i += 1

    # -------------------------
    # WHILE 2 → REGLA 2
    # (puede encadenar merges)
    # -------------------------
    i = 0
    while i < n - 1:
        curr = str(rows[i][text_col]).strip()
        nxt = str(rows[i + 1][text_col]).strip()

        curr_col = rows[i][col_ref]
        next_col = rows[i + 1][col_ref]
        if curr == "" or nxt == "":
            i += 1
            continue

        if (
            curr[0].isupper()
            and nxt[0].islower()
            and (next_col == "" or next_col == curr_col)
        ):
            rows[i][text_col] = curr + " " + nxt
            rows[i + 1][text_col] = ""

        i += 1

    return pd.DataFrame(rows)


dfx = merge_rows_interrows(df_result1, "desc_col", "column")
dfx = merge_rows_interrows(dfx, "desc_q", "column")
dfx = merge_rows_interrows(dfx, "values", "column")
dfx = merge_rows_interrows(dfx, "value_pos", "column")
dfx.to_sql("pdf_rows", conn, if_exists="replace", index=False)
# 11163
