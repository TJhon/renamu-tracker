import re
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from unidecode import unidecode

from src import config

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────

db_main = config.OUTPUT_ROOT / "clasification" / "main.db"
conn = sqlite3.connect(db_main)

# ─────────────────────────────────────────────
# ETAPA 0: REFERENCIA DE COLUMNAS VÁLIDAS (desde parquets)
# ─────────────────────────────────────────────

df_parquets = pd.read_sql("SELECT DISTINCT * FROM cuadros", con=conn)
df_parquets["b_letter"] = df_parquets["column"].apply(lambda x: str(x)[0])

# Filtrar columnas no relevantes:
#   n → nombres de departamentos, ubigeos
#   _ → columnas _merge
#   o → columnas de orden
#   u, a, f → no relevantes; fvi_p22 es confusión de vfi_p22
drop_bl = ["n", "_", "o", "u", "a", "f"]
df_parquets = df_parquets.query("b_letter not in @drop_bl")
df_parquets = df_parquets.query("column not in ['reg', 'ruc', '']")

# De columnas "d" solo conservar las que empiezan con "descr"
drop_d = df_parquets.query("b_letter == 'd'")["column"]
drop_d = drop_d[~drop_d.str.startswith("descr")].tolist()
df_parquets = df_parquets.query("not (b_letter == 'd' and column in @drop_d)")

# De columnas "i" solo conservar id_original e id_jk
keep_ids = ["id_original", "id_jk"]
df_parquets = df_parquets.query("not (b_letter == 'i' and column not in @keep_ids)")

MAX_COL_LEN = df_parquets["column"].str.len().max()

# Letras iniciales válidas por año (excluye i=id y y=year que son de referencia)
ref_no_cols = ["i", "y"]
inicios_columns = df_parquets.drop_duplicates(["year", "b_letter"])[
    ["year", "b_letter"]
].query("b_letter not in @ref_no_cols")
valid_starts = inicios_columns["b_letter"].tolist()


# ─────────────────────────────────────────────
# ETAPA 1: CARGA DE CSVs DEL PARSER LLM
# ─────────────────────────────────────────────


def file_metadata(full_path: str) -> dict | None:
    """Extrae year, modulo y page del nombre del archivo CSV."""
    name = Path(full_path).name
    match = re.search(r"(\d{4})(?:_(\d+))?_page_(\d+)\.csv", name)
    if match:
        return {
            "year": int(match.group(1)),
            "modulo": match.group(2),  # puede ser None
            "page": int(match.group(3)),
        }
    return None


csv_paths = config.OUTPUT_ROOT / "llm_table_parser" / "df_parser"

dfs = []
for file_csv in csv_paths.rglob("*.csv"):
    try:
        _df = pd.read_csv(file_csv).reset_index(names="row_page")
        _meta = file_metadata(file_csv)
        _df = _df.assign(year=_meta["year"], modulo=_meta["modulo"], page=_meta["page"])
        dfs.append(_df)
    except Exception:
        print(f"Error leyendo: {file_csv}")

df_raw: pd.DataFrame = pd.concat(dfs, ignore_index=True)

col_ids = ["year", "modulo", "page", "row_page"]
df_raw[col_ids] = df_raw[col_ids].apply(
    lambda col: pd.to_numeric(col, errors="coerce").astype("Int64")
)
df_raw = df_raw.sort_values(col_ids).reset_index(names="id")
df_raw[df_raw.select_dtypes(include="object").columns] = df_raw.select_dtypes(
    include="object"
).fillna("")

df_raw.to_sql("pdf_raw_info", con=conn, index=False, if_exists="replace")


# ─────────────────────────────────────────────
# ETAPA 2: DETECCIÓN DE NOMBRES DE COLUMNA
# ─────────────────────────────────────────────
# Supuestos:
#   - La columna no puede tener espacios ni exceder MAX_COL_LEN
#   - Debe empezar por una letra válida según el año (valid_starts)
#   - Se excluyen palabras clave de cuadros/geográficas

df_col = df_raw.drop(
    columns=["row_page", "value_pos", "modulo", "page", "values"]
).copy()
str_cols = df_col.select_dtypes(include="object").columns

EXCLUDE_WORDS = [
    "cuadro",
    "tasas",
    "distrito",
    "canon",
    "cdir",
    "predial",
    "denuncias",
    "tiene",
    "costo",
]

for col in str_cols:
    s = df_col[col].astype(str).str.lower()
    mask = (
        ~s.str.contains(" ", na=False)
        & ~s.str.contains("|".join(EXCLUDE_WORDS), na=False)
        & (s.str.len() <= MAX_COL_LEN)
        & (s.str[0].isin(valid_starts))
    )
    df_col[col] = s.where(mask, "")

# Construir columna "column": parte de "col", se completa o reemplaza
# con desc_col, h2, n_q, cuadro, desc_q si son más informativas
df_col["column"] = df_col["col"].fillna("")
df_col["n"] = np.where(df_col["column"] != "", 1, 0)

for extra_col in ["desc_col", "h2", "n_q", "cuadro", "desc_q"]:
    val = df_col[extra_col].fillna("")
    mask_val = val != ""
    mask_empty = df_col["column"] == ""
    mask_longer = val.str.len() > df_col["column"].str.len()

    df_col["column"] = np.where(mask_empty & mask_val, val, df_col["column"])
    df_col["column"] = np.where(
        ~mask_empty & mask_val & mask_longer, val, df_col["column"]
    )
    df_col["n"] += (
        (mask_empty & mask_val) | (~mask_empty & mask_val & mask_longer)
    ).astype(int)

id_columnas = df_col[["id", "column"]]


# ─────────────────────────────────────────────
# ETAPA 3: LIMPIEZA DE DESCRIPCIONES
# ─────────────────────────────────────────────

CUADRO_RE = re.compile(r"CUADRO_\s*\w+\d(?:\s+[A-Z])?", re.IGNORECASE)


def _extract_cuadro_n(stem: str) -> str:
    """Extrae el número del cuadro del nombre de archivo. Si no hay número → '1'."""
    _NUM_RE = re.compile(r"(\d+)([a-zA-Z]*)")
    stem = stem.replace(r" ", "")
    matches = _NUM_RE.findall(stem)
    if not matches:
        return "1"
    num, suffix = matches[-1]
    return str(int(num)) + suffix


def complete_and_replace(df, col1, col2):
    """
    Completa col1 desde col2 cuando col1 está vacío y col2 supera MAX_COL_LEN.
    Luego elimina col2 cuando es idéntico a col1 (evita duplicados).
    """
    df[col1] = np.where(
        (df[col1] == "") & (df[col2] != "") & (df[col2].str.len() > MAX_COL_LEN),
        df[col2],
        df[col1],
    )
    df[col2] = np.where(
        df[col1].str.lower().str.strip() == df[col2].str.lower().str.strip(),
        "",
        df[col2],
    )


def merge_rows_interrows(df, text_col, col_ref="col"):
    """
    Une filas consecutivas en text_col según dos reglas:
      Regla 1: dos filas seguidas empiezan con minúscula → merge
      Regla 2: primera con mayúscula, segunda con minúscula y misma col_ref → merge
    """
    rows = df.to_dict("records")
    n = len(rows)

    for _ in range(2):  # dos pasadas (regla 1 y regla 2)
        i = 0
        while i < n - 1:
            curr = str(rows[i][text_col]).strip()
            nxt = str(rows[i + 1][text_col]).strip()
            next_col = rows[i + 1][col_ref]
            curr_col = rows[i][col_ref]

            if not curr or not nxt:
                i += 1
                continue

            rule1 = curr[0].islower() and nxt[0].islower() and next_col == ""
            rule2 = (
                curr[0].isupper()
                and nxt[0].islower()
                and (next_col == "" or next_col == curr_col)
            )

            if rule1 or rule2:
                rows[i][text_col] = curr + " " + nxt
                rows[i + 1][text_col] = ""
            i += 1

    return pd.DataFrame(rows)


df_desc = df_raw[["id", "col", "desc_col", "desc_q", "h2", "cuadro"]].merge(
    id_columnas, on="id"
)
df_desc["cuadro1"] = df_desc["cuadro"]
str_fill = df_desc.select_dtypes(include="object").columns
df_desc[str_fill] = df_desc[str_fill].fillna("")

# Limpiar columnas de texto: vaciar si el valor ya está contenido en "column"
# o si no tiene espacios (no es una descripción)
for c in ["col", "desc_col", "desc_q", "h2"]:
    col_lower = df_desc["column"].str.lower()
    val_lower = df_desc[c].str.lower()

    df_desc[c] = np.where(
        (df_desc["column"] != "") & val_lower.str.contains(col_lower, regex=False),
        "",
        df_desc[c],
    )
    df_desc[c] = np.where(~df_desc[c].str.contains(" ", na=False), "", df_desc[c])

    # Identificar si la celda menciona un cuadro
    df_desc["cuadro1"] = np.where(
        df_desc[c].str.lower().str.contains("cuadro"),
        df_desc[c],
        df_desc["cuadro1"],
    )

complete_and_replace(df_desc, "desc_col", "col")
complete_and_replace(df_desc, "desc_col", "desc_q")

# Extraer número de cuadro y encabezado h2 desde cuadro1
df_desc["c1"] = df_desc["cuadro1"].str.extract(
    f"({CUADRO_RE.pattern})", expand=False, flags=re.IGNORECASE
)
df_desc["c2"] = df_desc["cuadro"].str.extract(
    f"({CUADRO_RE.pattern})", expand=False, flags=re.IGNORECASE
)
df_desc["cuadro"] = df_desc["c1"].combine_first(df_desc["c2"])
df_desc["h2"] = df_desc["cuadro1"].str.replace(CUADRO_RE, "", regex=True)
df_desc[["cuadro", "c1", "c2", "h2"]] = df_desc[["cuadro", "c1", "c2", "h2"]].fillna("")

complete_and_replace(df_desc, "desc_q", "h2")

df_desc = df_desc.drop(columns=["col", "h2"])
df_desc["cuadro"] = df_desc["cuadro"].apply(_extract_cuadro_n)

# Unir con metadatos y aplicar merge de filas
df_result = df_raw[
    ["id", "row_page", "year", "modulo", "page", "values", "value_pos"]
].merge(df_desc, on="id")
df_result = df_result.drop(columns=["cuadro1", "c1", "c2"])

for text_col in ["desc_col", "desc_q", "values", "value_pos"]:
    df_result = merge_rows_interrows(df_result, text_col, "column")

df_result.to_sql("pdf_rows", conn, if_exists="replace", index=False)


# ─────────────────────────────────────────────
# ETAPA 4: EXTRACCIÓN Y LIMPIEZA DE TEXTO
# ─────────────────────────────────────────────


def extract_parentheses(text):
    """Extrae contenidos entre paréntesis."""
    matches = re.findall(r"\(([^)]*)\)", str(text))
    return "; ".join(m.strip() for m in matches if m.strip())


def extract_year(text):
    """Extrae años que empiecen por 20, tolerando espacios internos."""
    matches = re.findall(r"\b2\s*0\s*\d\s*\d\b", str(text))
    return "; ".join(re.sub(r"\s+", "", m) for m in matches)


def extract_quoted(text):
    """Extrae contenido entre comillas simples o dobles (incluyendo tipográficas)."""
    matches = re.findall(
        r'["\u201c\u201d\u2018\u2019]([^"\']*)["\u201c\u201d\u2018\u2019]', str(text)
    )
    return "; ".join(m.strip() for m in matches if m.strip())


def clean_text(text):
    """Elimina paréntesis con contenido, años 20xx y comillas con contenido."""
    t = str(text)
    t = re.sub(r"\([^)]*\)", "", t)
    t = re.sub(r"\b2\s*0\s*\d\s*\d\b", "", t)
    t = re.sub(r'["\u201c\u201d\u2018\u2019][^"\']*["\u201c\u201d\u2018\u2019]', "", t)
    t = re.sub(r"^\s*/+\s*", "", t)
    t = re.sub(r"/{2,}", "/", t)
    return t


def clean_alphanumeric(text):
    return re.sub(r"[^\w\s]", "", str(text) if text else "", flags=re.UNICODE)


def split_by_slash(text, cols=5):
    """
    Separa por '/' protegiendo 'S/' (nuevos soles) e 'y/o'.
    Devuelve lista de `cols` elementos (rellena con None si hay menos partes).
    """
    t = re.sub(r"S/\.", "__SOLES__", str(text))
    t = re.sub(r"S/", "__SOLES__", t)
    t = re.sub(r"\by\s*/\s*o\b", "__YO__", t, flags=re.IGNORECASE)

    parts = [
        p.replace("__SOLES__", "S/").replace("__YO__", "y/o").strip()
        for p in t.split("/")
    ]
    parts = [unidecode(clean_alphanumeric(p)).lower() for p in parts]
    parts += [None] * (cols - len(parts))
    return parts[:cols]


def split_last_comma_if_ends_number(text):
    """Separa por la última coma si el texto termina en número (ej: ref bibliográfica)."""
    if not text:
        return text, ""
    parts = text.split(",")
    if len(parts) < 2:
        return text, ""
    if re.search(r"\d\s*$", parts[-1]):
        return ",".join(parts[:-1]), parts[-1]
    return text, ""


df_pdf = pd.read_sql("SELECT * FROM pdf_rows", con=conn)

# Extracciones sobre desc_col
df_pdf["dc_acronimos"] = df_pdf["desc_col"].apply(extract_parentheses)
df_pdf["dc_anios"] = df_pdf["desc_col"].apply(extract_year)
df_pdf["dc_comillas"] = df_pdf["desc_col"].apply(extract_quoted)
df_pdf["dc_limpia"] = df_pdf["desc_col"].apply(clean_text)

# Separar dc_limpia en 5 partes por '/'
split_cols = ["c1", "c2", "c3", "c4", "c5"]
df_pdf[split_cols] = pd.DataFrame(
    df_pdf["dc_limpia"].apply(lambda x: split_by_slash(x, 5)).tolist(),
    index=df_pdf.index,
)
for c in split_cols:
    df_pdf[c] = (
        df_pdf[c].str.strip().apply(lambda x: unidecode(x) if isinstance(x, str) else x)
    )

# Extracciones sobre desc_q
df_pdf["dq_acronimos"] = df_pdf["desc_q"].apply(extract_parentheses)
df_pdf["dq_anios"] = df_pdf["desc_q"].apply(extract_year)
df_pdf["dq_comillas"] = df_pdf["desc_q"].apply(extract_quoted)
df_pdf[["dq_limpia", "dq_ref"]] = df_pdf["desc_q"].apply(
    lambda x: pd.Series(split_last_comma_if_ends_number(x))
)
df_pdf["dq_limpia"] = df_pdf["dq_limpia"].apply(clean_text)

df_pdf = df_pdf.query('c1 != "municipalidad informante"')
cols = ["dq_limpia", "c1", "c2", "c3", "c4", "c5"]

cols = ["c1", "c2", "c3", "c4", "c5"]

df_pdf["description"] = (
    df_pdf["dq_limpia"].fillna("").astype(str)
    + " | "
    + df_pdf[cols].apply(
        lambda row: "; ".join(
            str(x) for x in row if pd.notnull(x) and str(x).strip() != ""
        ),
        axis=1,
    )
).str.strip()

df_pdf.to_sql("clean_dict", con=conn, if_exists="replace")


conn.close()
