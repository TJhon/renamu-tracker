import re
import sqlite3

import pandas as pd
from unidecode import unidecode

from src import config

db_main = config.OUTPUT_ROOT / "clasification" / "main.db"
conn = sqlite3.connect(db_main)
df_pdf = pd.read_sql("SELECT * FROM pdf_rows", con=conn)
df_parquets = pd.read_sql("SELECT DISTINCT * FROM cuadros", con=conn)


def extract_parentheses(text):
    """Extrae contenidos entre paréntesis y los une con '; '"""
    matches = re.findall(r"\(([^)]*)\)", str(text))
    return "; ".join(m.strip() for m in matches if m.strip())


def extract_year(text):
    """Extrae años que empiecen por 20, tolerando espacios internos"""
    # Busca secuencias como 2021, 20 21, 2 021, 201 2, etc.
    matches = re.findall(r"\b2\s*0\s*\d\s*\d\b", str(text))
    return "; ".join(re.sub(r"\s+", "", m) for m in matches)


def extract_quoted(text):
    """Extrae contenido entre comillas simples o dobles"""
    matches = re.findall(
        r'["\u201c\u201d\u2018\u2019]([^"\']*)["\u201c\u201d\u2018\u2019]', str(text)
    )
    return "; ".join(m.strip() for m in matches if m.strip())


def clean_text(text):
    """Elimina paréntesis con contenido, años 20xx y comillas con contenido"""
    t = str(text)
    t = re.sub(r"\([^)]*\)", "", t)  # elimina (...)
    t = re.sub(r"\b2\s*0\s*\d\s*\d\b", "", t)  # elimina años 20xx
    t = re.sub(
        r'["\u201c\u201d\u2018\u2019][^"\']*["\u201c\u201d\u2018\u2019]', "", t
    )  # elimina "...
    t = re.sub(r"^\s*/+\s*", "", t)
    t = re.sub(r"/{2,}", "/", t)
    return t


def clean_alphanumeric(text):
    if text is None:
        return ""
    return re.sub(r"[^\w\s]", "", str(text), flags=re.UNICODE)


def split_by_slash(text, cols=5):
    """Separa por '/' pero protege 'S/' (nuevos soles)"""
    # Reemplaza S/ con un placeholder temporal
    t = re.sub(r"S/\.", "__SOLES__", str(text))
    t = re.sub(r"S/", "__SOLES__", t)
    t = re.sub(r"\by\s*/\s*o\b", "__YO__", t, flags=re.IGNORECASE)

    parts = t.split("/")
    parts = [p.replace("__SOLES__", "S/").strip() for p in parts]
    parts = [p.replace("__YO__", "y/o") for p in parts]
    parts = [clean_alphanumeric(p) for p in parts]
    parts = [unidecode(p).lower() for p in parts]
    # Completa con None si hay menos partes que columnas
    parts += [None] * (cols - len(parts))
    return parts[:cols]


col = "desc_col"  # cambia por el nombre real

# 1. Columna con contenido entre paréntesis
df_pdf["dc_acronimos"] = df_pdf[col].apply(extract_parentheses)

# 2. Columna con el año extraído
df_pdf["dc_anios"] = df_pdf[col].apply(extract_year)

# 3. Columna con contenido entre comillas
df_pdf["dc_comillas"] = df_pdf[col].apply(extract_quoted)

# 4. Columna limpia (sin paréntesis, sin años, sin comillas)
df_pdf["dc_limpia"] = df_pdf[col].apply(clean_text)

# 5. Separar col_limpia en 5 partes por '/'
split_cols = ["c1", "c2", "c3", "c4", "c5"]
df_pdf[split_cols] = pd.DataFrame(
    df_pdf["dc_limpia"].apply(lambda x: split_by_slash(x, 5)).tolist(),
    index=df_pdf.index,
)

# 6. Trim + unidecode en las columnas c1..c5
for c in split_cols:
    df_pdf[c] = (
        df_pdf[c].str.strip().apply(lambda x: unidecode(x) if isinstance(x, str) else x)
    )


def split_last_comma_if_ends_number(text):
    if text == "":
        return text, ""
    parts = text.split(",")
    if len(parts) < 2:
        return text, ""
    last = parts[-1]

    match = re.search(r"\d\s*$", last)
    if match:
        resto = ",".join(parts[:-1])

        return resto, last
    return text, ""


col = "desc_q"  # cambia por el nombre real

# 1. Columna con contenido entre paréntesis
df_pdf["dq_acronimos"] = df_pdf[col].apply(extract_parentheses)

# 2. Columna con el año extraído
df_pdf["dq_anios"] = df_pdf[col].apply(extract_year)

# 3. Columna con contenido entre comillas
df_pdf["dq_comillas"] = df_pdf[col].apply(extract_quoted)

df_pdf[["dq_limpia", "dq_ref"]] = df_pdf["desc_q"].apply(
    lambda x: pd.Series(split_last_comma_if_ends_number(x))
)
# 4. Columna limpia (sin paréntesis, sin años, sin comillas)
df_pdf["dq_limpia"] = df_pdf["dq_limpia"].apply(clean_text)

df_counts = df_pdf[split_cols].melt(value_name="value").dropna()

df_counts = df_counts[df_counts["value"] != ""]

result = df_counts["value"].value_counts().reset_index()
result.columns = ["value", "count"]

df_pdf = df_pdf.query('c1 != "municipalidad informante"')

df_pdf.value_counts(["c2"]).reset_index()


df_pdf.sample(400).to_csv("./output/sample_400.csv", index=False)
df_pdf.columns


df_pdf.to_sql("a1", con=conn, if_exists="replace")

13334
