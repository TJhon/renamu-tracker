"""
Reconstrucción y normalización de texto extraído de cuadros estadísticos.

Objetivo
--------
Corregir textos con problemas de extracción OCR/PDF, tales como:
- ausencia de espacios entre palabras;
- caracteres especiales o errores de codificación;
- fragmentación de texto por saltos de línea;
- inconsistencias en etiquetas, descripciones y valores categóricos.

Columnas objetivo
-----------------
- col_desc : descripción de la variable o columna.
- values   : categorías y códigos asociados a una variable.
- content  : metadatos o agrupaciones de columnas.
- h2       : encabezados de segundo nivel, códigos o grupos temáticos.
"""

import json
from datetime import datetime

import pandas as pd
from rich import print
from sqlalchemy import text
from tqdm import tqdm

from src_v2.db.postgresql import RAW_TEXT_RECONSTRUCTED_TEXT_V2, psql_engine
from src_v2.ia import ask_llm
from src_v2.ia.texto.prompts import GENERIC_PROMPT, VALUES_PROMPT


def now():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return timestamp


df_text = pd.read_sql(
    f"""
    SELECT distinct col, text FROM {RAW_TEXT_RECONSTRUCTED_TEXT_V2} 
    WHERE reconstructed_text IS NULL and values_json IS NULL and LENGTH(text) > 3
    ORDER BY text
    ;
    """,
    con=psql_engine,
)

print(df_text.shape)


for row in tqdm(df_text.to_dict("records")):
    query = row["text"]
    col = row["col"]
    system_prompt = GENERIC_PROMPT if col != "values" else VALUES_PROMPT
    output = "json" if col == "values" else ""
    reconstruct_text = ask_llm.ask_llm_get_response(
        query, system_prompt, output_type=output
    )
    row["r_text"] = reconstruct_text
    time = now()
    # print(row)

    with psql_engine.connect() as conn:
        if output == "json":
            conn.execute(
                text(
                    f"""
                    UPDATE {RAW_TEXT_RECONSTRUCTED_TEXT_V2}
                    SET
                        values_json = CAST(:values_json AS jsonb),
                        processed_at = :time
                    WHERE text = :query
                    AND col = :col
                    """
                ),
                {
                    "values_json": json.dumps(reconstruct_text),
                    "query": query,
                    "time": time,
                    "col": col,
                },
            )
        else:
            conn.execute(
                text(
                    f"""
            UPDATE {RAW_TEXT_RECONSTRUCTED_TEXT_V2}
                SET
                 reconstructed_text = :reconstructed_text,
                    processed_at = :time
                WHERE text = :query AND col = :col
                """
                ),
                {
                    "reconstructed_text": reconstruct_text,
                    "query": query,
                    "time": time,
                    "col": col,
                },
            )
        conn.commit()
