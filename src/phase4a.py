"""
phase4_data.py — Fase 4: Construir tabla final renamu_data.

Itera sobre mapper, lee parquets y hace upsert en renamu_data.
Reanudable: verifica pipeline_log para saltar filas ya procesadas.
"""

from rich import print
from tqdm import tqdm

from src.ai.utils import classify_batch, classify_categoria
from src.utils import (
    get_db_connection,
    get_existing_categories,
    get_unclassified_by_year,
    update_cat_classification,
    update_cat_classification_by,
)

print
conn = get_db_connection()

# //
df = get_unclassified_by_year(conn, 2025, 300)

#### Primera clasificacion
### usando la descripcion de la pregunta o cuadro

descriptions = df["desc_cuadro_pregunta"].dropna().unique().tolist()

if len(descriptions) > 1:
    result_df = classify_batch(descriptions)

    if result_df is not None:
        for _, row in result_df.iterrows():
            print(row)
            count = update_cat_classification(
                conn, row["desc_cuadro_pregunta"], row["categoria"]
            )


####### completando las categorias usando la descripcion del cuadro/pregunta
colname = "desc_cuadro_pregunta"
for y in range(2025, 2003, -1):
    df_y = get_unclassified_by_year(conn, y, 200, col=colname, type="cat")
    if len(df_y) < 1:
        continue
    for _, row in tqdm(df_y.iterrows(), total=len(df_y), desc=str(y)):
        desc = row[colname]
        # se actualiza por cada procesamiento por si hay una nueva categoria
        categorias_existentes = get_existing_categories(conn)
        result = classify_categoria(desc, categorias_existentes)
        if result and "categoria" in result:
            categoria = result["categoria"]
            update_cat_classification_by(conn, categoria, desc, column=colname)


cursor = conn.cursor()

import pandas as pd

tiene_categoria = pd.read_sql(
    "SELECT distinct desc_columna, categoria FROM renamu_variables where categoria != ''",
    con=conn,
)

for _, row in tiene_categoria.iterrows():
    _cat = row["categoria"]
    _desc = row["desc_columna"]
    cursor.execute(
        f"""
        UPDATE renamu_variables
        SET categoria = '{_cat}'
        WHERE desc_columna = '{_desc}' AND categoria == ''
    """
    )
    conn.commit()

print("clasificacion usando la descripcion")

####### completando las categorias usando la descripcion de la columna
colname = "desc_columna"
for y in range(2025, 2003, -1):
    df_y = get_unclassified_by_year(conn, y, 500, col=colname, type="cat")
    if len(df_y) < 1:
        continue
    for _, row in df_y.iterrows():
        desc = row[colname]
        print(desc)
        # se actualiza por cada procesamiento por si hay una nueva categoria
        categorias_existentes = get_existing_categories(conn)
        result = classify_categoria(desc, categorias_existentes)
        if result is not None and "categoria" in result:
            categoria = result["categoria"]
            update_cat_classification_by(conn, categoria, desc, column=colname)
