import pandas as pd

from src_v2.db.postgresql import RAW_CONTENT, psql_engine

header_mark = "xxHEADERxx"

df = pd.read_sql(
    f"""
    SELECT * FROM {RAW_CONTENT} 
    Where line_value != 1 or line_value is NULL
    ;
    """,
    con=psql_engine,
).drop(columns=["line_value", "line_value_natural"])


# caso 1 values ilike %valor%:
# para las filas que tengas ilike %valor%
# solucion para col_desc -> eliminar DESCRIPTION DEL CAMPO
# solucion para col name -> eliminar [NOMBRE, DEL, CAMPO]
# solucion para values -> reemplazar por NULL

df = df.copy()

mask = df["values"].fillna("").str.contains("valor", case=False, regex=False)

# col_desc
df.loc[mask, "col_desc"] = (
    df.loc[mask, "col_desc"]
    .fillna("")
    .str.replace(
        r"DESCRIPCI[ÓO]N\s+DEL\s+CAMPO",
        header_mark,
        case=False,
        regex=True,
    )
    .str.strip()
)

# col_name
df.loc[mask, "col_name"] = (
    df.loc[mask, "col_name"]
    .fillna("")
    .str.replace(
        r"\[?\s*NOMBRE\s+DEL\s+CAMPO\s*\]?",
        header_mark,
        case=False,
        regex=True,
    )
    .str.strip()
)

# values
df.loc[mask, "values"] = header_mark

# caso 2: h2 contenido no relevante
# solucion eliminar las filas h2 que coincidan coneste patron
# [CAMPO, NOMBRE, Nº, PREGUNTA, TABLA, DICCIONARIO, DESCRIPCION, REGISTRO NACIONAL, FORMULARIO]

H2_PATTERNS = [
    r"\bCAMPO\b",
    r"\bNOMBRE\b",
    r"\bN[°º]\b",
    r"\bPREGUNTA\b",
    r"\bTABLA\b",
    r"N°",
    r"Nº",
    r"\bDICCIONARIO\b",
    r"\bDESCRIPCI[ÓO]N\b",
    r"\bREGISTRO\s+NACIONAL\b",
    r"\bFORMULARIO\b",
]


def get_h2_rows_to_remove(df: pd.DataFrame) -> pd.DataFrame:
    """
    Devuelve las filas h2 candidatas a eliminar para revisión.
    """

    pattern = "|".join(H2_PATTERNS)

    mask = df["h2"].fillna("").str.upper().str.contains(pattern, regex=True)

    return df.loc[mask, ["id_row", "h2"]].drop_duplicates()


df_h2_remove = get_h2_rows_to_remove(df)
df = df[~df["id_row"].isin(df_h2_remove["id_row"])]
# df.value_counts("h2").reset_index().sort_values("h2")

id_groups = ["year", "module"]
id_file = id_groups + ["page"]

# fill los h2
df["h2"] = df.sort_values("id_row").groupby(id_groups)["h2"].ffill()

# metadata

df_content = (
    df[df["type_table"] == "raw_1_main"]
    .dropna(axis=1, how="all")
    .reset_index(drop=True)
    .reset_index(names="id_1")
)
df_meta = (
    df[df["type_table"] == "raw_2_metadata"]
    .dropna(axis=1, how="all")
    .reset_index(drop=True)
    .reset_index(names="id_1")
)
df_content.iloc[10:30]
df_meta


# para 2017, los headers no se detectaron bien modificar el rango de tolearancia del tamabio de letra

df.value_counts(["type"])

# para 2004-2006:
#   el colname = v1 significa inicio de cuadro y estan separados por cuadros usar el full line
# - cols npregunta, cuadro, description
# para 2007, 2014, el inicio del cuadro lo marca el vfi, y se puede usar el full line
# - cols npregunta, cuadro, description
# para 2015 - 2017
# el vfi siempre marca el inicio del cuadro
# pereo puede existir varios grupos de descripciion para el mismo cuadro
# cols, npregunta, cuadro, descriptions
# hay casos donde hay cuadro a y b hay quiebre entre el npregunta y cuadros (no siempre el full line)

#  para 2018 (pasar al siguiente muchas inconsitencias)
# cols: cuadro, npregunta, description. -> 1 cuadro puede tenere muhcas descriptions
# el vfi marca el inicio de la description
#  el cuadro parece encontrarse con los full-lines
# a veces el cuaro se repite en todo su ambito pero a vecees no hay una linea vertical por lo que no podira ser deteectado (podria omitirse el uso del cuadro )
# nota para personal entre el anio pasado y el actual, no hay vfi que distinga,

# para 2021 en adelante no hay cuadros solo npreegunta y descripcion (iniciado por bvfi)

df.to_sql("review", psql_engine, index=False, if_exists="replace")
