import os
import sqlite3
from pathlib import Path
from typing import List, Union

import dashscope
import pandas as pd
from dotenv import find_dotenv, load_dotenv

from src.config import OUTPUT_ROOT

# Config
load_dotenv(find_dotenv())
QWEN_API_KEY = os.environ.get("QWEN_API_KEY")
QWEN_MODEL = os.environ.get("QWEN_MODEL", "qwen-max")
dashscope.api_key = QWEN_API_KEY


# Paths
DB_PATH = OUTPUT_ROOT / "clasification" / "main.db"


# ===========================================================================
# UTILS: Base de datos
# ===========================================================================
def get_db_connection(db_path: Union[str, Path] = DB_PATH):
    """Retorna conexión SQLite con row_factory para dicts"""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def update_cat_classification_by(
    conn, categoria: str, description: str, column="desc_columna"
):
    """Actualiza categoría"""
    cursor = conn.cursor()
    cursor.execute(
        f"""
        UPDATE renamu_variables 
        SET categoria = '{categoria}'
        WHERE {column} = '{description}'
    """
    )
    conn.commit()
    # return cursor.rowcount


def update_cat_classification(conn, desc_cuadro_pregunta: str, categoria: str):
    """Actualiza categoría y subcategoría para todas las filas coincidentes"""
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE renamu_variables 
        SET categoria = ?
        WHERE desc_cuadro_pregunta = ?
        AND (categoria IS NULL OR categoria = '')
    """,
        (categoria, desc_cuadro_pregunta),
    )
    conn.commit()
    return cursor.rowcount


def update_classification(
    conn, desc_cuadro_pregunta: str, categoria: str, subcategoria: str
):
    """Actualiza categoría y subcategoría para todas las filas coincidentes"""
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE renamu_variables 
        SET categoria = ?, subcategoria = ?
        WHERE desc_cuadro_pregunta = ?
        AND (categoria IS NULL OR categoria = '' OR subcategoria IS NULL OR subcategoria = '')
    """,
        (categoria, subcategoria, desc_cuadro_pregunta),
    )
    conn.commit()
    return cursor.rowcount


def get_unclassified_by_year(
    conn,
    year: int,
    batch_size: int = 50,
    col="desc_cuadro_pregunta",
    # cat si categoria es nulo, subcat si subcategoria es nulo, both si es ambos
    type="cat",
) -> pd.DataFrame:
    """Obtiene descripciones no clasificadas para un año específico"""

    missing_cat = "categoria    IS NULL OR TRIM(categoria)    = ''"
    missing_subcat = "subcategoria IS NULL OR TRIM(subcategoria) = ''"

    filter_map = {
        "cat": f"({missing_cat})",
        "subcat": f"({missing_subcat})",
        "both": f"({missing_cat} OR {missing_subcat})",
    }

    if type not in filter_map:
        raise ValueError(f"type debe ser 'cat', 'subcat' o 'both'. Recibido: {type!r}")

    query = f"""
        SELECT DISTINCT {col}
        FROM renamu_variables
        WHERE year = {year}
          AND {col} IS NOT NULL
          AND TRIM({col}) != ''
          AND {filter_map[type]}
        LIMIT {batch_size}
    """
    return pd.read_sql_query(query, conn)


def get_existing_categories(conn) -> List[str]:
    """Retorna lista única de categorías ya clasificadas"""
    query = "SELECT DISTINCT categoria FROM renamu_variables WHERE categoria IS NOT NULL AND categoria != ''"
    df = pd.read_sql_query(query, conn)
    return df["categoria"].dropna().unique().tolist()


def get_existing_subcategories_by_category(conn, categoria: str) -> List[str]:
    """Retorna subcategorías existentes para una categoría específica"""
    query = """
        SELECT DISTINCT subcategoria 
        FROM renamu_variables 
        WHERE categoria = ? AND subcategoria IS NOT NULL AND subcategoria != ''
    """
    df = pd.read_sql_query(query, conn, params=(categoria,))
    return df["subcategoria"].dropna().unique().tolist()
