"""
phase1_scan.py — Fase 1: Escanear archivos .dbf / .dta y registrar en files_meta.

Reanudable: si (year, cuadro_n) ya existe en files_meta → skip.
"""

import logging
import re
import sqlite3
from pathlib import Path

import pandas as pd
from tqdm import tqdm

# from src import config, db
from . import config

logger = logging.getLogger(__name__)

# ── Helpers ────────────────────────────────────────────────────────────────


_YEAR_RE = re.compile(r"^\d{4}$")
_NUM_RE = re.compile(r"(\d+)([a-zA-Z]*)")
_MOD_RE = re.compile(r"Modulo(\d+)", re.IGNORECASE)


def extract_year_module(path: Path):
    year = None
    module = None

    parent = path.parent

    # detectar año
    if m := _YEAR_RE.match(parent.stem):
        year = m.group()

    # detectar modulo
    if m := _MOD_RE.search(parent.stem):
        module = m.group(1)

        # el año está en la carpeta superior
        if y := _YEAR_RE.match(parent.parent.stem):
            year = y.group()

    return year, module


db_main = config.OUTPUT_ROOT / "clasification" / "main.db"


def _extract_year(path: Path) -> int | None:
    """Busca la carpeta de año (4 dígitos) en la ruta, de más profunda a más superficial."""
    for part in reversed(path.parts):
        if _YEAR_RE.match(part):
            return int(part)
    return None


def _extract_cuadro_n(stem: str) -> int:
    """Extrae el número del cuadro del nombre de archivo. Si no hay número → 1."""
    matches = _NUM_RE.findall(stem)  # lista de tuplas (numero, sufijo)
    if not matches:
        return "1"
    num, suffix = matches[-1]
    return str(int(num)) + suffix


def _read_file(path: Path) -> pd.DataFrame:
    """Lee .dbf o .dta y retorna DataFrame."""
    suffix = path.suffix.lower()
    if suffix == ".dta":
        try:
            return pd.read_stata(path, convert_categoricals=False)
        except Exception:
            print(path)
            return pd.read_stata(path, convert_categoricals=False, encoding="latin-1")
    elif suffix == ".dbf":
        try:
            from simpledbf import Dbf5

            dbf = Dbf5(str(path), codec="latin1")
            return dbf.to_dataframe()
        except Exception:
            from dbfread import DBF

            table = DBF(str(path), encoding="latin1")
            return pd.DataFrame(iter(table))
    raise ValueError(f"Formato no soportado: {suffix}")


def _detect_id_col(columns: list[str]) -> str | None:
    """Detecta la primera columna que empiece por 'id' o 'ubigeo'."""
    for col in columns:
        if col.startswith("id") or col.startswith("ubig"):
            return col
    return None


def _build_id_column(df: pd.DataFrame, path="") -> pd.DataFrame:
    """
    Construye columna 'id' = ccdd(zfill 2) + ccpp(zfill 2) + ccdi(zfill 2).
    Elimina filas donde alguno de los tres no sea numérico.
    """
    # normalizar nombre
    if "ccdp" in df.columns and "ccdd" not in df.columns:
        df = df.rename(columns={"ccdp": "ccdd"})

    required = {"ccdd", "ccpp", "ccdi"}
    cols_lower = set(df.columns)

    if not required.issubset(cols_lower):
        print(df.head())
        logger.warning(f"No se encontraron columnas ccdd/ccpp/ccdi — Para {path}")
        return df

    df = df.copy()
    for col in ["ccdd", "ccpp", "ccdi"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["ccdd", "ccpp", "ccdi"])
    df[["ccdd", "ccpp", "ccdi"]] = df[["ccdd", "ccpp", "ccdi"]].astype(int)
    df["id_jk"] = (
        df["ccdd"].astype(str).str.zfill(2)
        + df["ccpp"].astype(str).str.zfill(2)
        + df["ccdi"].astype(str).str.zfill(2)
    )
    return df


def _register_cuadro(db_path, year: int, cuadro: str, modulo: str, columns: list[str]):
    df_meta = pd.DataFrame(
        {"year": year, "cuadro": cuadro, "column": columns, "modulo": modulo}
    )
    with sqlite3.connect(db_path) as con:
        df_meta.to_sql("cuadros", con, if_exists="append", index=False)


# ── Función principal ──────────────────────────────────────────────────────


def run():
    """Ejecuta la Fase 1 completa."""

    output_base = config.OUTPUT_ROOT / "parquets"
    data_root = config.DATA_ROOT

    # Recoger todos los archivos de datos
    patterns = list(data_root.rglob("*.dbf")) + list(data_root.rglob("*.dta"))

    for path in tqdm(sorted(patterns)):
        year, mod = extract_year_module(path)
        cuadro_n = _extract_cuadro_n(path.stem)
        out_path = output_base / str(year) / f"c{cuadro_n}.parquet"

        if out_path.exists():
            continue

        try:
            df = _read_file(path)
        except Exception as e:
            logger.error(f"Error leyendo {path}: {e}")
            continue

        # Normalizar columnas
        df.columns = [c.lower().strip() for c in df.columns]

        id_col = _detect_id_col(list(df.columns))

        df = _build_id_column(df, path=path)
        df["year"] = year

        if id_col:
            df["id_original"] = (
                df[id_col].astype(str).str.extract(r"(\d+)").astype(float)
            )

        # Guardar parquet
        out_path.parent.mkdir(parents=True, exist_ok=True)

        df.to_parquet(out_path, index=False)
        _register_cuadro(db_main, year, cuadro_n, mod, columns=list(df.columns))

    logger.info("Fase 1 completada.")


if __name__ == "__main__":
    run()
