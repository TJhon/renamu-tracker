"""
config.py — Carga variables de entorno del proyecto RENAMU.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# Cargar .env desde la raíz del proyecto (un nivel arriba de src/)
_ROOT = Path(__file__).parent.parent
load_dotenv(_ROOT / ".env")

# ── Rutas ──────────────────────────────────────────────────────────────────
DATA_ROOT = Path(os.environ["DATA_ROOT"])
OUTPUT_ROOT = Path(os.environ["OUTPUT_ROOT"])

# ── PostgreSQL ─────────────────────────────────────────────────────────────
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_DB = os.environ.get("PG_DB", "renamu")
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "postgres")
