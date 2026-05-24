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
OUTPUT_ROOT = Path(os.environ["OUTPUT_ROOT_v2"])
