"""
phase2b_img_to_llm.py — Fase 2b: Extraer metadatos de las imágenes PNG con modelos en cloud de qwen3.5.

Lee de: {OUTPUT_ROOT}/files/{year}/{modulo}_pagina_{n}.png

Los resulados de esta parte pueden diferenciarse ya que depende del resultado estadistico del LLM (no se coloco una semilla para que pueda reproducirse tal cual el resultado)
"""

import time

from rich import print
from tqdm import tqdm

from src import config

from .ai.qwen_table_extractor import extract_table_from_image

IMG_DIR = config.OUTPUT_ROOT / "pdf_img"

imgs = list(IMG_DIR.rglob("*.jpg"))

imgs = imgs[::-1]

print(imgs[:2])

for img in tqdm(imgs):
    try:
        extract_table_from_image(img)
    except Exception as e:
        print(e)
        time.sleep(60)
        continue
