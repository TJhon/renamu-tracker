# src/classification/qwen_classifier.py
import json
import os
import time
from typing import Dict, List, Optional

import dashscope
import pandas as pd
from dashscope import Generation
from dotenv import find_dotenv, load_dotenv

from src.ai.prompts import (
    PROMPT_CLASSIFY_BATCH,
    PROMPT_CLASSIFY_CATEGORIA,
    PROMPT_CLASSIFY_SUBCATEGORIA,
)
from src.config import OUTPUT_ROOT

# ---------------------------------------------------------------------------
load_dotenv(find_dotenv())

QWEN_API = os.environ.get("QWEN_API_KEY")
QWEN_MODEL = os.environ.get("QWEN_MODEL", "qwen-vl-max")
QWEN_URL = os.environ.get("QWEN_URL")

print(QWEN_MODEL)

dashscope.api_key = QWEN_API
dashscope.base_http_api_url = QWEN_URL

DB_PATH = OUTPUT_ROOT / "clasification" / "main.db"

# Cuántos desc distintos mandar en un solo llamado al LLM (workflow 1)
BATCH_SIZE_YEAR = 80
API_SLEEP = 1.0


def call_qwen_api(prompt: str, temperature: float = 0.0) -> Optional[Dict]:
    """Llama a Qwen con manejo de retries y parsing JSON"""

    try:
        response = Generation.call(
            model=QWEN_MODEL,
            prompt=prompt,
            temperature=temperature,
            enable_search=False,
            # Para forzar JSON en modelos que lo soportan:
            # response_format={"type": "json_object"},
        )

        if response.status_code == 200:
            content = response.output.choices[0].message.content
            print(content)
            # Limpieza básica de markdown si aparece
            if content.startswith("```json"):
                content = content.replace("```json", "").replace("```", "").strip()
            elif content.startswith("```"):
                content = content.replace("```", "").strip()

            return json.loads(content)
        else:
            time.sleep(2)

    except Exception as e:
        print(e)
        time.sleep(2)

    return None


# call_qwen_api("hola")


def classify_batch(descriptions: List[str]) -> Optional[pd.DataFrame]:
    """Clasifica un batch de descripciones usando PROMPT_CLASSIFY_BATCH"""
    # Preparar input JSON para el prompt
    descs_json = json.dumps(descriptions, ensure_ascii=False)
    prompt = PROMPT_CLASSIFY_BATCH.format(descs_json=descs_json)

    result = call_qwen_api(prompt)
    # print(prompt)

    if result and isinstance(result, list):
        df = pd.DataFrame(result)
        # Validación mínima de esquema
        required_cols = ["desc_cuadro_pregunta", "categoria", "subcategoria"]
        if all(col in df.columns for col in required_cols):
            return df
    return None


def classify_categoria(desc: str, existing_categories: List[str]) -> Optional[Dict]:
    """Determina categoría para una descripción individual"""
    cats_json = json.dumps(existing_categories, ensure_ascii=False)
    prompt = PROMPT_CLASSIFY_CATEGORIA.format(desc=desc, categorias_json=cats_json)
    return call_qwen_api(prompt)


def classify_subcategoria(
    desc: str, categoria: str, existing_subcats: List[str]
) -> Optional[Dict]:
    """Determina subcategoría dentro de una categoría asignada"""
    subs_json = json.dumps(existing_subcats, ensure_ascii=False)
    prompt = PROMPT_CLASSIFY_SUBCATEGORIA.format(
        desc=desc, categoria=categoria, subcategorias_json=subs_json
    )
    return call_qwen_api(prompt)
