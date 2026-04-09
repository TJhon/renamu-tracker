import base64
import json
import os
from pathlib import Path

import dashscope
import pandas as pd
from dotenv import find_dotenv, load_dotenv
from rich import print

from src.config import OUTPUT_ROOT

IMAGES_DIR = OUTPUT_ROOT / "pdf_img"
IMAGES_PARSER_LLM = OUTPUT_ROOT / "llm_table_parser"

load_dotenv(find_dotenv())

QWEN_API = os.environ.get("QWEN_API_KEY")
QWEN_MODEL = os.environ.get("QWEN_MODEL")
QWEN_URL = os.environ.get("QWEN_URL")
dashscope.api_key = QWEN_API
dashscope.base_http_api_url = QWEN_URL


PROMPT_EXTRACT_TABLE = """
Extrae la tabla de la imagen.

Campos que me interesan:

- cuadro: str:  cuadro (no siempre disponible)
- n_q : str: numero de pregunta (no siempre disponible)
- desc_q - str: descripcion de la pregunta o cuadro (no siempre disponible)
- col - str: nombre del campo (nombre de columna de base de datos; solo caracteres alfanuméricos y "_" sin espacios)
- desc_col - str: descripcion del campo
- values - str:  lista de cada  valores y descripcion del valor, se debe serparar por ";"
- value_pos - int: (solo para los que tienen 2 valores) que representa el valor positivo 

Para valores si solo hay 2 valores que representen ausencia o presencia de esa variable solo me interesa el valor numérico que representa al valor positivo. 

Si la pregunta/cuadro/descripcion se repiten, repítelos en cada fila.

A veces aparecerá texto adicional que representa un H2 (header nivel 2) y esta fuera de la tabla y no estara asociada a ningun `col` (importante).
En ese caso devuelve una fila intermedia con:
{"h2": "texto"}

Devuelve únicamente un JSON válido, sin texto adicional,
que pueda ser parseado directamente con pandas.
"""

# print(PROMPT_EXTRACT_TABLE)


# Function to encode the image
def encode_image(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode("utf-8")


def response_parser(response, path: Path):
    name = path.stem
    year = path.parent.stem

    path_json = IMAGES_PARSER_LLM / "json_parser" / year / f"{name}.json"
    path_df = IMAGES_PARSER_LLM / "df_parser" / year / f"{name}.csv"

    path_json.parent.mkdir(parents=True, exist_ok=True)
    path_df.parent.mkdir(parents=True, exist_ok=True)

    response_text = response.output.choices[0].message.content[0]["text"]

    try:
        json_data = json.loads(response_text)

        with open(path_json, "w", encoding="utf-8") as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        try:
            df_data: pd.DataFrame = pd.DataFrame(json_data)
            df_data.to_csv(path_df, index=False)
        except Exception:
            pass
    except Exception:
        print(Exception)
        pass


def extract_table_from_image(image_path: Path):
    name = image_path.stem
    year = image_path.parent.stem

    path_json = IMAGES_PARSER_LLM / "json_parser" / year / f"{name}.json"
    path_df = IMAGES_PARSER_LLM / "df_parser" / year / f"{name}.csv"

    if path_json.exists():
        # si existe esto se debe pasar esto al modelo  para no recargar los tokens nuevamente
        pass
    if path_df.exists():
        # se termina definitivamente
        return

    base64_image = encode_image(image_path)

    messages = [
        {
            "role": "user",
            "content": [
                {"text": PROMPT_EXTRACT_TABLE},
                {"image": f"data:image/jpeg;base64,{base64_image}"},
            ],
        }
    ]

    response = dashscope.MultiModalConversation.call(
        model=QWEN_MODEL,
        messages=messages,
        enable_thinking=False,
        response_format={"type": "json_object"},
        temperature=0,
    )

    response_parser(response, image_path)


# a
if __name__ == "__main__":
    img1 = Path(
        r"E:\All\temp_borrar\antigraviti\renamu_diccionario_tracker\output\pdf_img\2018\2018_4_page_7.jpg"
    )

    r1 = extract_table_from_image(img1)

    img2 = Path(
        r"E:\All\temp_borrar\antigraviti\renamu_diccionario_tracker\output\pdf_img\2020\2020_0_page_12.jpg"
    )
    r2 = extract_table_from_image(img2)
    img3 = Path(
        r"E:\All\temp_borrar\antigraviti\renamu_diccionario_tracker\output\pdf_img\2013\2013_7_page_22.jpg"
    )
    r3 = extract_table_from_image(img3)
    img4 = Path(
        r"E:\All\temp_borrar\antigraviti\renamu_diccionario_tracker\output\pdf_img\2005\2005_7_page_5.jpg"
    )
    r4 = extract_table_from_image(img4)
