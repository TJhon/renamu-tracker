import sqlite3

import chromadb
import ollama

from src import config

db_main = config.OUTPUT_ROOT / "clasification" / "main.db"
conn = sqlite3.connect(db_main)

chroma_dir = config.OUTPUT_ROOT / "chroma"
chroma_collection = "renamu"
EMBED_MODEL = "nomic-embed-text"


def get_collection(clean=False):
    client = chromadb.PersistentClient(path=chroma_dir)
    existing = [c.name for c in client.list_collections()]
    if chroma_collection in existing and clean:
        client.delete_collection(chroma_collection)
        print("eliminando coleccion anterior")
    coll = client.create_collection(
        name=chroma_collection, metadata={"hnsw:space": "cosine"}, get_or_create=True
    )
    return coll


def embed_one(text: str) -> list[float]:
    resp = ollama.embed(model=EMBED_MODEL, input=[text])
    return resp.embeddings[0]


# ── Búsqueda semántica ────────────────────────────────────────────────────────
TOP_K = 5  # candidatos que devuelve el vector DB
SIMILARITY_THRESHOLD = 0.80  # distancia coseno mínima para considerar match

# ── Prompts ───────────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """
Eres un experto en clasificación de variables estadísticas del sector público peruano.
Tu tarea es determinar si dos descripciones de variables representan esencialmente
el mismo concepto, aunque su redacción sea distinta. Si no representan el mismo concepto entonces coloca una puntuacion baja
 
Responde SOLO con un JSON válido, sin texto adicional.
""".strip()

MATCH_PROMPT_TEMPLATE = """
Descripción consulta : "{query}"
Descripción candidata: "{candidate}"
 
¿Representan el mismo concepto estadístico?
 
Responde con:
{{
  "match": true | false,
  "confidence": 0.000-1.000,
  "razon": "breve explicación"
}}
""".strip()

SUBSET_PROMPT_TEMPLATE = """
Descripción A: "{a}"
Descripción B: "{b}"
 
¿Es A un subconjunto o desagregación de B (o viceversa)?
 
Responde con:
{{
  "a_es_parte_de_b": true | false,
  "b_es_parte_de_a": true | false,
  "confidence": 0.0-1.0,
  "razon": "breve explicación"
}}
""".strip()


# ── Ollama / Qwen ─────────────────────────────────────────────────────────────
LLM_MODEL = "qwen3.5:9b"
LLM_TEMPERATURE = 0.0
LLM_MAX_TOKENS = 256
