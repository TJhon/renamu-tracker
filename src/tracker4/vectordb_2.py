import time

import ollama
import pandas as pd
from tqdm import tqdm

from src.tracker4.config import EMBED_MODEL, conn, get_collection

BATCH_SIZE = 50


def embed_texts(texts: list[str]) -> list[list[float]]:
    """Llama a Ollama para obtener embeddings en batch."""
    response = ollama.embed(model=EMBED_MODEL, input=texts)
    return response.embeddings


collection = get_collection(clean=True)

base_df = pd.read_sql(
    """
               select * from unique_groups
               where year = 2025 
               """,
    con=conn,
).drop_duplicates(["description", "uid_desc"])

texts = base_df["description"].tolist()
ids = base_df["uid_desc"].tolist()

all_base_emb = []

for i in tqdm(range(0, len(texts), BATCH_SIZE), desc="Embeddings"):
    batch = texts[i : i + BATCH_SIZE]
    embs = embed_texts(batch)
    all_base_emb.extend(embs)
    time.sleep(0.05)  # pequeña pausa para no saturar Ollama

collection.add(
    ids=ids,
    embeddings=all_base_emb,
    documents=texts,
    metadatas=[{"source": "base_year"} for _ in ids],
)
