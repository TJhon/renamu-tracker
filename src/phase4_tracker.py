import pandas as pd
import tqdm

# from rich import print
from .tracker4.config import (
    MATCH_PROMPT_TEMPLATE,
    SIMILARITY_THRESHOLD,
    conn,
    embed_one,
    get_collection,
)
from .tracker4.utils import ask_llm

collection = get_collection()


def cosine_to_similarity(distance: float) -> float:
    """ChromaDB devuelve distancia coseno (0=idéntico, 2=opuesto)."""
    return 1.0 - distance / 2.0


# ── Añadir nueva descripción al vector DB ─────────────────────────────────
def add_to_db(row, meta=None):

    text = row.get("description")
    uid = row.get("uid_desc")

    emb = embed_one(text)
    collection.add(
        ids=[uid],
        embeddings=[emb],
        documents=[text],
        metadatas=[meta or {"test": "true"}],
    )
    # se agrego esta filal


def search(query, top_k=10):
    emb = embed_one(query)
    res = collection.query(
        [emb],
        n_results=min(top_k, collection.count()),
        include=["documents", "distances", "metadatas"],
    )
    results = []
    for i, uid in enumerate(res["ids"][0]):
        dist = res["distances"][0][i]
        sim = cosine_to_similarity(dist)
        results.append(
            {
                "uid": uid,
                "document": res["documents"][0][i],
                "similarity": sim,
            }
        )
    return results


# ── Confirmar con LLM si son el mismo concepto ────────────────────────────
def confirm_match(query: str, candidate: str) -> tuple[bool, float]:

    prompt = MATCH_PROMPT_TEMPLATE.format(query=query, candidate=candidate)
    ans = ask_llm(prompt)
    # print(ans)
    match = bool(ans.get("match", False))
    conf = float(ans.get("confidence", 0.0))
    return match, conf


def process_description(row: dict) -> tuple[dict, str]:
    """
    Retorna (matched_row, outcome).
    outcome: 'exact' | 'llm_match' | 'new'
    """
    text = row["description"]
    candidates = search(text)

    # ── Match casi exacto (sin LLM) ──────────────────────────────────────────
    for c in candidates:
        if c["similarity"] > 0.99:
            return c, "exact"

    # ── Confirmar con LLM ─────────────────────────────────────────────────────
    best_match = None
    best_conf = SIMILARITY_THRESHOLD

    for c in candidates:
        is_match, conf = confirm_match(text, c["document"])
        if is_match and conf > best_conf:
            best_match = c
            best_conf = conf

    if best_match:
        return best_match, "llm_match"

    # ── Nueva entrada ─────────────────────────────────────────────────────────
    add_to_db(row)
    return row, "new"


def update_uid_group(uid_desc: str, uid_group: str):
    """Persiste el resultado en SQLite."""
    conn.execute(
        "UPDATE unique_groups SET uid_group = ? WHERE uid_desc = ?",
        (uid_group, uid_desc),
    )
    conn.commit()


df = (
    pd.read_sql(
        "select year, description, uid_desc from unique_groups where uid_group == ''",
        con=conn,
    )
    .drop_duplicates(["uid_desc"])
    .sort_values("year", ascending=False)
)


# df = df.sample(10, random_state=1)
# print(df)
rows = df.to_dict("records")

print("iniciando clasificacion")


for row_dict in tqdm.tqdm(rows):
    matched, _ = process_description(row_dict)
    uid_group = matched.get("uid") or matched.get("uid_desc")
    update_uid_group(row_dict["uid_desc"], uid_group)
