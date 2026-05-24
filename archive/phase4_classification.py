"""
RENAMU - Pipeline de mapping de columnas a lo largo del tiempo
==============================================================
Flujo:
  1. Carga master 2025 desde SQLite (pdf_rows.desc_col)
  2. Limpia descripciones (unicode ya aplicado, elimina no-alfanuméricos)
  3. Indexa master en ChromaDB (embeddings locales via Ollama)
  4. Para cada año anterior: match exacto → embedding auto → LLM ranking → nuevo ID
  5. Guarda resultados en SQLite (tabla column_mapping)

Requisitos:
    pip install chromadb ollama sentence-transformers

Ollama models necesarios (correr antes):
    ollama pull nomic-embed-text
    ollama pull qwen2.5:7b
"""

import json
import logging
import re
import sqlite3
from dataclasses import dataclass
from typing import Optional

import chromadb
import ollama
from chromadb.utils import embedding_functions

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

DB_PATH = r"E:\All\temp_borrar\antigraviti\renamu_diccionario_tracker\output\clasification\main.db"
CHROMA_PATH = r"E:\All\temp_borrar\antigraviti\renamu_diccionario_tracker\output\clasification\chroma_db"

MASTER_YEAR = 2025
EMBED_MODEL = "nomic-embed-text"  # ollama pull nomic-embed-text
LLM_MODEL = "qwen2.5:7b"  # ollama pull qwen2.5:7b

SIMILARITY_AUTO_THRESHOLD = 0.92  # score >= este valor → match automático sin LLM
TOP_K = 5  # cuántos candidatos pasar al LLM

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("renamu_pipeline.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------


def clean_desc(text: str) -> str:
    """
    Limpia una descripción:
      - Strip whitespace
      - Elimina '?' y todo carácter no alfanumérico excepto espacios y '/'
        (se conserva '/' porque es separador semántico en RENAMU, ej: 'Número / Total')
      - Colapsa espacios múltiples
    Ajusta el regex según necesites.
    """
    if not text:
        return ""
    text = text.strip()
    # Eliminar '?' explícitamente primero
    text = text.replace("?", "")
    # Eliminar caracteres no alfanuméricos excepto espacios y '/'
    text = re.sub(r"[^a-zA-Z0-9 /]", " ", text)
    # Colapsar espacios
    text = re.sub(r"\s+", " ", text).strip()
    return text


@dataclass
class MappingResult:
    desc_col_raw: str
    desc_col_clean: str
    year: int
    master_id: Optional[str]
    match_method: str  # 'exact' | 'embedding_auto' | 'llm_ranked' | 'new_id'
    similarity_score: float = 0.0
    llm_response: str = ""


# ---------------------------------------------------------------------------
# DATABASE
# ---------------------------------------------------------------------------


def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def setup_output_tables(conn: sqlite3.Connection):
    """Crea tablas de salida si no existen."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS master_columns (
            master_id   TEXT PRIMARY KEY,
            desc_clean  TEXT NOT NULL UNIQUE,
            desc_raw    TEXT,
            year_added  INTEGER
        );

        CREATE TABLE IF NOT EXISTS column_mapping (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            year            INTEGER NOT NULL,
            desc_col_raw    TEXT,
            desc_col_clean  TEXT,
            master_id       TEXT,
            match_method    TEXT,
            similarity_score REAL,
            llm_response    TEXT,
            FOREIGN KEY (master_id) REFERENCES master_columns(master_id)
        );

        CREATE INDEX IF NOT EXISTS idx_mapping_year ON column_mapping(year);
        CREATE INDEX IF NOT EXISTS idx_mapping_master ON column_mapping(master_id);
    """)
    conn.commit()
    log.info("Tablas de salida listas.")


def load_year_descriptions(conn: sqlite3.Connection, year: int) -> list[dict]:
    """Carga descripciones únicas de un año desde pdf_rows."""
    cur = conn.execute(
        "SELECT DISTINCT desc_col FROM pdf_rows WHERE year = ? AND desc_col IS NOT NULL",
        (year,),
    )
    rows = cur.fetchall()
    return [
        {"desc_raw": r["desc_col"], "desc_clean": clean_desc(r["desc_col"])}
        for r in rows
        if r["desc_col"]
    ]


def get_available_years(conn: sqlite3.Connection) -> list[int]:
    """Retorna años disponibles en pdf_rows, ordenados descendente."""
    cur = conn.execute(
        "SELECT DISTINCT year FROM pdf_rows WHERE year IS NOT NULL ORDER BY year DESC"
    )
    return [r["year"] for r in cur.fetchall()]


def save_master_column(
    conn: sqlite3.Connection, master_id: str, desc_clean: str, desc_raw: str, year: int
):
    conn.execute(
        "INSERT OR IGNORE INTO master_columns (master_id, desc_clean, desc_raw, year_added) VALUES (?,?,?,?)",
        (master_id, desc_clean, desc_raw, year),
    )
    conn.commit()


def save_mapping(conn: sqlite3.Connection, result: MappingResult):
    conn.execute(
        """INSERT INTO column_mapping
           (year, desc_col_raw, desc_col_clean, master_id, match_method, similarity_score, llm_response)
           VALUES (?,?,?,?,?,?,?)""",
        (
            result.year,
            result.desc_col_raw,
            result.desc_col_clean,
            result.master_id,
            result.match_method,
            result.similarity_score,
            result.llm_response,
        ),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# EMBEDDINGS (ChromaDB + Ollama nomic-embed-text)
# ---------------------------------------------------------------------------


class EmbeddingStore:
    """Wrapper sobre ChromaDB usando embeddings de Ollama."""

    def __init__(self, chroma_path: str):
        self.client = chromadb.PersistentClient(path=chroma_path)
        # Usamos función de embedding custom con Ollama
        self.ef = embedding_functions.OllamaEmbeddingFunction(
            url="http://localhost:11434/api/embeddings",
            model_name=EMBED_MODEL,
        )
        self.collection = self.client.get_or_create_collection(
            name="renamu_master",
            embedding_function=self.ef,
            metadata={"hnsw:space": "cosine"},
        )
        log.info(f"ChromaDB listo. Documentos indexados: {self.collection.count()}")

    def add(self, master_id: str, desc_clean: str):
        """Agrega una descripción al índice."""
        self.collection.upsert(
            ids=[master_id],
            documents=[desc_clean],
            metadatas=[{"master_id": master_id}],
        )

    def query(self, desc_clean: str, top_k: int = TOP_K) -> list[dict]:
        """
        Retorna top_k candidatos con su score de similitud.
        ChromaDB con cosine retorna distancias (0=idéntico, 2=opuesto),
        convertimos a similitud: sim = 1 - dist/2  (rango [0,1])
        """
        if self.collection.count() == 0:
            return []
        results = self.collection.query(
            query_texts=[desc_clean],
            n_results=min(top_k, self.collection.count()),
            include=["documents", "distances", "metadatas"],
        )
        candidates = []
        for doc, dist, meta in zip(
            results["documents"][0],
            results["distances"][0],
            results["metadatas"][0],
        ):
            similarity = 1.0 - dist / 2.0
            candidates.append(
                {
                    "master_id": meta["master_id"],
                    "desc_clean": doc,
                    "similarity": round(similarity, 4),
                }
            )
        return candidates


# ---------------------------------------------------------------------------
# LLM RANKING (Ollama)
# ---------------------------------------------------------------------------

LLM_PROMPT_TEMPLATE = """Eres un experto en datos municipales del Peru (RENAMU).
Tu tarea es determinar si alguna de las descripciones CANDIDATAS corresponde semanticamente
a la descripcion CONSULTA. Ambas pueden estar en distintos años y tener pequeñas variaciones
de redaccion pero referirse al mismo concepto.

CONSULTA:
  {query}

CANDIDATAS (ordenadas por similitud embedding):
{candidates}

Responde SOLO con un JSON valido con esta estructura:
{{
  "match": true | false,
  "best_master_id": "<master_id o null>",
  "confidence": "high" | "medium" | "low",
  "reason": "<explicacion breve en español>"
}}

- Si ninguna candidata corresponde semanticamente, pon match=false y best_master_id=null.
- No incluyas texto fuera del JSON.
"""


def llm_rank(query_desc: str, candidates: list[dict]) -> dict:
    """Llama al LLM local para rankear candidatos. Retorna dict con match, best_master_id, etc."""
    candidates_text = "\n".join(
        f"  [{i + 1}] master_id={c['master_id']} | sim={c['similarity']:.3f} | desc: {c['desc_clean']}"
        for i, c in enumerate(candidates)
    )
    prompt = LLM_PROMPT_TEMPLATE.format(query=query_desc, candidates=candidates_text)

    try:
        response = ollama.chat(
            model=LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            options={"temperature": 0.0},
        )
        raw = response["message"]["content"].strip()
        # Intentar extraer JSON aunque haya texto extra
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if match:
            return json.loads(match.group())
        log.warning(f"LLM no retornó JSON válido: {raw[:200]}")
        return {
            "match": False,
            "best_master_id": None,
            "confidence": "low",
            "reason": "parse_error",
        }
    except Exception as e:
        log.error(f"Error LLM: {e}")
        return {
            "match": False,
            "best_master_id": None,
            "confidence": "low",
            "reason": str(e),
        }


# ---------------------------------------------------------------------------
# GENERADOR DE IDs
# ---------------------------------------------------------------------------


def generate_master_id(conn: sqlite3.Connection) -> str:
    """Genera un ID secuencial tipo MCL-00001."""
    cur = conn.execute("SELECT COUNT(*) as n FROM master_columns")
    n = cur.fetchone()["n"] + 1
    return f"MCL-{n:05d}"


# ---------------------------------------------------------------------------
# PIPELINE PRINCIPAL
# ---------------------------------------------------------------------------


def index_master(
    conn: sqlite3.Connection, store: EmbeddingStore, year: int = MASTER_YEAR
):
    """Indexa el año master (2025) en ChromaDB y en master_columns."""
    log.info(f"=== Indexando master año {year} ===")
    descs = load_year_descriptions(conn, year)
    log.info(f"  {len(descs)} descripciones únicas encontradas.")

    already = conn.execute("SELECT COUNT(*) as n FROM master_columns").fetchone()["n"]
    if already > 0:
        log.info(f"  Master ya tiene {already} registros. Saltando re-indexación.")
        log.info(
            "  Si quieres re-indexar, borra la tabla master_columns y la carpeta chroma_db."
        )
        return

    for d in descs:
        if not d["desc_clean"]:
            continue
        mid = generate_master_id(conn)
        save_master_column(conn, mid, d["desc_clean"], d["desc_raw"], year)
        store.add(mid, d["desc_clean"])

    log.info(
        f"  Master indexado: {conn.execute('SELECT COUNT(*) FROM master_columns').fetchone()[0]} registros."
    )


def process_year(conn: sqlite3.Connection, store: EmbeddingStore, year: int):
    """Procesa un año y genera su mapping contra el master."""
    log.info(f"\n=== Procesando año {year} ===")
    descs = load_year_descriptions(conn, year)
    log.info(f"  {len(descs)} descripciones únicas.")

    # Cargar exact-match lookup desde master
    master_lookup = {
        r["desc_clean"]: r["master_id"]
        for r in conn.execute(
            "SELECT desc_clean, master_id FROM master_columns"
        ).fetchall()
    }

    stats = {"exact": 0, "embedding_auto": 0, "llm_ranked": 0, "new_id": 0}

    for d in descs:
        desc_clean = d["desc_clean"]
        desc_raw = d["desc_raw"]

        if not desc_clean:
            continue

        result = MappingResult(
            desc_col_raw=desc_raw,
            desc_col_clean=desc_clean,
            year=year,
            master_id=None,
            match_method="new_id",
        )

        # --- 1. Match exacto ---
        if desc_clean in master_lookup:
            result.master_id = master_lookup[desc_clean]
            result.match_method = "exact"
            result.similarity_score = 1.0
            stats["exact"] += 1
            log.debug(f"  [EXACT] {desc_clean[:60]} → {result.master_id}")

        else:
            # --- 2. Embeddings top-K ---
            candidates = store.query(desc_clean, top_k=TOP_K)

            if candidates and candidates[0]["similarity"] >= SIMILARITY_AUTO_THRESHOLD:
                # Score muy alto → match automático sin LLM
                result.master_id = candidates[0]["master_id"]
                result.match_method = "embedding_auto"
                result.similarity_score = candidates[0]["similarity"]
                stats["embedding_auto"] += 1
                log.debug(
                    f"  [EMB AUTO] {desc_clean[:60]} → {result.master_id} (sim={result.similarity_score})"
                )

            elif candidates:
                # --- 3. LLM ranking ---
                llm_out = llm_rank(desc_clean, candidates)
                result.llm_response = json.dumps(llm_out, ensure_ascii=False)

                if llm_out.get("match") and llm_out.get("best_master_id"):
                    result.master_id = llm_out["best_master_id"]
                    result.match_method = "llm_ranked"
                    result.similarity_score = candidates[0]["similarity"]
                    stats["llm_ranked"] += 1
                    log.debug(f"  [LLM] {desc_clean[:60]} → {result.master_id}")
                else:
                    # --- 4. Nuevo ID ---
                    new_id = generate_master_id(conn)
                    save_master_column(conn, new_id, desc_clean, desc_raw, year)
                    store.add(new_id, desc_clean)
                    master_lookup[desc_clean] = new_id  # actualizar lookup en memoria
                    result.master_id = new_id
                    result.match_method = "new_id"
                    stats["new_id"] += 1
                    log.info(f"  [NEW ID] {desc_clean[:60]} → {new_id}")
            else:
                # Sin candidatos (store vacío o primer registro)
                new_id = generate_master_id(conn)
                save_master_column(conn, new_id, desc_clean, desc_raw, year)
                store.add(new_id, desc_clean)
                master_lookup[desc_clean] = new_id
                result.master_id = new_id
                result.match_method = "new_id"
                stats["new_id"] += 1

        save_mapping(conn, result)

    log.info(f"  Stats: {stats}")


def run_pipeline(years_to_process: Optional[list[int]] = None):
    """
    Punto de entrada principal.
    Si years_to_process=None, procesa todos los años disponibles excepto el master.
    """
    log.info("=" * 60)
    log.info("RENAMU Column Mapping Pipeline")
    log.info("=" * 60)

    conn = get_connection(DB_PATH)
    store = EmbeddingStore(CHROMA_PATH)
    setup_output_tables(conn)

    # Paso 1: Indexar master
    index_master(conn, store, year=MASTER_YEAR)

    # Paso 2: Determinar años a procesar
    all_years = get_available_years(conn)
    if years_to_process is None:
        years_to_process = [y for y in all_years if y != MASTER_YEAR]

    log.info(f"\nAños a procesar: {years_to_process}")

    # Paso 3: Procesar año por año (de más reciente a más antiguo)
    for year in sorted(years_to_process, reverse=True):
        process_year(conn, store, year)

    # Paso 4: Resumen final
    total_mapped = conn.execute("SELECT COUNT(*) FROM column_mapping").fetchone()[0]
    total_master = conn.execute("SELECT COUNT(*) FROM master_columns").fetchone()[0]
    log.info("\n" + "=" * 60)
    log.info("PIPELINE COMPLETO")
    log.info(f"  Master IDs generados : {total_master}")
    log.info(f"  Mappings guardados   : {total_mapped}")
    log.info("=" * 60)

    conn.close()


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Para procesar todos los años:
    run_pipeline()

    # Para procesar solo años específicos:
    # run_pipeline(years_to_process=[2024, 2023, 2022])
