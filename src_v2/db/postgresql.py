from sqlalchemy import create_engine

from src_v2.ia.config import MODEL_LLM

psql_engine = create_engine(
    "postgresql+psycopg://postgres:postgres@localhost:5432/textdb"
)


RAW_MAIN = "raw_1_main"
RAW_META = "raw_2_metadata"
RAW_CONTENT = "raw_content"
RAW_TEXT_RECONSTRUCTED_TEXT_V2 = f"raw_content_re_{MODEL_LLM}".replace(".", "").replace(
    ":", "_"
)
