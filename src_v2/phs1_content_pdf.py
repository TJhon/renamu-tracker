"""
Extracción de tablas desde PDFs del módulo de encuestas.

Para cada PDF en DATA_ROOT, itera página por página detectando líneas
horizontales/verticales que forman tablas, extrae el contenido de cada celda
(nombre de columna, descripción, valores) y guarda un CSV por página.
Si una página falla al procesar, guarda una imagen para revisión manual.

Salida: OUTPUT_ROOT/landing/tables/<año>/<año>_<módulo>_page_<N>.csv
"""

import shutil

import pandas as pd
import pdfplumber
import pdfplumber.page
from rich import print
from sqlalchemy import text
from tqdm import tqdm

from src_v2.config import DATA_ROOT, OUTPUT_ROOT
from src_v2.db.postgresql import (
    RAW_CONTENT,
    RAW_MAIN,
    RAW_META,
    RAW_TEXT_RECONSTRUCTED_TEXT_V2,
    psql_engine,
)
from src_v2.ia.config import MODEL_LLM
from src_v2.pdf_process.content_celdas import extract_headers, fill_cells_content
from src_v2.pdf_process.lines_horizontals import extract_hlines
from src_v2.pdf_process.lines_verticals import extract_vlines
from src_v2.pdf_process.tables import create_cells_ref, extract_tables_lines
from src_v2.utils import extract_year_module

TABLES_DIR = OUTPUT_ROOT / "landing" / "tables"
ERRORS_DIR = OUTPUT_ROOT / "landing" / "errors"
TABLES_IMG_DIR = OUTPUT_ROOT / "landing" / "tables" / "0_img"
COLUMN_TYPES = ["col_name", "col_desc"]
VALUE_TOLERANCE = 1

pdf_paths = list(DATA_ROOT.rglob("*.pdf"))


def main():
    shutil.rmtree(TABLES_DIR)
    for pdf_path in tqdm(sorted(pdf_paths)):
        year, module = extract_year_module(pdf_path)
        pdf_doc = pdfplumber.open(pdf_path)

        for page_idx, page in enumerate(pdf_doc.pages):
            page_num = page_idx + 1
            page_id = f"{year}_{module}_page_{page_num}"

            out_table = TABLES_DIR / year / f"{page_id}_main.csv"
            out_meta = TABLES_DIR / year / f"{page_id}_meta.csv"
            out_img_debug = TABLES_IMG_DIR / year / f"{page_id}.png"
            out_table.parent.mkdir(exist_ok=True, parents=True)
            out_img_debug.parent.mkdir(exist_ok=True, parents=True)

            words = page.extract_words()
            section_headers = extract_headers(page)

            # --- Detección de estructura de tabla ---
            try:
                vertical_lines = extract_vlines(page)
            except Exception:
                print(f"[red]Sin líneas verticales:[/red] {page_id}")
                # Guardamos imagen de la página para revisión manual
                error_path = ERRORS_DIR / year / f"{page_id}_error.png"
                error_path.parent.mkdir(exist_ok=True, parents=True)
                page.to_image(resolution=150).save(error_path)
                continue

            if len(vertical_lines) == 0:  # Página sin contenido estructurado
                continue

            horizontal_lines = extract_hlines(page)
            tables = extract_tables_lines(horizontal_lines, vertical_lines)
            cells, hlines = create_cells_ref(tables)

            # --- Metadatos de posición: líneas horizontales + encabezados de sección ---
            df_meta = pd.DataFrame()
            if hlines:
                df_hlines = pd.DataFrame(hlines).rename(columns={"y": "ymin"})
                df_meta = df_hlines.copy()
                if section_headers:
                    df_headers = pd.DataFrame(section_headers).sort_values("ymin")
                    df_meta = pd.concat(
                        [df_hlines, df_headers], ignore_index=True
                    ).sort_values("ymin")
                df_meta["ymin"] += 0.4  # La línea queda visualmente debajo del texto

            # --- Contenido por celda ---
            df_cells = pd.DataFrame(
                [c.to_dict() for c in fill_cells_content(cells, words)]
            )

            # Pivot: una fila por banda vertical, columnas = col_name y col_desc
            row_pos_cols = ["ymin", "ymax"]
            df_columns = (
                df_cells[df_cells["type"].isin(COLUMN_TYPES)]
                .pivot_table(
                    index=row_pos_cols,
                    columns="type",
                    values="content",
                    aggfunc="first",
                )
                .reset_index()
                .sort_values("ymin")
            )
            df_columns.columns.name = None
            column_rows = df_columns.to_dict("records")

            value_rows = (
                df_cells[df_cells["type"] == "value"]
                .drop(columns="type")
                .sort_values("ymin")
                .to_dict("records")
            )

            # Asignamos los valores que caen dentro del rango vertical de cada columna
            for col_row in column_rows:
                top_limit = col_row["ymax"] - VALUE_TOLERANCE
                bottom_limit = col_row["ymin"] + VALUE_TOLERANCE
                matched_values = [
                    v["content"]
                    for v in value_rows
                    if top_limit < v["ymax"] and bottom_limit > v["ymin"]
                ]
                col_row["values"] = "\n".join(matched_values)

            df_columns = (
                pd.DataFrame(column_rows)
                .sort_values("ymin")
                .reset_index(names="id_row_page")
            )

            # --- Guardado ---
            # CSV principal: columnas detectadas + líneas de referencia, ordenado verticalmente
            df_output = (
                pd.concat([df_columns, df_meta], ignore_index=True)
                .assign(year=year, module=module, page=page_num)
                .sort_values("ymin")
            )
            df_output.to_csv(out_table, index=False)

            # CSV de metadatos: celdas que no son col_name / col_desc / value
            df_other_cells = (
                df_cells[~df_cells["type"].isin([*COLUMN_TYPES, "value"])]
                .sort_values("ymin")
                .assign(year=year, module=module, page=page_num)
            )
            df_other_cells.columns.name = None
            df_other_cells.to_csv(out_meta, index=False)

            # img para verificar y probar
            img = page.to_image(resolution=130)
            img.save(out_img_debug)


def upload_to_db():

    csv_main = TABLES_DIR.rglob("*main.csv")
    metadata_main = TABLES_DIR.rglob("*meta.csv")
    results = []
    for table, source in [(RAW_MAIN, csv_main), (RAW_META, metadata_main)]:
        result = pd.concat(
            [pd.read_csv(csv) for csv in source], ignore_index=True
        ).assign(type_table=table)

        results.append(result)
    results_df = pd.concat(results, ignore_index=True)
    results_df[["year", "module", "page"]] = results_df[
        ["year", "module", "page"]
    ].astype("Int64")
    results_df = (
        results_df.sort_values(["year", "module", "page", "ymin"])
        .reset_index(drop=True)
        .reset_index(names="id_row")
    ).drop(columns="id_row_page", errors="ignore")
    results_df["module"] = results_df["module"].fillna(1)
    print(results_df.tail(20))

    results_df.to_sql(RAW_CONTENT, con=psql_engine, if_exists="replace", index=False)


def gen_text_to_recons():
    df = pd.read_sql(
        f"""
    select id_row, col_desc, values, content from {RAW_CONTENT}
    """,
        con=psql_engine,
    )
    df = df.melt(id_vars="id_row", var_name="col", value_name="text")
    df = df.dropna(subset=["text"])
    df = df[~df["text"].str.replace("\n", "").str.strip().str.len() < 3]
    df = df[~df["text"].str.contains("cuadro", case=False, na=False)]
    # df["text"] = df["text"].str.replace("\n", "__SALTO_LINEA__")
    df = df.reset_index(drop=True)
    df = df.assign(reconstructed_text=None, model=MODEL_LLM, processed_at=None)
    df.to_sql(
        RAW_TEXT_RECONSTRUCTED_TEXT_V2,
        con=psql_engine,
        index=False,
        # no reescribir lo que hace la IA
        if_exists="replace",
    )
    with psql_engine.connect() as conn:
        conn.execute(
            text(
                f"""
            ALTER TABLE {RAW_TEXT_RECONSTRUCTED_TEXT_V2} 
            ADD COLUMN values_json JSONB;
            """
            )
        )
        conn.commit()


if __name__ == "__main__":
    # main()
    upload_to_db()
    gen_text_to_recons()
