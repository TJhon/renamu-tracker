import sqlite3
from pathlib import Path

import pandas as pd
import pdfplumber
from rich import print

from src_v2.config import DATA_ROOT, OUTPUT_ROOT
from src_v2.utils import (
    extract_year_module,
)

pd
test_save = OUTPUT_ROOT / "test"
test_save.mkdir(exist_ok=True, parents=True)
print

pdf_paths = list(DATA_ROOT.rglob("*.pdf"))

all_dfs = []

for pdf in pdf_paths:
    year, module = extract_year_module(pdf)

    with pdfplumber.open(pdf) as pdf_open:
        for page_num, page in enumerate(pdf_open.pages, start=1):
            tables = page.extract_tables()

            if not tables:
                continue

            for table_num, table in enumerate(tables, start=1):
                if not table:
                    continue

                df = pd.DataFrame(table)

                if df.empty:
                    continue

                n_cols = len(df.columns)

                cols = [f"col_{i}" for i in range(1, n_cols + 1)]

                if n_cols >= 1:
                    cols[-1] = "valores"

                if n_cols >= 2:
                    cols[-2] = "desc_campo"

                if n_cols >= 3:
                    cols[-3] = "nombre_campo"

                df.columns = cols

                # eliminar fila de encabezado si existe
                first_row = df.iloc[0].astype(str).str.upper().str.cat(sep=" ")

                if "NOMBRE" in first_row and "CAMPO" in first_row:
                    df = df.iloc[1:]

                df["year"] = int(year)
                df["module"] = module
                df["page"] = page_num
                df["table_number"] = table_num
                df["pdf_name"] = Path(pdf).name

                all_dfs.append(df)

final_df = pd.concat(all_dfs, ignore_index=True)

final_df.insert(0, "row_number", range(1, len(final_df) + 1))

final_df = final_df.sort_values(
    ["year", "module", "page", "table_number", "row_number"]
).reset_index(drop=True)

with sqlite3.connect("encuestas.sqlite") as conn:
    final_df.to_sql("preguntas", conn, if_exists="replace", index=False)

# # nuevo pdf
# page_number = 3
# new_pdf = fitz.open()

# # copiar página
# new_pdf.insert_pdf(dc, from_page=page_number, to_page=page_number)

# # guardar
# new_pdf.save(test_save / "pagina_3.pdf")


# new_pdf.close()
# dc.close()

# doc = pdfplumber.open(test_save / "pagina_3.pdf")
# p = doc.pages[0]
# # lines = p.lines
# im = p.to_image()
# im.draw_lines(p.lines)
# im.draw_lines(p.rects)
# im.draw_lines(p.curves)
# im.save(test_save / "pagina_3.png")
# selecte_pdf = pdf_paths[12]
# print(selecte_pdf)

# pdf_open = pdfplumber.open(selecte_pdf)
# page = pdf_open.pages[2]

# lines = page.lines
# im = page.to_image()
# im.draw_lines(lines)
# print(test_save / "sample.png")
# im.save(test_save / "sample.png")
