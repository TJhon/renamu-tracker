from pathlib import Path

import pandas as pd
import pdfplumber
import pdfplumber.page
from rich import print
from tqdm import tqdm

from src_v2.config import DATA_ROOT, OUTPUT_ROOT
from src_v2.pdf_process.content_celdas import (
    extract_headers,
    fill_cells_content,
)
from src_v2.pdf_process.lines_horizontals import extract_hlines
from src_v2.pdf_process.lines_verticals import extract_vlines
from src_v2.pdf_process.tables import create_cells_ref, extract_tables_lines
from src_v2.utils import (
    extract_year_module,
)

pd
test_save = OUTPUT_ROOT / "test"
test_save.mkdir(exist_ok=True, parents=True)
print

main_info = OUTPUT_ROOT / "landing"
tables_dir = (
    main_info / "tables"
)  # solo las columnas col_name, col_description y values
metadata_dir = (
    main_info / "cuadros"
)  # todo lo demas como cuadro, numero de pregunta, description de cuadro y posicion de headers


pdf_paths = list(DATA_ROOT.rglob("*.pdf"))


for pdf in tqdm(sorted(pdf_paths)):
    year, module = extract_year_module(pdf)
    pdf_open = pdfplumber.open(pdf)

    for i, page in enumerate(pdf_open.pages):
        metadata_df = pd.DataFrame(
            {"year": [year], "module": [module], "page": [i + 1]}
        )

        # solo test: para ver si recoge correctamente las celdas
        output_file = f"test/debug/{year}_{module}_{Path(pdf).stem}_page_{i + 1}.png"
        out_table = tables_dir / year / f"{year}_{module}_page_{i + 1}.csv"
        out_table1 = tables_dir / year / f"{year}_{module}_page_{i + 1}_q.csv"
        out_h2 = tables_dir / year / f"{year}_{module}_page_{i + 1}_h2.csv"
        out_lines = tables_dir / year / f"{year}_{module}_page_{i + 1}_lines.csv"
        out_meta = tables_dir / year / f"{year}_{module}_page_{i + 1}_meta.csv"
        for out in [out_table, out_meta]:
            out.parent.mkdir(parents=True, exist_ok=True)

        if out_table.exists():
            continue

        # headers
        h2 = extract_headers(page)
        df_h2 = pd.DataFrame()
        if h2:
            df_h2 = pd.DataFrame(h2).sort_values("ymin")
            # df_h2.to_csv(out_h2, index=False)
        # lineas verticales que representan a las divisiones de columnas
        try:
            verticals = extract_vlines(page)
        except:
            print(f"{year}_{module}_page_{i + 1}")
            continue

        if len(verticals) == 0:  # omitimos paginas sin contenido
            continue

        # lineas horizontales
        horizontals = extract_hlines(page)
        # identificacion de las tablas
        tables = extract_tables_lines(horizontals, verticals)
        #
        cells, hlines = create_cells_ref(tables)
        df_meta = pd.DataFrame()
        if hlines:
            df_lines = pd.DataFrame(hlines).rename(columns={"y": "ymin"})
            if h2:
                df_meta = pd.concat([df_lines, df_h2], ignore_index=True).sort_values(
                    "ymin"
                )
                # df_lines.to_csv(out_lines, index=False)
                # df_meta.to_csv(out_meta, index=False)
        words = page.extract_words()

        cells_content = fill_cells_content(cells, words)
        # contenido por celdas
        cells_content_dict = [c.to_dict() for c in cells_content]
        id_pos = ["ymin", "ymax"]
        # solo el nombre de columnas/descripcion y valores
        df = pd.DataFrame(cells_content_dict)
        # df_types = df.groupby("type")
        cols_main = ["col_name", "col_desc"]
        df_cols = (
            df[df["type"].isin(cols_main)]
            .pivot_table(
                index=id_pos, columns="type", values="content", aggfunc="first"
            )
            .reset_index()
        ).sort_values("ymin")
        df_cols.columns.name = None
        cols_dict = df_cols.to_dict("records")

        values = (
            df[df["type"].isin(["value"])]
            .drop(columns="type")
            .sort_values("ymin")
            .to_dict("records")
        )
        tol = 1
        for row in cols_dict:
            top_y, bottom_y = row["ymax"] - tol, row["ymin"] + tol
            row_values = []
            for v in values:
                top_y_v, bottom_y_v = v["ymax"], v["ymin"]
                if top_y < top_y_v and bottom_y > bottom_y_v:
                    row_values.append(v["content"])
            row["values"] = "\n".join(row_values)

        df_cols = (
            pd.DataFrame(cols_dict).sort_values("ymin").reset_index(names="id_row_page")
        )

        q_dict = (
            df[df["type"].isin(["q_desc"])]
            .drop(columns="type")
            .sort_values("ymin")
            .to_dict("records")
        )
        tol = 1
        qs = []
        for row_q in q_dict:
            top_y, bottom_y = row_q["ymax"] - tol, row_q["ymin"] + tol
            ref = df_cols.query("@top_y < ymax").query("@bottom_y > ymin")[
                "id_row_page"
            ]
            ref = ref.to_list()

            row_q["id_row_page"] = ref
            q = (
                pd.DataFrame(row_q)
                .reset_index(names="id_q")
                .explode("id_row_page")
                .drop(columns=["ymin", "ymax"])
                .rename(columns={"content": "desc_q"})
            )
            qs.append(q)
        q_df = df_cols.copy()
        if len(qs) > 0:
            pass
            # q_df = pd.concat(qs, ignore_index=True)
            # q_df = df_cols.merge(q_df)
        r = (
            pd.concat([q_df, df_meta], ignore_index=True)
            .assign(year=year, module=module, page=i + 1)
            .sort_values("ymin")
        )

        r.to_csv(out_table, index=False)
        # print(df_content)
        # Path("test/debug").mkdir(exist_ok=True)

        # save_cells_debug(page=page, cells=cells, output_path=output_file)
