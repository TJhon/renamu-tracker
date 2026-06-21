"""
Docstring for src_v2.phs1_pdf
Extraer el contenido de los pdf

por el momento ya tenemos las lineas verticales que definen a las columnas como tambien las coordenadas de inicio de tabla y final de tabla vertical.

propuestas siguientes
- obtener las lineas horizontales
- tomar el ultimo par de coordenadas para cada 'tabla' y ver entre linea superior e inferior y dentro de los margenes de la tabla hay texto
    - si no hay texto entonces 'dibujar la linea hasta la 4 x de derecha a izquierda (de la tabla) luego extraer el contenido del nombre de columnas y descripcion podemos estar casi 100% seguros que hace referencia a su descripcion y column ya que no es una variable categorica
    - mover esto a una tabla independiente, y limpiar el texto de descripcion de columna  y las variables categoricas dejar con x pero con su metadata de texto
    - luego para las variables categoricas solo
    - y de lo anterior reemplazar el texto con 'x' para posterior debuging



"""

import pandas as pd
import pdfplumber
from rich import print

from src_v2.config import DATA_ROOT, OUTPUT_ROOT
from src_v2.pdf_process.content_celdas import (
    extract_large_text_lines,
    fill_cells_content,
)
from src_v2.pdf_process.lines_horizontals import extract_hlines
from src_v2.pdf_process.lines_verticals import extract_vlines
from src_v2.pdf_process.tables import create_cells, extract_tables_lines
from src_v2.utils import (
    extract_year_module,
)

pd
test_save = OUTPUT_ROOT / "test"
test_save.mkdir(exist_ok=True, parents=True)
print

pdf_paths = list(DATA_ROOT.rglob("*.pdf"))


for pdf in pdf_paths:
    year, module = extract_year_module(pdf)
    pdf_open = pdfplumber.open(pdf)
    i = 1

    for page in pdf_open.pages[:5]:
        # if year != "2013":
        #     continue
        # if module != "86":
        #     continue
        if i != 0:
            lines = extract_large_text_lines(page)
            # s = [{k: v for k, v in d.items() if k != "chars"} for d in s]
            print(lines)

        verticals = extract_vlines(page)
        if len(verticals) > 0:
            hori = extract_hlines(page)
            tables = extract_tables_lines(hori, verticals)
            # print({"pdf": pdf, "page": i, "tables": tables})
            r = create_cells(tables)
            words = page.extract_words()

            cells = fill_cells_content(r, words, 0.6)
            # print(cells)

        # for j, _r in enumerate(r):
        #     # print(_r)
        #     c = page.crop(_r.bbox)
        #     im = c.to_image()
        #     im.save(f"test/page_{i}_{j}.png", bits=780)
        #     content = c.extract_text()
        #     _r.content = content
        #     _r.path = f"test/page_{i}_{j}.png"

        # print(r)
        # if i == 5:
        #     s = page.extract_words()
        # s = [{k: v for k, v in d.items() if k != "chars"} for d in s]
        # print(s[-30:])
        # # if len(hori) > 1:
        # print({"pdf": pdf, "ho": hori})

        # content = extract_table_content(page, verticals, horizontals, snap_tol=5)

        # for row in content:
        #     row["page"] = i
        #     row["year"] = year
        #     row["module"] = module
        #     # print(row)

        i += 1

# dc = fitz.open(
#     r"E:\All\carlos\data\renamu\2007\207-Modulo87\Diccionario Datos Modulo87_2007.pdf"
# )

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
