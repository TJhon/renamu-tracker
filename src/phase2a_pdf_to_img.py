import re
from pathlib import Path

import fitz  # PyMuPDF
import tqdm
from rich import print

from .config import DATA_ROOT as BASE_DIR
from .config import OUTPUT_ROOT

IMAGES_DIR = OUTPUT_ROOT / "pdf_img"

YEAR_RE = re.compile(r"^\d{4}$")
MOD_RE = re.compile(r"Modulo(\d+)", re.IGNORECASE)


def process_pdf(pdf_path: Path, year: str, module: str = ""):
    """Convierte cada página de un PDF a una imagen independiente usando PyMuPDF."""
    dir_image = IMAGES_DIR / year
    dir_image.mkdir(parents=True, exist_ok=True)
    if module is not None:
        image_name = f"{year}_{module}_page"
    else:
        image_name = f"{year}_page"
    try:
        doc = fitz.open(pdf_path)
        for i, page in tqdm.tqdm(enumerate(doc)):
            page_num = i + 1
            image_name_path = f"{image_name}_{page_num}.jpg"
            image_path = dir_image / image_name_path
            if not image_path.exists():
                pix = page.get_pixmap(dpi=150)
                pix.save(image_path)
        doc.close()
    except Exception as e:
        print(f"❌ Error convirtiendo a imágenes {pdf_path}: {e}")


def extract_year_module(path: Path):
    year = None
    module = None

    parent = path.parent

    # detectar año
    if m := YEAR_RE.match(parent.stem):
        year = m.group()

    # detectar modulo
    if m := MOD_RE.search(parent.stem):
        module = m.group(1)

        # el año está en la carpeta superior
        if y := YEAR_RE.match(parent.parent.stem):
            year = y.group()

    return year, module


def run_extraction():
    """Rastrea el directorio BASE_DIR y extrae PDFs recursivamente basándose en las reglas de año."""

    pdfs_path = BASE_DIR.rglob("*.pdf")

    for pdf in pdfs_path:
        year, mod = extract_year_module(pdf)
        # print(dict(pdf=pdf, year=year, mod=mod))
        process_pdf(pdf, str(year), mod)


if __name__ == "__main__":
    run_extraction()
