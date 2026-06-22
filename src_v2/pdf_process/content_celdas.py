from collections import defaultdict

import numpy as np


def fill_cells_content(cells, words, tolerance=0.6):
    """
    Asigna a cada Celldas el texto contenido dentro de su bbox.

    Parameters
    ----------
    cells : list[Celldas]
    words : list[dict]
        Resultado de page.extract_words()
    tolerance : float
        Tolerancia para considerar una palabra dentro de la celda.
    """

    for cell in cells:
        left, top, right, bottom = cell.bbox

        inside_words = []

        for w in words:
            if (
                w["x0"] >= left - tolerance
                and w["x1"] <= right + tolerance
                and w["top"] >= top - tolerance
                and w["bottom"] <= bottom + tolerance
            ):
                inside_words.append(w)

        # Agrupar por línea
        lines = defaultdict(list)

        for w in inside_words:
            key = round(w["top"], 1)
            lines[key].append(w)

        text_lines = []

        for _, line_words in sorted(lines.items()):
            line_words.sort(key=lambda x: x["x0"])
            text_lines.append(";+;".join(w["text"] for w in line_words))

        cell.content = ";-;-;".join(text_lines)

    return cells


def extract_headers(page):
    lines = page.extract_text_lines()

    enriched = []

    # 1. construir métricas por línea
    for line in lines:
        chars = line.get("chars", [])

        if not chars:
            continue

        sizes = [c.get("size", 0) for c in chars if c.get("size")]
        fonts = [c.get("fontname", "") for c in chars if c.get("fontname")]

        if not sizes:
            continue

        avg_size_line = sum(sizes) / len(sizes)

        # NEGRITA REAL: debe venir del fontname
        is_bold = any("bold" in f.lower() for f in fonts)

        enriched.append(
            {
                "text": line.get("text", ""),
                "ymin": line.get("bottom"),
                "size": avg_size_line,
                "bold": is_bold,
            }
        )

    if not enriched:
        return []

    # 2. promedio global de la página
    page_avg_size = np.mean([l["size"] for l in enriched])

    # 3. filtro estricto
    result = [
        {"h2": l["text"], "ymin": float(np.round(l["ymin"], 2))}
        for l in enriched
        if l["bold"] and l["size"] > page_avg_size
    ]

    return result
