import re
from pathlib import Path

import numpy as np
import pandas as pd
from rich import print

print
YEAR_RE = re.compile(r"^\d{4}$")
MOD_RE = re.compile(r"Modulo(\d+)", re.IGNORECASE)


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


def extract_vertical_edges(page, tolerance=3):
    """
    Extrae líneas verticales desde:
    - lines
    - rects
    - curves

    y fusiona coordenadas X cercanas.
    """

    verticals = []
    # =========
    # LINES
    # =========
    for obj in page.lines:
        x0 = obj["x0"]
        x1 = obj["x1"]

        y0 = obj["top"]
        y1 = obj["bottom"]

        # casi vertical
        if abs(x1 - x0) < 1:
            verticals.append(
                {
                    "x": (x0 + x1) / 2,
                    "top": min(y0, y1),
                    "bottom": max(y0, y1),
                    "source": "line",
                }
            )

    # =========
    # RECTS
    # =========
    for obj in page.rects:
        width = obj["width"]
        height = obj["height"]
        # rectángulo vertical delgado
        if width < 2 and height > 5:
            x = (obj["x0"] + obj["x1"]) / 2

            verticals.append(
                {"x": x, "top": obj["top"], "bottom": obj["bottom"], "source": "rect"}
            )

    # =========
    # CURVES
    # =========
    for obj in page.curves:
        width = obj["width"]
        height = obj["height"]

        # curve vertical delgada
        if width < 2 and height > 5:
            x = (obj["x0"] + obj["x1"]) / 2

            verticals.append(
                {"x": x, "top": obj["top"], "bottom": obj["bottom"], "source": "curve"}
            )

    # =========================
    # MERGE X CERCANAS
    # =========================

    clusters = []

    for v in sorted(verticals, key=lambda z: z["x"]):
        if not clusters:
            clusters.append([v])
            continue

        prev_x = np.mean([k["x"] for k in clusters[-1]])

        if abs(v["x"] - prev_x) <= tolerance:
            clusters[-1].append(v)
        else:
            clusters.append([v])

    merged = []

    for cluster in clusters:
        merged.append(
            {
                "x": float(np.mean([c["x"] for c in cluster])),
                "top": min(c["top"] for c in cluster),
                "bottom": max(c["bottom"] for c in cluster),
                "count": len(cluster),
                "sources": list(set(c["source"] for c in cluster)),
            }
        )

    return merged


def sort_hlines(verticals, TOL=1.5):
    # ---------------------------------------------------
    # 1. Detectar tops agrupados con tolerancia
    # ---------------------------------------------------
    df = pd.DataFrame(verticals)
    tops_sorted = sorted(df["top"].unique())

    groups = []

    for t in tops_sorted:
        placed = False

        for g in groups:
            if abs(t - g["top"]) <= TOL:
                g["values"].append(t)
                placed = True
                break

        if not placed:
            groups.append({"top": t, "values": [t]})

    # top representativo
    table_tops = [sum(g["values"]) / len(g["values"]) for g in groups]

    # ---------------------------------------------------
    # 2. Detectar bottoms agrupados
    # ---------------------------------------------------

    bottoms_sorted = sorted(df["bottom"].unique())

    bottom_groups = []

    for b in bottoms_sorted:
        placed = False

        for g in bottom_groups:
            if abs(b - g["bottom"]) <= TOL:
                g["values"].append(b)
                placed = True
                break

        if not placed:
            bottom_groups.append({"bottom": b, "values": [b]})

    table_bottoms = [sum(g["values"]) / len(g["values"]) for g in bottom_groups]

    # ---------------------------------------------------
    # 3. Construir tablas válidas
    #    (top -> siguiente bottom antes del próximo top)
    # ---------------------------------------------------

    table_ranges = []

    for top in table_tops:
        valid_bottoms = [b for b in table_bottoms if b > top]

        if not valid_bottoms:
            continue

        next_top_candidates = [t for t in table_tops if t > top]

        next_top = min(next_top_candidates) if next_top_candidates else None

        if next_top:
            valid_bottoms = [b for b in valid_bottoms if b < next_top]

        if valid_bottoms:
            table_ranges.append({"top": top, "bottom": max(valid_bottoms)})

    # ---------------------------------------------------
    # 4. Dividir líneas que cruzan varias tablas
    # ---------------------------------------------------

    new_rows = []

    for _, row in df.iterrows():
        overlaps = []

        for r in table_ranges:
            # intersección vertical
            if row["top"] <= r["bottom"] and row["bottom"] >= r["top"]:
                overlaps.append(r)

        # si cruza varias tablas -> partir
        for r in overlaps:
            new_row = row.copy()

            new_row["top"] = r["top"]
            new_row["bottom"] = r["bottom"]

            new_rows.append(new_row)

    result = pd.DataFrame(new_rows)

    return result.to_dict("records")


# print(result.sort_values(["top", "x"]))


def extract_horizontal_edges(page, verticals, tolerance=3):
    """
    Extrae líneas horizontales desde:
    - lines
    - rects
    - curves

    Extiende cada línea horizontal para que vaya desde la primera
    vertical relevante hasta la última vertical relevante.
    Fusiona coordenadas Y cercanas.

    Parameters
    ----------
    page       : pdfplumber page
    verticals  : lista de dicts con keys 'x', 'top', 'bottom'
                 (salida de extract_vertical_edges / sort_hlines)
    tolerance  : tolerancia en puntos para agrupar Y cercanas
    """

    if not verticals:
        return []

    x_min = min(v["x"] for v in verticals)
    x_max = max(v["x"] for v in verticals)

    horizontals = []

    # =========
    # LINES
    # =========
    for obj in page.lines:
        x0, x1 = obj["x0"], obj["x1"]
        y0, y1 = obj["top"], obj["bottom"]

        # casi horizontal
        if abs(y1 - y0) < 1:
            horizontals.append(
                {
                    "y": (y0 + y1) / 2,
                    "left": min(x0, x1),
                    "right": max(x0, x1),
                    "source": "line",
                }
            )

    # =========
    # RECTS
    # =========
    for obj in page.rects:
        width = obj["width"]
        height = obj["height"]

        # rectángulo horizontal delgado
        if height < 2 and width > 5:
            y = (obj["top"] + obj["bottom"]) / 2
            horizontals.append(
                {
                    "y": y,
                    "left": obj["x0"],
                    "right": obj["x1"],
                    "source": "rect",
                }
            )

    # =========
    # CURVES
    # =========
    for obj in page.curves:
        width = obj["width"]
        height = obj["height"]

        # curve horizontal delgada
        if height < 2 and width > 5:
            y = (obj["top"] + obj["bottom"]) / 2
            horizontals.append(
                {
                    "y": y,
                    "left": obj["x0"],
                    "right": obj["x1"],
                    "source": "curve",
                }
            )

    # =========================
    # MERGE Y CERCANAS
    # =========================
    # Tolerancia más grande que en verticales porque pdfplumber
    # tiene más imprecisión en alineación horizontal de segmentos

    clusters = []

    for h in sorted(horizontals, key=lambda z: z["y"]):
        if not clusters:
            clusters.append([h])
            continue

        prev_y = np.mean([k["y"] for k in clusters[-1]])

        if abs(h["y"] - prev_y) <= tolerance:
            clusters[-1].append(h)
        else:
            clusters.append([h])

    merged = []

    for cluster in clusters:
        raw_left = min(c["left"] for c in cluster)
        raw_right = max(c["right"] for c in cluster)

        # -------------------------------------------------------
        # Extender hasta las columnas verticales relevantes:
        # - left  → primera vertical cuya x >= raw_left  - tol
        # - right → última  vertical cuya x <= raw_right + tol
        # Si ninguna encaja, usar x_min / x_max globales
        # -------------------------------------------------------
        snap_tol = tolerance * 4  # margen de snap a columna

        left_candidates = [v["x"] for v in verticals if v["x"] >= raw_left - snap_tol]
        right_candidates = [v["x"] for v in verticals if v["x"] <= raw_right + snap_tol]

        snapped_left = min(left_candidates) if left_candidates else x_min
        snapped_right = max(right_candidates) if right_candidates else x_max

        merged.append(
            {
                "y": float(np.mean([c["y"] for c in cluster])),
                "left": snapped_left,
                "right": snapped_right,
                "count": len(cluster),
                "sources": list(set(c["source"] for c in cluster)),
            }
        )

    return merged


def extract_table_content(page, verticals, horizontals, snap_tol=5):
    """
    Extrae el contenido de las últimas 4 columnas de la tabla.

    Columnas (de izquierda a derecha desde la 4ta vertical del final):
        col_left | nombre_campo | desc_campo | valores

    Retorna lista de dicts con dos tipos de filas:
      - full_hline:  { 'type': 'hline', 'ypos': float, 'full_hline': True }
      - data row:    { 'type': 'row',
                       'ymaxtop': float, 'yminbottom': float,
                       'nombre_campo': str|None,
                       'desc_campo':   str|None,
                       'valores':      str|None }
    """
    if not verticals or not horizontals:
        return []

    # ------------------------------------------------------------------
    # 1. Ordenar verticales por x y tomar las últimas 4
    # ------------------------------------------------------------------
    verts_sorted = sorted(verticals, key=lambda v: v["x"])

    # necesitamos al menos 4 columnas → 4 bordes verticales (3 columnas útiles)
    # layout:  | col_extra | nombre_campo | desc_campo | valores |
    #  índices:     -4           -3            -2           -1
    if len(verts_sorted) < 4:
        raise ValueError(f"Se necesitan al menos 4 verticales, hay {len(verts_sorted)}")

    x_left = verts_sorted[-4]["x"]  # borde izq de nombre_campo
    x_nc = verts_sorted[-3]["x"]  # borde der de nombre_campo / izq desc_campo
    x_dc = verts_sorted[-2]["x"]  # borde der de desc_campo  / izq valores
    x_right = verts_sorted[-1]["x"]  # borde der de valores

    # rango vertical cubierto por las verticales
    y_top_global = min(v["top"] for v in verts_sorted)
    y_bottom_global = max(v["bottom"] for v in verts_sorted)

    # ------------------------------------------------------------------
    # 2. Filtrar horizontales relevantes:
    #    - dentro del rango vertical de las verticales
    #    - que su left esté cerca de x_left (full_hline candidate)
    #      O que su left esté cerca de x_nc  (línea interna de sub-filas)
    # ------------------------------------------------------------------
    def near(a, b, tol=snap_tol):
        return abs(a - b) <= tol

    relevant_h = []
    for h in sorted(horizontals, key=lambda z: z["y"]):
        y = h["y"]
        if y < y_top_global - snap_tol or y > y_bottom_global + snap_tol:
            continue

        is_full = near(h["left"], x_left) and near(h["right"], x_right)
        # línea que al menos llega desde x_nc hasta x_right
        is_inner = (h["left"] <= x_nc + snap_tol) and near(h["right"], x_right)

        if is_full or is_inner:
            relevant_h.append({**h, "full_hline": is_full})

    if not relevant_h:
        return []

    # ------------------------------------------------------------------
    # 3. Construir la lista de Y-límites (intervalos entre horizontales)
    # ------------------------------------------------------------------
    y_positions = [h["y"] for h in relevant_h]

    # ------------------------------------------------------------------
    # 4. Helper: extraer texto de un rectángulo de la página
    # ------------------------------------------------------------------
    def crop_text(x0, top, x1, bottom):
        margin = 1
        try:
            cropped = page.crop(
                (
                    x0 + margin,
                    top + margin,
                    x1 - margin,
                    bottom - margin,
                )
            )
            txt = cropped.extract_text(x_tolerance=3, y_tolerance=3)
            return txt.strip() if txt else None
        except Exception:
            return None

    # ------------------------------------------------------------------
    # 5. Construir rows
    # ------------------------------------------------------------------
    rows = []

    for i, h in enumerate(relevant_h):
        # ── full_hline row ──────────────────────────────────────────
        if h["full_hline"]:
            rows.append(
                {
                    "type": "hline",
                    "ypos": h["y"],
                    "full_hline": True,
                }
            )

        # ── data row entre esta hline y la siguiente ────────────────
        if i + 1 < len(relevant_h):
            y0 = h["y"]
            y1 = relevant_h[i + 1]["y"]
        else:
            # última hline → llegar hasta el fondo de las verticales
            y0 = h["y"]
            y1 = y_bottom_global

        if y1 - y0 < 1:  # intervalo vacío → saltar
            continue

        nombre_campo = crop_text(x_left, y0, x_nc, y1)
        desc_campo = crop_text(x_nc, y0, x_dc, y1)
        valores = crop_text(x_dc, y0, x_right, y1)

        # saltar filas completamente vacías
        if not any([nombre_campo, desc_campo, valores]):
            continue

        rows.append(
            {
                "type": "row",
                "ymaxtop": y0,
                "yminbottom": y1,
                "nombre_campo": nombre_campo,
                "desc_campo": desc_campo,
                "valores": valores,
            }
        )

    return rows
