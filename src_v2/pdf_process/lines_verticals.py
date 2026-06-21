from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class Verticals:
    top: float
    bottom: float
    xs: list
    counts: list
    sources: list
    x_min: float
    x_max: float
    x_col_begin: float


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

    result = pd.DataFrame(new_rows).round(3).sort_values("top")

    results = []

    for (top, bottom), g in result.groupby(["top", "bottom"], sort=False):
        g = g.sort_values("x")

        xs = g["x"].tolist()
        counts = g["count"].tolist()

        # flatten de listas de sources
        sources = [item for sublist in g["sources"] for item in sublist]

        results.append(
            Verticals(
                top=top,
                bottom=bottom,
                xs=xs,
                counts=counts,
                sources=sources,
                x_min=min(xs),
                x_max=max(xs),
                x_col_begin=xs[-4] if len(xs) >= 4 else xs[0],
            )
        )
    return results


def extract_vlines(page):
    verticals = extract_vertical_edges(page, 3)
    verticals = sort_hlines(verticals)
    return verticals
