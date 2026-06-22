from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class Horizontals:
    left: float
    right: float
    ys: list
    # counts: list
    # sources: list
    y_min: float
    y_max: float


def extract_horizontal_edges(page, tolerance=3):
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
                }
            )

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
        merged.append(
            {
                "y": float(np.mean([c["y"] for c in cluster])),
                "left": min(c["left"] for c in cluster),
                "right": max(c["right"] for c in cluster),
                "count": len(cluster),
            }
        )
    merged = pd.DataFrame(merged).round(10)
    return merged


def sort_vlines(horizontals, TOL=1.5):

    df = pd.DataFrame(horizontals)

    # -----------------------------
    # LEFTS
    # -----------------------------

    lefts_sorted = sorted(df["left"].unique())

    groups = []

    for l in lefts_sorted:
        placed = False

        for g in groups:
            if abs(l - g["left"]) <= TOL:
                g["values"].append(l)
                placed = True
                break

        if not placed:
            groups.append(
                {
                    "left": l,
                    "values": [l],
                }
            )

    table_lefts = [sum(g["values"]) / len(g["values"]) for g in groups]

    # -----------------------------
    # RIGHTS
    # -----------------------------

    rights_sorted = sorted(df["right"].unique())

    right_groups = []

    for r in rights_sorted:
        placed = False

        for g in right_groups:
            if abs(r - g["right"]) <= TOL:
                g["values"].append(r)
                placed = True
                break

        if not placed:
            right_groups.append(
                {
                    "right": r,
                    "values": [r],
                }
            )

    table_rights = [sum(g["values"]) / len(g["values"]) for g in right_groups]

    # -----------------------------
    # RANGOS
    # -----------------------------

    table_ranges = []

    for left in table_lefts:
        valid_rights = [r for r in table_rights if r > left]

        if not valid_rights:
            continue

        next_left_candidates = [l for l in table_lefts if l > left]

        next_left = min(next_left_candidates) if next_left_candidates else None

        if next_left:
            valid_rights = [r for r in valid_rights if r < next_left]

        if valid_rights:
            table_ranges.append(
                {
                    "left": left,
                    "right": max(valid_rights),
                }
            )

    # -----------------------------
    # DIVIDIR LINEAS
    # -----------------------------

    new_rows = []

    for _, row in df.iterrows():
        overlaps = []

        for r in table_ranges:
            if row["left"] <= r["right"] and row["right"] >= r["left"]:
                overlaps.append(r)

        for r in overlaps:
            new_row = row.copy()

            new_row["left"] = r["left"]
            new_row["right"] = r["right"]

            new_rows.append(new_row)

    result = pd.DataFrame(new_rows).round(3).sort_values("y")

    results = []

    for (left, right), g in result.groupby(
        ["left", "right"],
        sort=False,
    ):
        g = g.sort_values("y")

        ys = g["y"].tolist()

        results.append(
            Horizontals(
                left=left,
                right=right,
                ys=ys,
                y_min=min(ys),
                y_max=max(ys),
            )
        )

    return result


def extract_hlines(page):
    horizontals = extract_horizontal_edges(page, 3)

    return horizontals
