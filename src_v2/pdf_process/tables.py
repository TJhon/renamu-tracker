from dataclasses import dataclass

import pandas as pd

from src_v2.pdf_process.lines_verticals import Verticals


@dataclass
class LinesR:
    verticals: Verticals
    horizontals: list


def extract_tables_lines(
    horizontals_lines: pd.DataFrame, verticals: list[Verticals], tolerance=3
):
    groups = []

    for ver in verticals:
        # el restar hace que este mas cerca del top
        top = ver.top - tolerance
        bottom = ver.bottom + tolerance
        xmin = ver.x_min
        xmax = ver.x_max
        xcol = ver.x_col_begin

        group_df = horizontals_lines.query("y > @top and y < @bottom")

        # la linea representa todo de lado a lado de la tabla
        group_df["line_full"] = group_df.apply(
            lambda x: 1 if abs(x["left"] - xmin) < tolerance else 0, axis=1
        )
        # linea desde empieza el nombre de campo
        group_df["line_col"] = group_df.apply(
            lambda x: 1 if abs(x["left"] - xcol) < tolerance else 0, axis=1
        )

        # linea desde empieza el valor
        group_df["line_value"] = group_df.apply(
            lambda x: 1 if abs(x["left"] - ver.xs[-2]) < tolerance else 0, axis=1
        )

        groups.append(
            LinesR(
                verticals=ver, horizontals=group_df.sort_values("y").to_dict("records")
            )
        )

    return groups


@dataclass
class Celldas:
    xmin: float
    xmax: float
    ymin: float
    ymax: float
    type: str
    content: str = None
    path: str = None

    def __post_init__(self, tol=0):
        left = min(self.xmin, self.xmax) - tol
        right = max(self.xmin, self.xmax) + tol

        top = min(self.ymin, self.ymax) - tol
        bottom = max(self.ymin, self.ymax) + tol
        self.ymax = top
        self.ymin = bottom

        self.bbox = (left, top, right, bottom)


def create_cells(group: list[LinesR]):
    # si la primera linea es line_full, entonces representa toda la linea orizonar de arriba

    values_cols = []
    # --==== si representa una celda de valor

    for g_values in group:
        vlines = g_values.verticals
        left_val = vlines.xs[-1]
        right_val = vlines.xs[-2]
        hlines = g_values.horizontals

        for i, h in enumerate(hlines):
            if h["line_value"] == 1 and i > 0:
                # print(h)
                y_actual = h["y"]
                # caso 1 la linea esta debajo del valor
                prev_value = hlines[i - 1]
                y_prev_top = prev_value["y"]

                prev = Celldas(
                    xmin=left_val,
                    xmax=right_val,
                    ymin=y_prev_top,
                    ymax=y_actual,
                    type="value",
                )
                values_cols.append(prev)
                # caso 2 la linea esta arriba del valor
                # y_actual > vlines.bottom
                next_value = hlines[i + 1]
                y_next_bottom = next_value["y"]
                next = Celldas(
                    xmin=left_val,
                    xmax=right_val,
                    ymin=y_next_bottom,
                    ymax=y_actual,
                    type="value",
                )
                values_cols.append(next)

    for g_columns in group:
        vlines = g_columns.verticals
        right_desc = vlines.xs[-2]
        # linea vertical entre el nombre de columna y description
        middle = vlines.xs[-3]
        left_col = vlines.xs[-4]

        hlines_cols = [h for h in hlines if h["line_value"] != 1]

        for i, h in enumerate(hlines_cols):
            if i + 1 == len(hlines_cols):
                continue
            ytop = h["y"]
            ybottom = hlines_cols[i + 1]["y"]

            # nombre de columnas
            col = Celldas(
                xmin=left_col, xmax=middle, ymin=ytop, ymax=ybottom, type="column_name"
            )
            values_cols.append(col)
            # description de columna
            desc_col = Celldas(
                xmin=middle,
                xmax=right_desc,
                ymin=ytop,
                ymax=ybottom,
                type="column_description",
            )
            values_cols.append(desc_col)

    for g_meta in group:
        vlines_m = g_meta.verticals
        xs = vlines_m.xs
        rest_xs = len(xs) - 4
        xs_l = xs[:-3]
        bottom_y = vlines_m.bottom

        if rest_xs == 0:
            continue

        tup_meta = list(zip(xs_l, xs_l[1:]))

        hlines_meta = [h for h in hlines if h["line_full"] == 1]

        for i, h in enumerate(hlines_meta):
            for j, (x1, x2) in enumerate(tup_meta[::-1]):
                if i + 1 == len(hlines_meta):
                    continue
                ytop = h["y"]
                ybottom = hlines_meta[i + 1]["y"]

                # ante variabilidad confiaremos en el vfi que usualmente anuncia el inicio de una metadata y para el final tambien sera ello
                # nombre de columnas
                col = Celldas(
                    xmin=x1, xmax=x2, ymin=ytop, ymax=ybottom, type=f"col_meta_{j}"
                )
                values_cols.append(col)

    return values_cols
