from ast import Dict
from dataclasses import dataclass

import pandas as pd
from rich import print

from src_v2.pdf_process.lines_verticals import Verticals

print


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
        if len(ver.xs) > 4:
            group_df["line_semi_full"] = group_df.apply(
                lambda x: 1 if abs(x["left"] - ver.xs[-6]) < tolerance else 0, axis=1
            )
            group_df["line_desc_q"] = group_df.apply(
                lambda x: 1 if abs(x["left"] - ver.xs[-5]) < tolerance else 0, axis=1
            )
        # linea desde empieza el nombre de campo
        group_df["line_col"] = group_df.apply(
            lambda x: 1 if abs(x["left"] - xcol) < tolerance else 0, axis=1
        )

        # linea desde empieza el valor
        group_df["line_value"] = group_df.apply(
            lambda x: 1 if abs(x["left"] - ver.xs[-2]) < tolerance else 0,
            axis=1,
        )

        # linea desde empieza el valor
        group_df["line_value_natural"] = group_df.apply(
            lambda x: 1 if x["line_col"] == 1 or x["line_full"] == 1 else 0,
            axis=1,
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
        self.xmin = left
        self.xmax = right

        self.bbox = (left, top, right, bottom)

    def to_dict(self):
        return {
            "ymin": self.ymin,
            "ymax": self.ymax,
            "type": self.type,
            "content": self.content,
        }


def get_list_lines(
    col_ranges_xs,
    horizontal_lines: Dict,
    top_line_table: float,
    bottom_line_table: float,
    col_type: str,
    tol_line_content: int = 5,
) -> list[Celldas]:
    results = []
    if not col_ranges_xs:
        return results

    left_v, right_v = min(col_ranges_xs), max(col_ranges_xs)

    for idx, hline in enumerate(horizontal_lines):
        top_y = hline["y"]
        # 1: no hay linea superior, pero hay espacio para contenido
        if idx == 0 and top_y - top_line_table > tol_line_content:
            actual_celdas = Celldas(
                xmin=left_v,
                xmax=right_v,
                type=col_type,
                ymin=top_y,
                ymax=top_line_table,
            )
            results.append(actual_celdas)
            # print({"no top": actual_celdas})
        # 2: no hay linea inferior pero hay espacio para contenido
        elif (
            idx + 1 == len(horizontal_lines)
            and bottom_line_table - top_y > tol_line_content
        ):
            actual_celdas = Celldas(
                xmin=left_v,
                xmax=right_v,
                type=col_type,
                ymin=top_y,
                ymax=bottom_line_table,
            )
            results.append(actual_celdas)
            # print({"no bottom": actual_celdas})
        # 3: el borde entre lineas horizontales en la columna actual

        if idx + 1 == len(horizontal_lines):
            continue
        bottom_y = horizontal_lines[idx + 1]["y"]
        actual_celdas = Celldas(
            xmin=left_v,
            xmax=right_v,
            type=col_type,
            ymin=top_y,
            ymax=bottom_y,
        )
        results.append(actual_celdas)

    return results


def create_cells_ref(group: list[LinesR]):
    values_cols = []
    cols_names_types = {
        0: "value",
        1: "col_desc",
        2: "col_name",
        3: "q_desc",
        4: "meta_2",
        5: "meta_3",
        6: "meta_4",
        7: "meta_5",
    }
    full_lines = []
    for g in group:
        lines_v = g.verticals
        # print(lines_v)
        xs = sorted(lines_v.xs)
        t_line_table, b_line_table = lines_v.top, lines_v.bottom
        lines_h = g.horizontals
        full_lines.extend(lines_h)

        cols = list(zip(xs, xs[1:]))[::-1]
        # print(cols[::-1])

        for idx_cols, limit_cols in enumerate(cols):
            type_name = cols_names_types.get(idx_cols)
            # VALUES
            if idx_cols == 0:
                h_lines = lines_h.copy()
            # columnas descripcion y nombres campo
            if idx_cols in [1, 2]:
                h_lines = lines_h.copy()
                h_lines = [h for h in lines_h if h["line_value"] != 1]
                # print(hlines)
            if idx_cols in [3, 4]:
                if len(cols) < 4:
                    continue
                h_lines = lines_h.copy()
                # print(h_lines)
                if idx_cols == 3:
                    h_lines = [
                        h
                        for h in h_lines
                        # if h["line_semi_full"] == 1
                        if h["line_full"] == 1 or h["line_desc_q"] == 1
                    ]
                else:
                    h_lines = [
                        h
                        for h in h_lines
                        if h["line_semi_full"] == 1 or h["line_full"] == 1
                    ]

            if idx_cols == len(cols) - 1:
                h_lines = lines_h.copy()
                h_lines = [h for h in lines_h if h["line_full"]]

            results = get_list_lines(
                col_ranges_xs=limit_cols,
                horizontal_lines=h_lines,
                top_line_table=t_line_table,
                bottom_line_table=b_line_table,
                col_type=type_name,
            )
            values_cols.extend(results)

    return values_cols, full_lines
