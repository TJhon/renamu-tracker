COLOR_MAP = {
    "col_name": "red",
    "col_desc": "blue",
    "meta_1": "green",
    "meta_2": "orange",
    "meta_3": "purple",
    "meta_4": "darkblue",
    "meta_5": "pink",
    "meta_6": "black",
    "value": "yellow",
}


def save_cells_debug(page, cells, output_path):
    """
    Guarda una imagen de la página con las celdas dibujadas.

    Parameters
    ----------
    page : pdfplumber.Page
    cells : list[Celldas]
    output_path : str | Path
    """

    im = page.to_image(resolution=150)

    for cell in cells:
        color = COLOR_MAP.get(cell.type, "black")
        # print(cell)
        bbox = (
            cell.xmin,
            cell.ymax,  # top
            cell.xmax,
            cell.ymin,  # bottom
        )

        im.draw_rect(bbox, stroke=color, stroke_width=2, fill=None)

    im.save(output_path)
