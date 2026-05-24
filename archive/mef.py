"""PROPUESTA DE UTILIZACION DEL NUEVO CODIGO - API NO OFICIAL DEL MEF"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

# ──────────────────────────────────────────────────────────────────
# 1. BOTONES
# ──────────────────────────────────────────────────────────────────


class Button(str, Enum):
    """
    se utiliza los id de los botones y se actualiza automaticamente el valor
    """

    FUNCION = "Función"
    CATEGORIA_PRESUP = "Categoría Presupuestal"
    GOB_LOCALES = "Gobiernos Locales"
    GOB_LOC_MANCOM = "Gob.Loc/Mancom"
    MUNICIPALIDADES = "Municipalidades"
    DEPARTAMENTO = "Departamento"
    PROVINCIA = "Provincia"
    DISTRITO = "Distrito"


# ──────────────────────────────────────────────────────────────────
# 3. POLÍTICA ANTE DATOS FALTANTES
# ──────────────────────────────────────────────────────────────────


class OnMissing(str, Enum):
    SKIP = "skip"
    RECORD = "record"  # ← default recomendado
    RAISE = "raise"


# ──────────────────────────────────────────────────────────────────
# 4. STEPS
# ──────────────────────────────────────────────────────────────────


@dataclass
class ClickRow:
    """
    Selecciona una fila haciendo click directo.

    ClickRow([])
    []: corre todo lo que encuentra
    ['salud', 'educacion', '123']: Buscara dentro de la tabla resultante solo las filas que tengan algunos de estos valores dentro de sus filas es un "|"
        '

    """

    rows: list[str] = field(default_factory=list)
    on_missing: OnMissing = OnMissing.RECORD


@dataclass
class Search:
    """
    Simula una busqueda usando su busqueda y metodo de busqueda del mef
    Y actualiza la tabla con las coincidencias que encuentra el backend del mef
    """

    query: str | None = None
    method: str = "description" | "code"
    on_missing: OnMissing = OnMissing.RECORD


@dataclass
class ClickBtn:
    """Click en un botón del sistema MEF."""

    button: Button


@dataclass
class SavePartial:
    """
    Guarda los datos hasta este punto del workflow.
    Puede aparecer múltiples veces en la lista de steps.

    La cache usa (año + índice_del_SavePartial + button_value_anterior)
    como clave, de modo que un rerun saltea exactamente hasta donde
    se guardó, sin reejecutar los pasos previos.

    filename_prefix:
        Prefijo del archivo de salida.
        None → se genera automáticamente desde el button y el valor del loop.

    Ejemplo de nombre generado:
        "2015__step2__DEPARTAMENTO__Junín.csv"
    """

    filename_prefix: str | None = None


# Tipo alias — lo que puede ir en la lista de steps
Step = ClickRow | Search | ClickBtn | SavePartial


# ──────────────────────────────────────────────────────────────────
# 5. RESULTADOS Y ERRORES
# ──────────────────────────────────────────────────────────────────


@dataclass
class Table:
    year: int
    metadata: dict[str, str]
    rows: list[dict[str, Any]]
    saved_at: Path | None = None


@dataclass
class ScrapingError:
    year: int
    step_index: int
    row_value: str
    reason: str
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass
class ResultSet:
    tables: list[Table] = field(default_factory=list)
    errors: list[ScrapingError] = field(default_factory=list)

    def to_dataframe(self): ...
    def to_csv(self, path: str | Path) -> None: ...


# ──────────────────────────────────────────────────────────────────
# 6. CACHE  (SQLite)
#
# Schema sugerido:
#   CREATE TABLE done (
#       year         INTEGER,
#       save_index   INTEGER,   -- qué SavePartial (0, 1, 2...)
#       button_value TEXT,      -- valor del ClickBtn anterior al SavePartial
#       loop_value   TEXT,      -- valor de fila / search actual
#       done_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#       PRIMARY KEY (year, save_index, button_value, loop_value)
#   );
#   CREATE TABLE errors (
#       year TEXT, step INTEGER, row_value TEXT, reason TEXT, ts TIMESTAMP
#   );
# ──────────────────────────────────────────────────────────────────


class CacheManager:
    def __init__(self, db_path: str | Path = ".mef_cache.db"):
        self.db_path = Path(db_path)

    def cache_key(
        self,
        year: int,
        save_index: int,  # índice del SavePartial en la lista de steps
        button_value: str,  # Button.value del ClickBtn justo antes del SavePartial
        loop_value: str,  # valor de fila/search que se está procesando
    ) -> str:
        return f"{year}|save{save_index}|{button_value}|{loop_value}"

    def is_done(self, key: str) -> bool: ...
    def mark_done(self, key: str) -> None: ...
    def record_error(self, key: str, error: ScrapingError) -> None: ...
    def get_errors(self) -> list[ScrapingError]: ...
    def clear(self) -> None: ...


# ──────────────────────────────────────────────────────────────────
# 7. WORKFLOW
# ──────────────────────────────────────────────────────────────────


class Workflow:
    """
    Parameters
    ----------
    steps:
        Lista plana de ClickRow / Search / ClickBtn / SavePartial.
        Los pares ClickRow + ClickBtn son la unidad de navegación.
        SavePartial puede aparecer después de cualquier ClickBtn.

    years:
        range(2008, 2024) o lista explícita [2015, 2018, 2020].

    output_dir:
        Carpeta raíz donde se guardan los archivos de SavePartial.

    cache:
        CacheManager opcional. Si se provee, los SavePartial ya
        completados se saltean en reruns.
    """

    def __init__(
        self,
        steps: list[Step],
        years: range | list[int],
        output_dir: str | Path = "./data",
        cache: CacheManager | None = f"{output_dir}/mef.db",
    ):
        self.steps = steps
        self.years = list(years)
        self.output_dir = Path(output_dir)
        self.cache = cache

    def run(self) -> ResultSet:
        """
        Este deberia ser capaz de detectar si ya se corrioo o no algunos loops para y para esto esta el SavePartial si existe entonces se omite ese loop

        """
        ...


# ══════════════════════════════════════════════════════════════════
# EJEMPLOS DE USO
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    cache = CacheManager("data/mef.db")

    # ── Workflow pre-2012 ─────────────────────────────────────────────────

    steps_pre_2012 = [
        ClickRow(["total"]),
        ClickBtn(Button.FUNCION),
        ClickRow(["Salud", "Educación"]),
        ClickBtn(Button.CATEGORIA_PRESUP),
        ClickRow(),
        ClickBtn(Button.GOB_LOCALES),
        ClickRow(),
        ClickBtn(Button.DEPARTAMENTO),
    ]

    workflow_pre = Workflow(
        steps=steps_pre_2012,
        years=range(2008, 2012),
        output_dir="./data/pre2012",
        cache=cache,
    )

    df: pd.DataFrame = workflow_pre.run()

    # ── Workflow post-2012 ────────────────────────────────────────────────

    steps_post_2012 = [
        ClickRow(["total"]),
        ClickBtn(Button.FUNCION),
        ClickRow(["Salud", "Educación"]),
        ClickBtn(Button.CATEGORIA_PRESUP),
        ClickRow(),
        ClickBtn(Button.GOB_LOCALES),
        ClickRow(),
        ClickBtn(Button.GOB_LOC_MANCOM),
        ClickRow(),
        ClickBtn(Button.MUNICIPALIDADES),
        SavePartial(),  # guarda nivel municipio (save0)
        ClickRow(),
        ClickBtn(Button.DEPARTAMENTO),
        SavePartial(),  # guarda nivel departamento (save1)
    ]

    workflow_post = Workflow(
        steps=steps_post_2012,
        years=range(2012, 2024),
        cache=cache,
    )

    df: pd.DataFrame = workflow_post.run()

    # ── Workflow con Search (lista larga de municipios) ───────────────────

    steps_search = [
        ClickRow(["Total"]),
        ClickBtn(Button.GENERICA),
        ClickRow(["Salud"]),
        ClickBtn(Button.FUNCION),
        ClickRow(),
        ClickBtn(Button.CATEGORIA_PRESUP),
        ClickRow(),
        ClickBtn(Button.GOB_LOCALES),
        ClickRow(),
        ClickBtn(Button.GOB_LOC_MANCOM),
        ClickBtn(Button.MUNICIPALIDADES),
        SavePartial(
            filename_prefix="muni"
        ),  # "muni__2015__save0__MUNICIPALIDADES__Huancayo.csv"
        Search(
            method="description",  # búsqueda por código, valores fijos
            value="vaso de leche",
        ),
        ClickBtn(Button.DEPARTAMENTO),
        SavePartial(filename_prefix="dept"),
    ]

    workflow_search = Workflow(
        steps=steps_search,
        years=range(2015, 2024),
        output_dir="./data/search",
        cache=cache,
    )

    # Inspeccionar errores
    for err in cache.get_errors():
        print(f"[{err.year}] paso {err.step_index} | {err.row_value!r} → {err.reason}")
