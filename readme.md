# RENAMU Dictionary Tracker

Pipeline para unificar y armonizar las encuestas **RENAMU** (Registro Nacional de Municipalidades del Perú) de múltiples años. La estructura de columnas cambia entre años: variables migran entre cuadros, cambian de nombre, se agregan o desagregan. El objetivo es un diccionario maestro longitudinal que permita comparar variables a lo largo del tiempo.

> Publicación completa: [LinkedIn](https://www.linkedin.com/in/tjhon/)

---

## Resultados actuales

- **1,279 páginas** de diccionarios PDF procesadas con visión por computadora
- **~27,000 filas** de metadata estructurada extraídas
- **~30 MB** de datos en Parquet (archivos .dbf, .csv y .dta)
- **71 %** de columnas matchean correctamente entre diccionario y archivos binarios
- El 29 % restante tiene errores tipográficos en los propios archivos del INEI o cambios de nomenclatura entre versiones del cuestionario
- Costo total de API (~Qwen/Dashscope): **~45 USD**

---

## Variables de entorno requeridas

```env
DATA_ROOT=/ruta/a/data/renamu
OUTPUT_ROOT=/ruta/a/output
QWEN_API_KEY=...
QWEN_MODEL=qwen-vl-max      # modelo multimodal para extracción de tablas
QWEN_URL=...                # base URL de Dashscope (opcional si usas el endpoint por defecto)
```

---

## Estructura de carpetas esperada en `DATA_ROOT`

```
{DATA_ROOT}/
  {year}/                        # ej: 2004, 2008, ..., 2025
    {Modulo_n}/                  # solo existe para años <= 2020
      cuadro_n.dbf
      Diccionario del modulo n.pdf
    cuadro_n.dta                 # solo para años > 2020
    Diccionario del modulo n.pdf
```

---

## Estructura de salida en `OUTPUT_ROOT`

```
{OUTPUT_ROOT}/
  parquets/
    {year}/
      c{cuadro_n}.parquet        # un parquet por cuadro, con columna id_jk estandarizada
  pdf_img/
    {year}/
      {year}_{modulo}_page_{n}.jpg
  llm_table_parser/
    json_parser/{year}/{stem}.json
    df_parser/{year}/{stem}.csv
  clasification/
    main.db                      # SQLite: tablas cuadros, pdf_rows, renamu_variables
    chroma_db/                   # índice vectorial ChromaDB (embeddings locales)
```

---

## Base de datos SQLite (`main.db`)

El proyecto usa **SQLite** (no PostgreSQL) como fuente de verdad persistente.

```sql
-- Columnas de archivos binarios escaneados (una fila por columna por cuadro por año)
CREATE TABLE cuadros (
    year    TEXT,
    cuadro  TEXT,
    column  TEXT,
    modulo  TEXT
);

-- Filas extraídas de los diccionarios PDF por Qwen
CREATE TABLE pdf_rows (
    year        TEXT,
    modulo      TEXT,
    page        TEXT,
    cuadro      TEXT,
    n_q         TEXT,
    desc_q      TEXT,
    col         TEXT,    -- nombre del campo en el .dbf/.dta
    desc_col    TEXT,
    values      TEXT,
    value_pos   TEXT,    -- valor que representa "sí/positivo" en variables dicotómicas
    h2          TEXT     -- encabezado de sección cuando no hay campo asociado
);

-- Variables unificadas con categoría/subcategoría y tracking longitudinal
CREATE TABLE renamu_variables (
    year                  TEXT,
    col                   TEXT,
    desc_columna          TEXT,
    desc_cuadro_pregunta  TEXT,
    categoria             TEXT,
    subcategoria          TEXT,
    ...
);

-- Mapeo longitudinal final
CREATE TABLE column_mapping (
    year             INTEGER,
    col              TEXT,
    desc_col         TEXT,
    master_id        TEXT,    -- identificador canónico del año 2025
    match_type       TEXT,    -- 'exact', 'embedding_auto', 'llm', 'new'
    similarity_score REAL,
    ...
);
```

---

## Fases del pipeline

### Fase 1 — Escanear y convertir archivos de datos (`phase1_scan.py`)

Lee todos los `.dbf` y `.dta`, normaliza IDs y guarda parquets.

- Detecta `year` y `cuadro_n` desde la estructura de carpetas
- Reanudable: si el parquet ya existe → skip
- Normaliza nombres de columnas a minúsculas
- Construye columna `id_jk` = `ccdd(zfill 2)` + `ccpp(zfill 2)` + `ccdi(zfill 2)`
- Registra metadata en tabla `cuadros` (SQLite)

```bash
python -m src.phase1_scan
```

---

### Fase 2a — PDF a imágenes (`phase2a_pdf_to_img.py`)

Convierte cada página de cada diccionario PDF a un JPEG usando **PyMuPDF** (`fitz`).

- DPI: 150
- Reanudable: si la imagen ya existe → skip
- Salida: `{OUTPUT_ROOT}/pdf_img/{year}/{year}_{modulo}_page_{n}.jpg`

```bash
python -m src.phase2a_pdf_to_img
```

---

### Fase 2b — Imágenes a metadata con LLM (`phase2b_img_to_llm.py`)

Envía cada imagen a **Qwen** (Dashscope, modelo multimodal) para extraer la tabla del diccionario.

El prompt solicita los campos: `cuadro`, `n_q`, `desc_q`, `col`, `desc_col`, `values`, `value_pos`.  
Para variables dicotómicas (2 valores), solo extrae el valor numérico positivo (`value_pos`).

- Reanudable: si el JSON de salida ya existe → skip
- Manejo de errores: `time.sleep(60)` ante excepciones, continúa con la siguiente imagen
- Resultados intermedios: JSON por imagen + CSV agregado

```bash
python -m src.phase2b_img_to_llm
```

---

### Fase 3a — Limpieza y normalización del info DB (`phase3a_clean_infodb.py`)

Filtra columnas irrelevantes del registro `cuadros` (columnas de ubigeo, orden, flags de validación, RUC, etc.) y prepara `renamu_variables` para la clasificación.

Ejecutar como script de Python directamente o importar sus funciones.

---

### Fase 3a — Clasificación temática con Qwen (`phase3a_ref_classification.py`)

Clasifica cada variable en **categoría** y **subcategoría** temática usando Qwen vía Dashscope.

Tres prompts encadenados:
1. **Batch inicial** (`PROMPT_CLASSIFY_BATCH`): clasifica lotes de hasta 80 descripciones del año 2025 de una sola vez
2. **Categoría incremental** (`PROMPT_CLASSIFY_CATEGORIA`): para años anteriores, decide si usar una categoría existente o crear una nueva
3. **Subcategoría** (`PROMPT_CLASSIFY_SUBCATEGORIA`): dentro de la categoría asignada, reutiliza subcategorías existentes o crea una nueva

Helpers reutilizables en `utils.py`: `get_unclassified_by_year`, `get_existing_categories`, `get_existing_subcategories_by_category`, `update_classification`.

---

### Fase 3 — Tracking semántico longitudinal (`phase4_classification.py`)

Construye el mapeo entre variables de distintos años usando **embeddings locales + LLM local**.

Tecnologías:
- **ChromaDB** como índice vectorial persistente
- **Ollama** con modelo `nomic-embed-text` para embeddings
- **Ollama** con modelo `qwen2.5:9b` (o similar) para ranking final cuando el embedding no es concluyente

Flujo por variable:
1. Año de referencia (2025) → indexado en ChromaDB
2. Para cada año anterior: busca coincidencia exacta → si no, consulta embedding (top-5 candidatos)
3. Si `similarity_score >= 0.92` → match automático sin LLM
4. Si no → LLM decide entre los candidatos o marca como variable nueva
5. Resultado guardado en tabla `column_mapping` (SQLite)

```
SIMILARITY_AUTO_THRESHOLD = 0.92
TOP_K = 5
MASTER_YEAR = 2025
```

---

### Fase 4a — Clasificación final y propagación (`phase4a.py`)

Propaga categorías asignadas en años anteriores hacia variables aún sin clasificar, usando `desc_cuadro_pregunta` y `desc_columna` como pivotes.


