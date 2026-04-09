
# RENAMU Dictionary Tracker — Prompt para agente

## Contexto del proyecto

Eres un agente de automatización de datos que debe construir un pipeline para unificar y armonizar las encuestas RENAMU (Registro Nacional de Municipalidades del Perú) de múltiples años. La estructura de columnas cambia entre años: variables migran entre cuadros, cambian de nombre, se agregan o desagregan. Tu objetivo es crear un mapeo longitudinal que permita comparar variables a lo largo del tiempo.

**Este proyecto se ejecuta de forma incremental (sin paralelismo), paso a paso, con recursos limitados.** Cada fase debe poder reanudarse desde donde se interrumpió usando PostgreSQL como fuente de verdad persistente.

---

## Variables de entorno requeridas

```env
DATA_ROOT=E:/All/carlos/data/renamu
OUTPUT_ROOT=E:\All\temp_borrar\antigraviti\renamu_diccionario_tracker\output
PG_HOST=localhost
PG_PORT=5432
PG_DB=renamu
PG_USER=postgres
PG_PASSWORD=postgres
GEMINI_API_KEY=...
ANTHROPIC_API_KEY=...
```

---

## Estructura de carpetas esperada en `DATA_ROOT`

```
{DATA_ROOT}/
  {year}/                        # ej: 2004, 2008, ..., 2025
    {modulo_n}/                  # solo existe para años <= 2020
      cuadro_n.dbf
      diccionario del modulo n.pdf
    cuadro_n.dta                 # solo para años > 2020 (un archivo por cuadro o uno total)
    diccionario del modulo n.pdf # solo para años > 2020
```

**Reglas de interpretación:**
- Si solo existe un archivo de datos en un año → ese cuadro recibe número `1` por defecto.
- Si existe `mod1/cuadro_10.dbf` y `mod2/cuadro_10.dbf` en el mismo año → usar el primero encontrado, ignorar el duplicado (loguear advertencia).

---

## Estructura de tablas PostgreSQL

Todas las fases escriben directamente en PostgreSQL. No se usan JSONs intermedios.

```sql
-- Metadatos de archivos escaneados
CREATE TABLE IF NOT EXISTS files_meta (
    year        INTEGER,
    cuadro_n    INTEGER,
    source_path TEXT,
    columns     TEXT[],         -- nombres de columnas en minúsculas
    id_col      TEXT,           -- columna id_original detectada
    loaded_at   TIMESTAMP DEFAULT now(),
    PRIMARY KEY (year, cuadro_n)
);

-- Columnas extraídas de diccionarios PDF (una fila por campo por página)
CREATE TABLE IF NOT EXISTS dict_columns (
    year            INTEGER,
    modulo_name     TEXT,
    page_number     INTEGER,
    num_pregunta    TEXT,
    cuadro          TEXT,
    descripcion_cuadro TEXT,
    nombre_campo    TEXT,       -- nombre real de la columna en el .dbf/.dta
    descripcion_campo TEXT,
    valores_campo   TEXT,
    value_si        TEXT,       -- valor que representa "sí" en variables dicotómicas
    tipo_dato       TEXT,
    anio_referencia TEXT,       -- si el campo referencia un año anterior
    es_parte_de     TEXT,       -- nombre del campo agregado del que forma parte
    categoria       TEXT,
    subcategoria    TEXT,
    imagen_path     TEXT,
    procesado       BOOLEAN DEFAULT false,
    PRIMARY KEY (year, modulo_name, page_number, nombre_campo)
);

-- Mapeo longitudinal: una fila por variable "canónica"
CREATE TABLE IF NOT EXISTS mapper (
    id_mapper       SERIAL PRIMARY KEY,
    identificador   TEXT UNIQUE NOT NULL,   -- nombre canónico de la variable (sin espacios ni caracteres especiales)
    categoria       TEXT,
    subcategoria    TEXT,
    descripcion     TEXT,
    valores_ref     TEXT,
    clasificado_claude BOOLEAN DEFAULT false,
    clasificado_gemini BOOLEAN DEFAULT false
    -- columnas dinámicas por año se agregan con ALTER TABLE:
    -- y{year}_nombre_campo TEXT
    -- y{year}_archivo      INTEGER
    -- y{year}_num_pregunta TEXT
    -- y{year}_valores      TEXT
    -- y{year}_tipo_dato    TEXT
    -- y{year}_value_si     TEXT
    -- y{year}_tipo_ref_anio TEXT
    -- y{year}_es_parte_de  TEXT
);

-- Tabla de hechos - final - resultados - output - producto de venta
CREATE TABLE IF NOT EXISTS renamu_data (
    id          TEXT,           -- ccdd+ccpp+ccdi (6 dígitos con zfill)
    id_original TEXT,
    year        INTEGER,
    PRIMARY KEY (id, year)
    -- columnas adicionales = identificadores del mapper, se agregan con ALTER TABLE
);
```

---

## Fase 1 — Escanear y convertir archivos de datos

**Objetivo:** leer todos los `.dbf` y `.dta`, normalizar IDs y registrar metadatos en `files_meta`. Guardar parquets para uso posterior.

**Pasos:**

1. Recorrer `DATA_ROOT` buscando archivos `.dbf` y `.dta` recursivamente.
2. Para cada archivo:
   - Extraer `year` (nombre de la carpeta de año) y `cuadro_n` (número del cuadro desde el nombre del archivo; si no hay número → `1`).
   - Si ya existe `(year, cuadro_n)` en `files_meta` → loguear y saltar.
   - Leer con pandas: `.dta` con `pd.read_stata()`, `.dbf` con `simpledbf` o `dbfread`.
   - Normalizar nombres de columnas a minúsculas.
   - Detectar `id_original`: primera columna que empiece por `"id"` o `"ubigeo"`.
   - Construir columna `id`:
     - Convertir `ccdd`, `ccpp`, `ccdi` a entero (eliminar filas donde alguno no sea dígito).
     - Convertir a string con `.zfill(2)`.
     - Concatenar → `id = ccdd + ccpp + ccdi`.
   - Añadir columna `year`.
   - Guardar parquet en `{OUTPUT_ROOT}/files/{year}/c{cuadro_n}.parquet`.
   - Insertar registro en `files_meta`.

---

## Fase 2 — Extraer columnas de diccionarios PDF

**Objetivo:** convertir cada página de cada PDF en un registro estructurado en `dict_columns` usando Gemini.

**Pasos:**

1. Recorrer `DATA_ROOT` buscando archivos `.pdf` recursivamente.
2. Para cada PDF:
   - Extraer `year` y `modulo_name` (nombre del archivo sin extensión; para años > 2020 usar el año como `modulo_name`).
   - Convertir cada página a imagen PNG en memoria (no guardar en disco a menos que falle el procesamiento).
   - Para cada imagen, consultar Gemini con el siguiente prompt estructurado:

```
Eres un extractor de metadatos de diccionarios de datos estadísticos peruanos.
Analiza esta imagen de una página de diccionario y extrae TODOS los campos que encuentres.
Devuelve un array JSON. Cada elemento debe tener EXACTAMENTE estas claves
(null si no aplica, nunca omitas la clave):

[{
  "num_pregunta": string|null,
  "cuadro": string|null,
  "descripcion_cuadro": string|null,
  "nombre_campo": string,           // nombre exacto de la columna en el archivo .dbf/.dta
  "descripcion_campo": string|null,
  "valores_campo": string|null,
  "value_si": string|null,          // si es dicotómica, qué valor representa "sí"
  "tipo_dato": string|null,         // "numerico", "texto", "binario", "categorico"
  "anio_referencia": string|null,   // si el campo referencia un año anterior al de la encuesta
  "es_parte_de": string|null,       // nombre del campo agregado del que este es componente
  "categoria": string|null,
  "subcategoria": string|null
}]

Devuelve SOLO el array JSON, sin texto adicional ni markdown.
```

3. Parsear respuesta. Si falla → loguear `(year, modulo_name, page_number)` en tabla `dict_errors` y continuar.
4. Insertar filas en `dict_columns` con `INSERT ... ON CONFLICT DO NOTHING`.
5. Al finalizar un PDF, marcar páginas como `procesado = true`.
6. **Reintento:** al iniciar la fase, consultar primero por páginas en `dict_errors` o con `procesado = false`.

---

## Fase 3 — Clasificación longitudinal con Claude y Gemini

**Objetivo:** construir la tabla `mapper` que relaciona la misma variable a través de los años.

### 3a. Inicializar mapper con año de referencia (2025)

1. Leer todas las filas de `dict_columns` donde `year = 2025`.
2. Para cada fila única por `nombre_campo`:
   - Construir `identificador`: tomar `subcategoria` (o `descripcion_campo` si no hay), limpiar (snake_case, sin tildes, sin caracteres especiales, máximo 60 caracteres), garantizar unicidad añadiendo sufijo numérico si hay conflicto.
   - Añadir columnas dinámicas `y2025_*` con `ALTER TABLE IF NOT EXISTS` antes de insertar.
   - Insertar en `mapper`.

### 3b. Clasificar años anteriores (2024 → 2004)

Para cada año en orden descendente:

1. Leer filas de `dict_columns` para ese año (donde `procesado = true`).
2. Para cada fila:
   - Llamar a Claude y Gemini (por separado) con este prompt:

```
Eres un clasificador de variables estadísticas del RENAMU (encuesta municipal peruana).

Variable a clasificar:
- nombre_campo: {nombre_campo}
- descripcion: {descripcion_campo}
- valores: {valores_campo}
- categoria: {categoria}
- subcategoria: {subcategoria}

Base de referencia (variables canónicas existentes):
{JSON con id_mapper, identificador, categoria, subcategoria, descripcion, valores_ref}

Tarea: determina si esta variable corresponde a alguna variable canónica existente,
considerando que puede haber cambiado de nombre, estar desagregada/agregada, o
referir a un año anterior.

Responde SOLO con JSON:
{
  "id_mapper": integer|null,     // null si es variable nueva
  "confianza": "alta"|"media"|"baja",
  "razon": string                // explicación breve de la decisión
}
```

3. Si ambos modelos coinciden en `id_mapper` con confianza "alta" o "media" → usar ese valor.
4. Si discrepan o confianza "baja" → insertar en tabla `mapper_discrepancias` para revisión manual.
5. Si `id_mapper` es null → crear nueva fila en `mapper`.
6. Actualizar columnas `y{year}_*` con `ALTER TABLE` + `UPDATE`.

### 3c. Asignar sufijos al identificador

Una vez completa la clasificación, para cada `identificador` en `mapper`, llamar a Gemini con la descripción consolidada para inferir sufijos:

| Sufijo | Significado |
|---|---|
| `_T` | Total o suma de otras columnas |
| `_N` | Conteo o cantidad |
| `_F` / `_M` | Desagregado por sexo (female/male) |
| `_R` | Valor en rango |
| `_P` | Componente de otra variable |
| `_t1` / `_t2` | Referencia a año anterior (t-1, t-2) |

El identificador final debe ser único, sin espacios, sin tildes, en snake_case.

---

## Fase 4 — Construir tabla final `renamu_data`

**Objetivo:** poblar `renamu_data` con una columna por variable canónica, una fila por (id, year).

**Pasos (fila por fila del mapper, reanudable):**

1. Para cada fila en `mapper`:
   - Para cada año que tenga `y{year}_nombre_campo` no nulo:
     - Leer `{OUTPUT_ROOT}/files/{year}/c{y{year}_archivo}.parquet`.
     - Seleccionar columnas: `year`, `id_original`, `id`, y `y{year}_nombre_campo`.
     - Si `y{year}_value_si` no es null → convertir columna a booleano (1 si el valor original == `value_si`, 0 si no, NULL si vacío).
     - Si existe columna `vfi` en el parquet y `vfi == 0` → reemplazar valor con NULL.
     - Renombrar columna a `identificador`.
     - Añadir columna al esquema de `renamu_data` con `ALTER TABLE IF NOT EXISTS`.
     - Hacer upsert: `INSERT INTO renamu_data (...) ON CONFLICT (id, year) DO UPDATE SET {identificador} = EXCLUDED.{identificador}`.

2. Loguear progreso en tabla `pipeline_log (fase, id_mapper, year, status, ts)`.

---

## Reglas generales para el agente - codigo 

- **Reanudabilidad:** antes de cada operación, verificar si ya existe el resultado en PostgreSQL. Nunca reprocesar lo que ya está hecho.
- **Logging:** toda advertencia, error o decisión ambigua se registra en tabla `pipeline_log`.
- **Sin paralelismo:** procesar de forma secuencial para controlar costos de API.
- **Tokens:** al llamar a Claude/Gemini, enviar solo los campos necesarios, no dumps completos de tablas.
- **Errores de API:** 
    - continuar a la siguiente
    - 3 erroes consecutivos significa parar por el dia
    - reintentar máximo 3 veces con backoff exponencial antes de loguear y continuar.
- **Prioridad de ids:** usar `id` (ccdd+ccpp+ccdi) como clave principal. Conservar `id_original` como columna adicional.