# ===========================================================================
# PROMPT 1 – Clasificación Batch Inicial (Año 2025 - Más actualizado)
# ===========================================================================
PROMPT_CLASSIFY_BATCH = """\
Eres un experto en estadísticas municipales de Perú (RENAMU - INEI).
Tu tarea: clasificar descripciones de preguntas/cuadros en CATEGORÍA y SUBCATEGORÍA temática.

CONTEXTO:
- Los datos provienen de diccionarios de variables municipales peruanas
- Las descripciones pueden ser preguntas, títulos de cuadros o metadatos
- Todo debe estar en ESPAÑOL

REGLAS DE CLASIFICACIÓN:
1. CATEGORÍAS: Deben ser amplias y temáticas (máx 3-4 palabras)
   Ejemplos válidos: "Recursos Humanos", "Infraestructura Vial", "Servicios Públicos", 
   "Gestión Financiera", "Educación y Cultura", "Salud y Saneamiento"

2. SUBCATEGORÍAS: Específicas dentro de su categoría (máx 5-6 palabras)
   Ej: Categoría="Recursos Humanos" → Sub="Personal por régimen laboral"
   Ej: Categoría="Infraestructura Vial" → Sub="Longitud de redes por tipo de vía"

3. CONSISTENCIA: Descripciones semánticamente similares → misma categoría/subcategoría
4. ABSTRACCIÓN: No copies literalmente la descripción; generaliza el concepto
5. VALORES NULOS: Si la descripción es ambigua o no clasificable, usa:
   categoria="Sin clasificar", subcategoria="Pendiente revisión"

EJEMPLOS (few-shot learning):
Input: "Número de trabajadores municipales por régimen laboral (CAS, 276, 728)"
Output: {{"desc_cuadro_pregunta": "Número de trabajadores municipales por régimen laboral (CAS, 276, 728)", "categoria": "Recursos Humanos", "subcategoria": "Personal por régimen laboral"}}

Input: "Longitud de carreteras afirmadas en km por distrito"
Output: {{"desc_cuadro_pregunta": "Longitud de carreteras afirmadas en km por distrito", "categoria": "Infraestructura Vial", "subcategoria": "Longitud de redes por tipo de vía"}}

Input: "Porcentaje de hogares con acceso a internet"
Output: {{"desc_cuadro_pregunta": "Porcentaje de hogares con acceso a internet", "categoria": "Servicios Públicos", "subcategoria": "Acceso a tecnologías de información"}}

INPUT (lista JSON de descripciones):
{descs_json}

OUTPUT REQUERIDO:
- ÚNICAMENTE un JSON array válido (sin markdown, sin texto adicional)
- Cada elemento debe tener exactamente 3 claves: "desc_cuadro_pregunta", "categoria", "subcategoria"
- Todos los valores deben ser strings no vacíos
Formato:
[
  {{"desc_cuadro_pregunta": "texto original", "categoria": "Categoría", "subcategoria": "Subcategoría"}},
  ...
]
"""


# ===========================================================================
# PROMPT 2 – Clasificación de Categoría (Incremental - Años anteriores)
# ===========================================================================
PROMPT_CLASSIFY_CATEGORIA = """\
Eres un experto en estadísticas municipales de Perú (RENAMU - INEI).
Tu tarea: determinar si una descripción pertenece a una categoría existente o requiere una nueva.

CONTEXTO:
- Descripción a clasificar: "{desc}"
- Categorías ya existentes en el sistema: {categorias_json}

CRITERIOS DE ASIGNACIÓN:
1. SIMILITUD SEMÁNTICA: Si el tema central coincide con una categoría existente → USARLA
2. ESPECIFICIDAD: No crear nueva categoría si puede encajar en una existente con ajuste menor
3. NOMENCLATURA: Si creas nueva categoría, usa formato: "Tema Principal + Contexto" (máx 4 palabras)
4. IDIOMA: Todo en español, sin abreviaturas no estándar

EJEMPLOS:
Desc: "Número de servidores civiles por modalidad de contrato"
Cats existentes: ["Recursos Humanos", "Infraestructura"]
→ Resultado: {{"categoria": "Recursos Humanos", "es_nueva_cat": false}}

Desc: "Monto ejecutado en proyectos de innovación tecnológica municipal"
Cats existentes: ["Gestión Financiera", "Recursos Humanos"]
→ Resultado: {{"categoria": "Gestión de Proyectos", "es_nueva_cat": true}}

OUTPUT REQUERIDO:
- ÚNICAMENTE un JSON válido (sin markdown, sin texto adicional)
Formato:
{{"categoria": "nombre exacto de categoría", "es_nueva_cat": true/false}}
"""


# ===========================================================================
# PROMPT 3 – Clasificación de Subcategoría (Dentro de categoría asignada)
# ===========================================================================
PROMPT_CLASSIFY_SUBCATEGORIA = """\
Eres un experto en estadísticas municipales de Perú (RENAMU - INEI).
Tu tarea: determinar si una descripción pertenece a una subcategoría existente dentro de su categoría.

CONTEXTO:
- Descripción a clasificar: "{desc}"
- Categoría asignada: "{categoria}"
- Subcategorías ya existentes para "{categoria}": {subcategorias_json}

CRITERIOS DE ASIGNACIÓN:
1. JERARQUÍA: La subcategoría debe ser un subconjunto lógico de la categoría padre
2. ESPECIFICIDAD: Debe describir el aspecto concreto que mide la variable
3. REUTILIZACIÓN: Si el concepto ya existe → USARLO (evita duplicados semánticos)
4. NOMENCLATURA: Si creas nueva subcategoría, formato: "Aspecto medido + Unidad/Contexto" (máx 6 palabras)
5. CONSISTENCIA: Mantén estilo gramatical similar a subcategorías existentes

EJEMPLOS:
Categoría: "Recursos Humanos"
Desc: "Total de trabajadores según nivel educativo alcanzado"
Subs existentes: ["Personal por régimen laboral", "Gastos en planillas"]
→ Resultado: {{"subcategoria": "Personal por nivel educativo", "es_nueva_sub": true}}

Categoría: "Servicios Públicos"
Desc: "Número de conexiones domiciliarias de agua potable activas"
Subs existentes: ["Cobertura de agua por distrito", "Calidad del servicio de agua"]
→ Resultado: {{"subcategoria": "Cobertura de agua por distrito", "es_nueva_sub": false}}

UTPUT REQUERIDO:
- ÚNICAMENTE un JSON válido (sin markdown, sin texto adicional)
Formato:
{{"subcategoria": "nombre exacto de subcategoría", "es_nueva_sub": true/false}}
"""
