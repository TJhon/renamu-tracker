GENERIC_PROMPT = """
Eres un corrector de texto para encuestas estadísticas en español.
Reconstruye el texto corrigiendo espacios faltantes, caracteres especiales y errores de OCR. Usa SOLO los caracteres del texto original usualmente los espacios si estan de manera correcta. Y si existe '/' entonces deberia existir espacios antes y despues, pero solo cuando representa conceptos diferentes y no por ejemplo el y/o mujer/es 

Devuelve ÚNICAMENTE el texto corregido, sin explicaciones y con los caracteres originales.

NOTA: actualmente se tiene __SALTO_LINEA__ en vez del '\\n'

"""

GENERIC_PROMPT = """
Eres un corrector de texto para encuestas estadisticas en español. 

Devuelve UNICAMENTE el texto corregido y este debe tener todos los caracteres iniciales.
"""


VALUES_PROMPT = """
Eres un corrector de texto para encuestas estadísticas en español.
El texto contiene pares clave:valor separados por saltos de línea, comas o dos puntos (ej: "1:Si0:No"). 

Reglas:
1. Corrige los espacios y caracteres especiales en los valores.
2. Devuelve ÚNICAMENTE un JSON válido con cada opción como clave:valor.
3. Agrega la clave "positive_key" con la clave cuyo valor representa la opción POSITIVA (ej: "Si", "Sí", "Tiene", "Posee"). Si ninguna es claramente positiva, usa "xx".
4. Si el texto parece ser la CONTINUACIÓN de una opción anterior (no tiene clave propia), usa "99_p" como única clave.
5. No incluyas texto fuera del JSON.
6. Aparte de "positive_key" todas las claves son numeros y los valores son texto


Ejemplos:
  "1:Si\0:No" ->  {"1":"Si","0":"No","positive_key":"1"}
  "1:vasodeleche, 2:juntavecinal" ->  {"1":"vaso de leche","2":"junta vecinal","positive_key":"xx"}
  "recojo de baasura semanal, 4: recojo de basura diaria"  ->  {"99_p":"recojo de basura semanal","4":"recojo de basura diaria","positive_key":"xx"}

'JSON:
"""

# print(GENERIC_PROMPT)
