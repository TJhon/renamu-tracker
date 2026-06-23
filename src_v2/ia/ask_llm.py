import json

from ollama import chat

from src_v2.ia.config import MODEL_LLM
from src_v2.ia.texto.prompts import GENERIC_PROMPT, VALUES_PROMPT


def ask_llm(prompt: str, system_prompt, type="json") -> dict:
    """Llama a Qwen y parsea la respuesta JSON."""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": prompt},
    ]

    # print(messages)
    resp = chat(
        model=MODEL_LLM,
        messages=messages,
        options={"temperature": 0.0, "seed": 100},
        format=type,
        think=False,
    )

    return resp


def parse_output(response, type="json"):
    if type == "json":
        try:
            return json.loads(response.message.content)
        except json.JSONDecodeError:
            return {}
    return response.message.content


def ask_llm_get_response(prompt, system_prompt, output_type="json"):
    response = ask_llm(prompt, system_prompt, type=output_type)
    return parse_output(response, type=output_type)


if __name__ == "__main__":
    from rich import print

    textos_malformateados = [
        "DireccióndelaMunicipalidad/Km",
        "\nCondiciónde PropiedaddellocalMunicipal/\nCondiciónAlquilado\nEquiposdeComputoydeoficina/Fotocopiadoras",
        "administracionpublica y trabajadoresal31deagosto",
    ]
    for i in textos_malformateados:
        resp = ask_llm(i, GENERIC_PROMPT, type="")
        r = parse_output(resp, "str")
        print(r)

    valores_malformateados = [
        "1:Si\0:No",
        "recojo de Baasura Semanal, \n 4: recojo de basura diaria",
        "1:Afirma\n0:Noafirma",
        "1al 6\n(Verdescripciónen\nlatablaCUADRO\n_CATEGORÍA)",
        "1: Falta aprobar por el Concejo Municipal\n2: En elaboración\n3: Falta de recursos para su elaboración\n4: Otro",
        "1: Provincial\n2: Distrital\n3. Centro\nPoblado",
        "1: Si y está\nactualizado\n2: Si y está\ndesactualizado\n3: No tiene,\nporque está en\nproceso de\nimplementación\n4: No tiene,\nporque\ndesconoce cómo\nimplementarlo",
    ]
    for i in valores_malformateados:
        resp = ask_llm(i, VALUES_PROMPT)
        r = parse_output(resp, "json")
        print(r)

    """
    # output: 
    Dirección de la Municipalidad / Km
    Condición de Propiedad del local Municipal / Alquilado  
    Equipos de Computo y de oficina / Fotocopíadoras        
    {'1': 'Si', '0': 'No', 'positive_key': '1'}
    {'99_p': 'recojo de basuras semanal', '4': 'recojo de basura diaria', 'positive_key': 'xx'}
    """
