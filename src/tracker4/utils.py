import json

from ollama import chat

from .config import LLM_MODEL, SYSTEM_PROMPT


def ask_llm(prompt: str) -> dict:
    """Llama a Qwen y parsea la respuesta JSON."""

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": prompt},
    ]

    # print(messages)
    resp = chat(
        model=LLM_MODEL,
        messages=messages,
        options={"temperature": 0.0},
        format="json",
        think=False,
    )
    # print(resp)
    # Limpiar posibles backticks de markdown
    try:
        return json.loads(resp.message.content)
    except json.JSONDecodeError:
        return {}


ask_llm("cuanto es 12 + 12")


def cosine_to_similarity(distance: float) -> float:
    """ChromaDB devuelve distancia coseno (0=idéntico, 2=opuesto)."""
    return 1.0 - distance / 2.0
