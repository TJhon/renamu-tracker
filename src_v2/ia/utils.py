def cosine_to_similarity(distance: float) -> float:
    """ChromaDB devuelve distancia coseno (0=idéntico, 2=opuesto)."""
    return 1.0 - distance / 2.0
