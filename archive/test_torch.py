from sentence_transformers import SentenceTransformer
import torch
import umap

device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"  Dispositivo: {device}")

# multilingual-e5-large: mejor calidad para espanol, cabe en 8GB VRAM con margen
# El modelo e5 requiere un prefijo especial en los textos
MODEL_NAME = "intfloat/multilingual-e5-base"
model = SentenceTransformer(MODEL_NAME, device=device)


data_unique = [
    "Viviendas unifamiliares /  / Número de licencias según monto de obra / Hasta S/. 28000",
    "Viviendas unifamiliares /  / Número de licencias según monto de obra / De S/. 29000 a S/. 40000",
    "Viviendas unifamiliares /  / Número de licencias según monto de obra / De S/. 41000 a más",
    "Viviendas unifamiliares /  / Número de licencias según monto de obra / Área construida",
    "Viviendas multifamiliares /  / Número de licencias según monto de obra / Hasta S/. 28000",
    "Viviendas multifamiliares /  / Número de licencias según monto de obra / De S/. 29000 a S/. 40000",
    "Viviendas multifamiliares /  / Número de licencias según monto de obra / De S/. 41000 a más",
    "Viviendas multifamiliares /  / Número de licencias según monto de obra / Área construida",
    "Viviendas bifamiliares /  / Número de licencias según monto de obra / Hasta S/. 28000",
    "Viviendas bifamiliares /  / Número de licencias según monto de obra / De S/. 29000 a S/. 40000",
    "Viviendas bifamiliares /  / Número de licencias según monto de obra / De S/. 41000 a más",
    "Viviendas bifamiliares /  / Número de licencias según monto de obra / Área construida"
]

# e5 necesita prefijo "query: " o "passage: " segun el uso
# Para clustering (no hay query vs documento) usamos "passage: " en todos
# textos_e5 = ["passage: " + t for t in df["texto_canonico"].tolist()]
textos_e5 = ["passage: " + t for t in data_unique]

embeddings = model.encode(
    textos_e5,
    batch_size=128,        # 128 es seguro para 8GB; puedes subir a 256 si no hay OOM
    show_progress_bar=True,
    convert_to_numpy=True,
    normalize_embeddings=True,
    device=device,
)

print(f"  Shape embeddings: {embeddings.shape}")  # (29000, 1024)



# Parametros de clustering (ajustar segun resultados)
UMAP_N_COMPONENTS   = 3    # dimensiones reducidas
UMAP_N_NEIGHBORS    = 3    # balance local/global (subir si hay pocos clusters)
HDBSCAN_MIN_CLUSTER = 5     # min filas para formar un cluster (bajar = mas clusters)
HDBSCAN_MIN_SAMPLES = 3     # sensibilidad al ruido
 
# Cuantos ejemplos por cluster para el prompt LLM
EJEMPLOS_POR_CLUSTER = 8
 
 
reducer = umap.UMAP(
    n_components=UMAP_N_COMPONENTS,
    n_neighbors=UMAP_N_NEIGHBORS,
    min_dist=0.0,          # 0.0 = clusters mas compactos (mejor para HDBSCAN)
    metric="cosine",
    random_state=42,
    low_memory=False,      # cambiar a True si hay problemas de RAM con 29k filas
)

embeddings_2d = reducer.fit_transform(embeddings)

import hdbscan

clusterer = hdbscan.HDBSCAN(
    min_cluster_size=HDBSCAN_MIN_CLUSTER,
    min_samples=HDBSCAN_MIN_SAMPLES,
    metric="euclidean",           # sobre el espacio UMAP reducido
    cluster_selection_method="eom",   # excess of mass: menos clusters, mas estables
    prediction_data=True,         # necesario para soft_clustering
)
 
labels = clusterer.fit_predict(embeddings_2d)
probs  = clusterer.probabilities_   # probabilidad de pertenencia (0-1)