import pandas as pd
from geopy.distance import geodesic

# === 1️⃣ Ler os dados ===
onibus = pd.read_csv("gtfs_gps_merged_by_order_parallel.csv")
clima = pd.read_csv("app/output/clima/estacoes.csv")

# === 2️⃣ Converter colunas de data/hora ===
onibus["arrival_real"] = pd.to_datetime(
    onibus["arrival_real"], errors="coerce")
clima["datetime"] = pd.to_datetime(clima["datetime"], errors="coerce")

# (opcional: eliminar linhas com horários inválidos)
onibus = onibus.dropna(subset=["arrival_real"])
clima = clima.dropna(subset=["datetime"])

# === 3️⃣ Encontrar estação mais próxima de cada parada ===
# Cria lista de estações únicas com coordenadas
estacoes = clima[["estacao", "latitude", "longitude"]
                 ].drop_duplicates().reset_index(drop=True)


def estacao_mais_proxima(lat, lon):
    """Retorna o nome da estação mais próxima dado um par (lat, lon).
    GPT disse isso
    Aqui uso uma aproximação rápida (distância Euclidiana no plano), 
    mas se quiser mais precisão, dá pra usar geopy.distance.geodesic.
    """
    return estacoes.iloc[
        ((estacoes["latitude"] - lat)**2 +
         (estacoes["longitude"] - lon)**2).idxmin()
    ]["estacao"]


# Aplica função para cada parada
onibus["estacao"] = onibus.apply(
    lambda r: estacao_mais_proxima(r["stop_lat"], r["stop_lon"]), axis=1)

# === 4️⃣ Ajustar hora para o merge temporal ===
# Arredonda para a hora cheia
# Ex: 09:25 → 09:00.
onibus["datetime"] = onibus["arrival_real"].dt.floor("H")

# === 5️⃣ Fazer o merge ===
# Cada registro de ônibus recebe o clima da hora e estação mais próximas
merged = pd.merge(
    onibus,
    clima,
    how="left",
    on=["estacao", "datetime"]
)

# === 6️⃣ Salvar resultado ===
arquivo_saida = "app/output/merge_final/onibus_clima_unificado.csv"
merged.to_csv(arquivo_saida, index=False)

print(f"Arquivo salvo como {arquivo_saida}")
print(f"Linhas totais: {len(merged)}")
print(merged.head())
