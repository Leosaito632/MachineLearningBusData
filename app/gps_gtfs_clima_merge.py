import pandas as pd
import numpy as np
from geopy.distance import geodesic

# === 1️⃣ Ler os dados ===
onibus = pd.read_csv("gtfs_gps_merged_by_order_parallel_vitor.csv")
clima = pd.read_csv("app/output/clima/estacoes.csv")


# === 2️⃣ Converter colunas de data/hora ===
onibus["arrival_real"] = pd.to_datetime(
    onibus["arrival_real"], errors="coerce")
clima["datetime"] = pd.to_datetime(clima["datetime"], errors="coerce")

# === 3️⃣ Criar flag: tem horário real? ===
onibus["tem_horario_real"] = onibus["arrival_real"].notna()

# === 4️⃣ Preparar tabela de estações ===
# (garantindo que cada estação tenha lat/lon únicos)
estacoes = clima[["estacao", "latitude", "longitude"]
                 ].drop_duplicates().reset_index(drop=True)

# === 5️⃣ Função: qual estação é mais próxima de cada parada? ===


def estacao_mais_proxima(lat, lon):
    if pd.isna(lat) or pd.isna(lon):
        return np.nan
    dists = estacoes.apply(
        lambda e: geodesic(
            (lat, lon), (e["latitude"], e["longitude"])).kilometers,
        axis=1
    )
    return estacoes.loc[dists.idxmin(), "estacao"]


# === 6️⃣ Aplicar função às paradas ===
onibus["estacao"] = onibus.apply(
    lambda r: estacao_mais_proxima(
        r["stop_lat"], r["stop_lon"]),  # type: ignore
    axis=1
)  # type: ignore

# === 7️⃣ Preparar DataFrame de ônibus com datetime arredondado ===
onibus["datetime"] = onibus["arrival_real"].dt.floor("H")

# === 8️⃣ Merge ônibus + clima ===
merged = pd.merge(
    onibus,              # todos os registros, inclusive sem horário real
    clima,
    how="left",
    on=["estacao", "datetime"]
)

# === 7️⃣ Reanexar registros sem horário real ===
merged_completo = pd.concat(
    [merged, onibus[~onibus["tem_horario_real"]]],
    ignore_index=True
)
# === 6️⃣ Salvar resultado ===
arquivo_saida = "app/output/merge_final/onibus_clima_unificado.csv"
merged.to_csv(arquivo_saida, index=False)

print(f"Arquivo salvo como {arquivo_saida}")
print(f"Linhas totais: {len(merged)}")
print(merged.head())
