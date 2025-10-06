import pandas as pd
from zipfile import ZipFile

# Caminho para o GTFS
gtfs_path = "gtfs_rio-de-janeiro/"
target_route = "371"

# === 1. Ler os arquivos diretamente do zip ===
routes = pd.read_csv(gtfs_path+'routes.txt')
trips = pd.read_csv(gtfs_path+'trips.txt')
stop_times = pd.read_csv(gtfs_path+'stop_times.txt')
stops = pd.read_csv(gtfs_path+'stops.txt')

# === 2. Filtrar rota desejada ===
rota_id = routes.loc[routes['route_short_name'] == target_route, 'route_id'].iloc[0]
print(f"Rota selecionada: {rota_id}")

# === 3. Filtrar trips e stop_times ===
trips_rota = trips[trips['route_id'] == rota_id]
stop_times_rota = stop_times.merge(trips_rota, on='trip_id')

# === 4. Juntar stops ===
gtfs_rota = stop_times_rota.merge(stops, on='stop_id')

# === 5. Ordenar ===
gtfs_rota = gtfs_rota.sort_values(by=['trip_id', 'stop_sequence']).reset_index(drop=True)

# === 6. Salvar resultado ===
gtfs_rota.to_csv("gtfs_rota_371.csv", index=False)
print("Arquivo salvo: gtfs_rota_371.csv")
