import pandas as pd
import numpy as np
from geopy.distance import geodesic

# === 1. Carregar dados ===

# GTFS filtrado (gerado antes)
gtfs = pd.read_csv("gtfs_rota_371.csv")

# GPS real (filtrado pela rota 371)
gps = pd.read_csv("rota_371.csv")

# Converter tempo previsto (HH:MM:SS) para datetime (precisa de uma data base)
base_date = pd.to_datetime("2019-01-25")  # ajuste conforme seu dataset
gtfs['arrival_datetime'] = pd.to_timedelta(gtfs['arrival_time']) + base_date

# Converter tempo real do GPS
gps['datetime'] = pd.to_datetime(gps['date'] + " " + gps['time'],
                                 format="%m-%d-%Y %H:%M:%S")

print("GTFS:", gtfs.shape, "GPS:", gps.shape)

# === 2. Função auxiliar para encontrar ponto GPS mais próximo ===

def find_nearest_gps(stop_lat, stop_lon, stop_time, gps_subset, time_tolerance=pd.Timedelta("10min")):
    """Retorna o ponto GPS mais próximo (dentro da tolerância de tempo)."""
    # Filtrar por tempo próximo
    mask = (gps_subset['datetime'] >= stop_time - time_tolerance) & \
           (gps_subset['datetime'] <= stop_time + time_tolerance)
    nearby = gps_subset[mask]

    if nearby.empty:
        return None, None, None

    # Calcular distância geodésica
    distances = nearby.apply(
        lambda row: geodesic((stop_lat, stop_lon), (row['latitude'], row['longitude'])).meters,
        axis=1
    )

    min_idx = distances.idxmin()
    nearest = nearby.loc[min_idx]

    return nearest['datetime'], nearest['latitude'], nearest['longitude']

# === 3. Iterar sobre paradas e casar com GPS ===

resultados = []

for _, row in gtfs.iterrows():
    trip = row['trip_id']
    stop_id = row['stop_id']
    stop_lat = row['stop_lat']
    stop_lon = row['stop_lon']
    stop_time = row['arrival_datetime']

    # Filtrar GPS do mesmo dia (para eficiência)
    gps_dia = gps[gps['datetime'].dt.date == stop_time.date()]

    dt_real, lat_real, lon_real = find_nearest_gps(stop_lat, stop_lon, stop_time, gps_dia)

    if dt_real is not None:
        atraso_min = (dt_real - stop_time).total_seconds() / 60.0
        resultados.append({
            'trip_id': trip,
            'stop_id': stop_id,
            'stop_lat': stop_lat,
            'stop_lon': stop_lon,
            'arrival_previsto': stop_time,
            'arrival_real': dt_real,
            'atraso_min': atraso_min,
            'lat_real': lat_real,
            'lon_real': lon_real
        })

df_result = pd.DataFrame(resultados)

# === 4. Salvar resultado ===
df_result.to_csv("gtfs_gps_merged.csv", index=False)
print(f"✅ Arquivo salvo: gtfs_gps_merged.csv")
print(df_result.head())
