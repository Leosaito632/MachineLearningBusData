import pandas as pd
import numpy as np
from geopy.distance import geodesic

# === 1. Carregar dados ===

gtfs = pd.read_csv("gtfs_rota_371.csv")
gps = pd.read_csv("rota_371.csv")

# Converter datetime do GPS
gps['datetime'] = pd.to_datetime(gps['datetime'])

# Estimar o horário base (primeiro ponto GPS do dia)
hora_inicio_real = gps['datetime'].min()
data_base = hora_inicio_real.normalize()  # pega só a data (00:00:00)
print("🕒 Início detectado:", hora_inicio_real)

# Converter tempo do GTFS (HH:MM:SS) para datetime real
gtfs['arrival_datetime'] = pd.to_timedelta(gtfs['arrival_time']) + hora_inicio_real

# === 2. Função auxiliar ===

def find_nearest_gps(stop_lat, stop_lon, stop_time, gps_subset, time_tolerance=pd.Timedelta("15min")):
    """Retorna o ponto GPS mais próximo dentro da tolerância de tempo."""
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

# === 3. Casamento GTFS ↔ GPS ===

resultados = []

for _, row in gtfs.iterrows():
    stop_lat = row['stop_lat']
    stop_lon = row['stop_lon']
    stop_time = row['arrival_datetime']

    dt_real, lat_real, lon_real = find_nearest_gps(stop_lat, stop_lon, stop_time, gps)

    if dt_real is not None:
        atraso_min = (dt_real - stop_time).total_seconds() / 60.0
        resultados.append({
            'trip_id': row['trip_id'],
            'stop_id': row['stop_id'],
            'arrival_previsto': stop_time,
            'arrival_real': dt_real,
            'atraso_min': atraso_min,
            'stop_lat': stop_lat,
            'stop_lon': stop_lon,
            'lat_real': lat_real,
            'lon_real': lon_real
        })

df_result = pd.DataFrame(resultados)

# === 4. Salvar resultado ===
df_result.to_csv("gtfs_gps_merged.csv", index=False)
print(f"✅ Arquivo salvo: gtfs_gps_merged.csv")
print(df_result.head())
