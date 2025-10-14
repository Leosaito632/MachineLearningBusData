#!/usr/bin/env python3
"""
GTFS <-> GPS merge por ÔNIBUS (order) - Paralelo
Alinhamento por ORDER (cada ônibus usa seu primeiro timestamp como referência).
Processa orders em paralelo (ProcessPoolExecutor).
"""

import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import timedelta
import math
import os

# ------------------ Parâmetros ------------------
DIST_THRESHOLD_M = 250       # metros para considerar "próximo" (ajustável)
INIT_TIME_TOL = pd.Timedelta("1h")
PER_STOP_MAX_TOL = pd.Timedelta("2h")
NUM_WORKERS = max(1, (os.cpu_count() or 2) - 1)  # ajuste se quiser

# ------------------ Caminhos (ajuste se necessário) ------------------
GTFS_FILE = "app/output/gtfs/gtfs_rota_371.csv"
GPS_FILE = "app/output/gps_371.csv"
OUTPUT_FILE = "gtfs_gps_merged_by_order_parallel.csv"

# ------------------ Carregar dados ------------------
print("Loading data...")
gtfs = pd.read_csv(GTFS_FILE)
gps = pd.read_csv(GPS_FILE)

# Converter datetime do GPS (cria coluna datetime se não existir)
if 'datetime' not in gps.columns:
    gps['datetime'] = pd.to_datetime(gps['date'] + ' ' + gps['time'], format="%m-%d-%Y %H:%M:%S")
else:
    gps['datetime'] = pd.to_datetime(gps['datetime'])

# garantir colunas numéricas de lat/lon no GPS
gps['latitude'] = gps['latitude'].astype(float)
gps['longitude'] = gps['longitude'].astype(float)

# Preparar GTFS: guardar offsets (timedelta)
gtfs['arrival_time'] = gtfs['arrival_time'].astype(str)
gtfs['arrival_td'] = pd.to_timedelta(gtfs['arrival_time'], errors='coerce')  # NaT se inválido

# Ordenar GTFS por trip_id + stop_sequence (se disponível)
if 'stop_sequence' in gtfs.columns:
    gtfs = gtfs.sort_values(['trip_id', 'stop_sequence']).reset_index(drop=True)
else:
    gtfs = gtfs.sort_values(['trip_id']).reset_index(drop=True)

# Indexar stops por trip para acesso rápido
trips_grouped = {tid: df.reset_index(drop=True) for tid, df in gtfs.groupby('trip_id')}

print(f"GTFS trips: {len(trips_grouped)} | GPS rows: {len(gps)}")
orders = gps['order'].unique()
print(f"Orders found: {len(orders)}")

# ------------------ Funções auxiliares (módulo-level para pickling) ------------------
def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000.0
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    c = 2*math.asin(math.sqrt(a))
    return R * c

def distance_m(a_lat, a_lon, b_lat, b_lon):
    # usa haversine sempre (leve e consistente)
    return haversine_m(a_lat, a_lon, b_lat, b_lon)

def find_nearest_gps_in_set(stop_lat, stop_lon, time_ref, gps_subset, time_tol):
    mask = (gps_subset['datetime'] >= time_ref - time_tol) & (gps_subset['datetime'] <= time_ref + time_tol)
    nearby = gps_subset[mask]
    if nearby.empty:
        return None, None, None
    dists = nearby.apply(lambda r: distance_m(stop_lat, stop_lon, r['latitude'], r['longitude']), axis=1)
    min_idx = dists.idxmin()
    row = nearby.loc[min_idx]
    return row, dists.loc[min_idx], (row['datetime'] - time_ref)

# ------------------ Worker function (módulo-level) ------------------
# A função precisa estar no nível do módulo para ser picklable pelo ProcessPool.
def process_order_worker(order_id, gps_df, trips_grouped_local,
                         dist_threshold_m, init_time_tol, per_stop_max_tol):
    """
    Processa um único order (ônibus). Retorna lista de dicts (linhas resultantes).
    gps_df: dataframe completo de GPS (será filtrado por order aqui).
    trips_grouped_local: dict {trip_id: df_of_stops} com coluna 'arrival_td'.
    """
    results_local = []
    gps_order = gps_df[gps_df['order'] == order_id].sort_values('datetime').reset_index(drop=True)
    if gps_order.empty:
        return results_local

    # anchor (inicio de atividade do ônibus)
    order_start = gps_order['datetime'].min()

    assigned_trips = set()
    gps_pointer_idx = 0

    while gps_pointer_idx < len(gps_order):
        start_point = gps_order.loc[gps_pointer_idx]
        start_time = start_point['datetime']
        start_lat = float(start_point['latitude'])
        start_lon = float(start_point['longitude'])

        # 3) candidates
        candidates = []
        for trip_id, trip_df in trips_grouped_local.items():
            if trip_id in assigned_trips:
                continue
            for si, stop in trip_df.iterrows():
                stop_td = stop.get('arrival_td', None)
                if pd.isna(stop_td):
                    continue
                stop_time = order_start + stop_td
                time_diff = start_time - stop_time
                if abs(time_diff) > init_time_tol:
                    continue
                try:
                    d = distance_m(start_lat, start_lon, float(stop['stop_lat']), float(stop['stop_lon']))
                except Exception:
                    continue
                if d <= dist_threshold_m:
                    candidates.append((trip_id, si, stop, d, time_diff))

        if not candidates:
            gps_pointer_idx += 1
            continue

        # 4) desempate por heurística com próximo stop
        candidates_by_trip = {}
        for trip_id, si, stop_row, d, tdiff in candidates:
            candidates_by_trip.setdefault(trip_id, []).append((si, stop_row, d, tdiff))

        trip_scores = []
        for trip_id, cand_list in candidates_by_trip.items():
            cand_list_sorted = sorted(cand_list, key=lambda x: x[2])
            si, stop_row, d, tdiff = cand_list_sorted[0]
            trip_df = trips_grouped_local[trip_id]
            if si + 1 < len(trip_df):
                next_stop = trip_df.loc[si + 1]
                next_td = next_stop.get('arrival_td', None)
                if pd.isna(next_td):
                    score = 1e6 + d
                    trip_scores.append((trip_id, score, {'matched_stop_index': si, 'first_dist': d}))
                    continue
                next_stop_time = order_start + next_td
                gps_after = gps_order[gps_order['datetime'] > start_time]
                if not gps_after.empty:
                    next_row, next_d, next_time_diff = find_nearest_gps_in_set(
                        next_stop['stop_lat'], next_stop['stop_lon'], next_stop_time, gps_after, time_tol=init_time_tol)
                    if next_row is not None:
                        sched_delta = (next_stop_time - (order_start + stop_row['arrival_td'])).total_seconds()
                        gps_delta = (next_row['datetime'] - start_time).total_seconds()
                        score = abs(gps_delta - sched_delta)
                        trip_scores.append((trip_id, score, {
                            'matched_stop_index': si,
                            'first_stop_row': stop_row,
                            'first_dist': d,
                            'second_gps_row': next_row,
                            'second_dist': next_d,
                            'sched_delta': sched_delta,
                            'gps_delta': gps_delta
                        }))
                        continue
            score = 1e9 + d
            trip_scores.append((trip_id, score, {'matched_stop_index': si, 'first_dist': d}))

        trip_scores_sorted = sorted(trip_scores, key=lambda x: x[1])
        chosen_trip_id, chosen_score, chosen_info = trip_scores_sorted[0]

        # 5) percorrer stops da trip e casar
        trip_df = trips_grouped_local[chosen_trip_id]
        last_matched_time = start_time
        assigned_trips.add(chosen_trip_id)

        for si, stop in trip_df.iterrows():
            stop_td = stop.get('arrival_td', None)
            if pd.isna(stop_td):
                results_local.append({
                    'order': order_id,
                    'trip_id': chosen_trip_id,
                    'stop_sequence': stop.get('stop_sequence', si),
                    'stop_id': stop['stop_id'],
                    'stop_lat': stop['stop_lat'],
                    'stop_lon': stop['stop_lon'],
                    'arrival_previsto': pd.NaT,
                    'arrival_real': pd.NaT,
                    'atraso_min': None,
                    'matched_distance_m': None
                })
                continue

            stop_time = order_start + stop_td
            gps_search_set = gps_order[gps_order['datetime'] > last_matched_time]
            if gps_search_set.empty:
                gps_search_set = gps_order

            tol = pd.Timedelta("5min")
            matched = None
            while tol <= per_stop_max_tol:
                row_match, dist_m, time_diff = find_nearest_gps_in_set(stop['stop_lat'], stop['stop_lon'], stop_time, gps_search_set, time_tol=tol)
                if row_match is not None:
                    matched = (row_match, dist_m, time_diff)
                    break
                tol += pd.Timedelta("5min")

            if matched is None:
                results_local.append({
                    'order': order_id,
                    'trip_id': chosen_trip_id,
                    'stop_sequence': stop.get('stop_sequence', si),
                    'stop_id': stop['stop_id'],
                    'stop_lat': stop['stop_lat'],
                    'stop_lon': stop['stop_lon'],
                    'arrival_previsto': stop_time,
                    'arrival_real': pd.NaT,
                    'atraso_min': None,
                    'matched_distance_m': None
                })
            else:
                gps_row, dist_m, time_diff = matched
                arrival_real = gps_row['datetime']
                atraso_min = (arrival_real - stop_time).total_seconds() / 60.0
                results_local.append({
                    'order': order_id,
                    'trip_id': chosen_trip_id,
                    'stop_sequence': stop.get('stop_sequence', si),
                    'stop_id': stop['stop_id'],
                    'stop_lat': stop['stop_lat'],
                    'stop_lon': stop['stop_lon'],
                    'arrival_previsto': stop_time,
                    'arrival_real': arrival_real,
                    'atraso_min': atraso_min,
                    'matched_distance_m': dist_m
                })
                last_matched_time = arrival_real

        # mover ponteiro
        idxs_after = gps_order[gps_order['datetime'] > last_matched_time].index
        if len(idxs_after) > 0:
            gps_pointer_idx = int(idxs_after[0])
        else:
            break

    return results_local

# ------------------ Executando em paralelo ------------------
all_results = []
print(f"Starting parallel processing with {NUM_WORKERS} workers...")

# We will pass gps and trips_grouped as args for each worker call.
# For moderate dataset sizes this is fine. For very large ones consider alternative sharing.
with ProcessPoolExecutor(max_workers=NUM_WORKERS) as exe:
    # submit futures
    futures = {exe.submit(process_order_worker, order_id, gps, trips_grouped,
                          DIST_THRESHOLD_M, INIT_TIME_TOL, PER_STOP_MAX_TOL): order_id
               for order_id in orders}

    for fut in as_completed(futures):
        order_id = futures[fut]
        try:
            res = fut.result()
            all_results.extend(res)
            print(f"Order {order_id} done. Rows returned: {len(res)}")
        except Exception as e:
            print(f"Order {order_id} failed with exception: {e}")

# ------------------ Salvar resultado ------------------
df_res = pd.DataFrame(all_results)
df_res.to_csv(OUTPUT_FILE, index=False)
print(f"Done. Results saved to {OUTPUT_FILE}. Rows: {len(df_res)}")

# ------------------ Observações ------------------
print("""
Observações / dicas:
- Cada processa recebe cópias dos objetos passados (gps, trips_grouped). Para bases muito grandes,
  isso pode consumir muita memória. Se tiver problema de memória, podemos:
    * dividir orders em lotes e processar em série por lote (menor paralelismo),
    * usar um servidor dask / joblib com compartilhamento,
    * ou persistir trips em disco e carregar por demanda nos workers.
- NUM_WORKERS: ajuste para não saturar máquina.
- Uso de haversine garante consistência sem dependência externa nos workers.
""")
