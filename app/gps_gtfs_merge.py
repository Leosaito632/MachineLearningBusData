#!/usr/bin/env python3
"""
GTFS <-> GPS merge por ÔNIBUS (order) - Debug / matching para UMA trip específica
Corrige problemas de distância, adiciona diagnóstico e modo single-thread para depuração.
"""

import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import timedelta
import math
import os
import traceback
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ------------------ Configurações (ajuste) ------------------
DIST_THRESHOLD_M = 250        # metros para considerar "próximo" (ajustável)
INIT_TIME_TOL = pd.Timedelta("1h")
PER_STOP_MAX_TOL = pd.Timedelta("2h")
NUM_WORKERS = max(1, (os.cpu_count() or 2) - 1)

# Se quiser rodar apenas para uma trip específica (string/valor do trip_id), coloque aqui.
# Ex: TARGET_TRIP_ID = 'TRIP_12345' ou TARGET_TRIP_ID = 12345
TARGET_TRIP_ID = "1a516c64-e0c1-41ab-9975-71487d6bd5a6"

# Se True, não salva output final; apenas roda diagnósticos e mostra candidatos.
DIAGNOSTIC_ONLY = False

# Se True, força execução single-thread (mais fácil para debug).
FORCE_SINGLE_THREAD = True

# Arquivos (ajuste caminhos)
GTFS_FILE = "app/output/gtfs-rio-309.csv"
GPS_FILE = "app/output/gps_371.csv"
OUTPUT_FILE = "gtfs_gps_merged_by_order_parallel.csv"
OUTPUT_DEBUG_FILE = "debug_candidates_trip.csv"
OUTPUT_DIAG_FILE = "diagnostic_trip_orders.csv"

# ------------------ Ler dados ------------------
print("Loading data...")
gtfs = pd.read_csv(GTFS_FILE)
gps = pd.read_csv(GPS_FILE)

# normalizar nomes de colunas potenciais (strip)
gtfs.columns = [c.strip() for c in gtfs.columns]
gps.columns = [c.strip() for c in gps.columns]

# Converter datetime do GPS (cria coluna datetime se não existir)
if 'datetime' not in gps.columns:
    # tenta juntar date+time se existir
    if 'date' in gps.columns and 'time' in gps.columns:
        gps['datetime'] = pd.to_datetime(gps['date'].astype(str) + ' ' + gps['time'].astype(str),
                                         errors='coerce')
    else:
        gps['datetime'] = pd.to_datetime(gps.get('datetime'), errors='coerce')
else:
    gps['datetime'] = pd.to_datetime(gps['datetime'], errors='coerce')

# garantir colunas numéricas de lat/lon no GPS
for col in ['latitude', 'longitude']:
    if col in gps.columns:
        gps[col] = pd.to_numeric(gps[col], errors='coerce')
    else:
        raise RuntimeError(f"Coluna esperada '{col}' não encontrada em GPS.")

# remover linhas GPS com datetime ou coordenadas inválidas
gps = gps.dropna(subset=['datetime', 'latitude', 'longitude']).reset_index(drop=True)

# Preparar GTFS: converter arrival_time para timedelta (se existir)
if 'arrival_time' in gtfs.columns:
    gtfs['arrival_time'] = gtfs['arrival_time'].astype(str)
    gtfs['arrival_td'] = pd.to_timedelta(gtfs['arrival_time'], errors='coerce')
else:
    # Se GTFS já tem coluna com offset em minutos/segundos, adapte aqui
    gtfs['arrival_td'] = pd.NaT

# Ordenar GTFS por trip_id + stop_sequence (se disponível)
if 'stop_sequence' in gtfs.columns:
    gtfs = gtfs.sort_values(['trip_id', 'stop_sequence']).reset_index(drop=True)
else:
    gtfs = gtfs.sort_values(['trip_id']).reset_index(drop=True)

# Indexar stops por trip para acesso rápido
trips_grouped = {tid: df.reset_index(drop=True) for tid, df in gtfs.groupby('trip_id')}
print(f"GTFS trips: {len(trips_grouped)} | GPS rows: {len(gps)}")

# ------------------ Funções auxiliares ------------------
def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000.0
    lat1, lon1, lat2, lon2 = map(math.radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    c = 2*math.asin(math.sqrt(a))
    return R * c

def distance_m(a_lat, a_lon, b_lat, b_lon):
    return haversine_m(a_lat, a_lon, b_lat, b_lon)

def haversine_m_array(lat1, lon1, lat2_arr, lon2_arr):
    R = 6371000.0
    lat1_r = math.radians(float(lat1))
    lon1_r = math.radians(float(lon1))
    lat2_r = np.radians(lat2_arr.astype(float))
    lon2_r = np.radians(lon2_arr.astype(float))
    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1_r) * np.cos(lat2_r) * np.sin(dlon / 2.0) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    return R * c

def find_nearest_gps_in_set(stop_lat, stop_lon, time_ref, gps_subset, time_tol, dist_threshold_m=None):
    mask_time = (gps_subset['datetime'] >= time_ref - time_tol) & (gps_subset['datetime'] <= time_ref + time_tol)
    nearby = gps_subset[mask_time]
    if nearby.empty:
        return None, None, None

    # bounding box pré-filtro usando aproximação (1 deg lat ~ 111km)
    if dist_threshold_m is not None:
        km_per_deg = 111000.0
        delta_deg = float(dist_threshold_m) / km_per_deg + 0.01
        bbox_mask = (
            (nearby['latitude'] >= float(stop_lat) - delta_deg) &
            (nearby['latitude'] <= float(stop_lat) + delta_deg) &
            (nearby['longitude'] >= float(stop_lon) - delta_deg) &
            (nearby['longitude'] <= float(stop_lon) + delta_deg)
        )
        nearby = nearby[bbox_mask]
        if nearby.empty:
            return None, None, None

    nearby_r = nearby.reset_index(drop=True)
    lats = nearby_r['latitude'].values
    lons = nearby_r['longitude'].values
    dists = haversine_m_array(stop_lat, stop_lon, lats, lons)
    pos = int(np.argmin(dists))
    min_dist = float(dists[pos])

    if (dist_threshold_m is not None) and (min_dist > dist_threshold_m):
        return None, None, None

    row = nearby_r.iloc[pos]
    time_diff = row['datetime'] - time_ref
    return row, min_dist, time_diff

# ------------------ Worker (com logs) ------------------
def process_order_worker(order_id, gps_df, trips_grouped_local,
                         dist_threshold_m, init_time_tol, per_stop_max_tol, target_trip_id=None, debug_list=None):
    """
    Processa um único order. Se target_trip_id for fornecido, só tenta casar aquela trip.
    debug_list (list) é preenchida com candidatos para inspeção.
    """
    results_local = []
    try:
        gps_order = gps_df[gps_df['order'] == order_id].sort_values('datetime').reset_index(drop=True)
        if gps_order.empty:
            return results_local

        order_start = gps_order['datetime'].min()
        assigned_trips = set()
        gps_pointer_idx = 0

        # lista de trips a considerar: se target_trip_id dado, apenas essa; senão todas
        trips_to_check = [target_trip_id] if (target_trip_id is not None) else list(trips_grouped_local.keys())

        while gps_pointer_idx < len(gps_order):
            start_point = gps_order.loc[gps_pointer_idx]
            start_time = start_point['datetime']
            start_lat = float(start_point['latitude'])
            start_lon = float(start_point['longitude'])

            # 3) candidates
            candidates = []
            for trip_id in trips_to_check:
                if trip_id in assigned_trips:
                    continue
                if trip_id not in trips_grouped_local:
                    continue
                trip_df = trips_grouped_local[trip_id]
                for si, stop in trip_df.iterrows():
                    stop_lat = stop.get('stop_lat') if 'stop_lat' in stop.index else stop.get('latitude')
                    stop_lon = stop.get('stop_lon') if 'stop_lon' in stop.index else stop.get('longitude')
                    stop_td = stop.get('arrival_td', None)
                    if pd.isna(stop_td):
                        continue
                    stop_time = order_start + stop_td
                    time_diff = start_time - stop_time
                    if abs(time_diff) > init_time_tol:
                        continue
                    try:
                        d = distance_m(start_lat, start_lon, float(stop_lat), float(stop_lon))
                    except Exception:
                        continue
                    # guardar candidato, mesmo que além do threshold (ajuda no debug)
                    candidates.append((trip_id, si, stop, d, time_diff))

                    # registrar candidato no debug_list se fornecido
                    if debug_list is not None:
                        debug_list.append({
                            'order': order_id, 'trip_id': trip_id, 'stop_index': si,
                            'stop_lat': stop_lat, 'stop_lon': stop_lon,
                            'gps_lat': start_lat, 'gps_lon': start_lon,
                            'gps_datetime': start_time, 'stop_time': stop_time,
                            'dist_m': d, 'time_diff_s': time_diff.total_seconds()
                        })

            if not candidates:
                gps_pointer_idx += 1
                continue

            # filtrar candidatos por threshold espacial para desempate (se houver)
            candidates_within = [c for c in candidates if c[3] <= dist_threshold_m]
            if candidates_within:
                candidates = candidates_within

            # 4) heurística de desempate: tentar usar próximo stop como confirmação
            candidates_by_trip = {}
            for trip_id, si, stop_row, d, tdiff in candidates:
                candidates_by_trip.setdefault(trip_id, []).append((si, stop_row, d, tdiff))

            trip_scores = []
            for trip_id, cand_list in candidates_by_trip.items():
                cand_list_sorted = sorted(cand_list, key=lambda x: x[2])
                si, stop_row, d, tdiff = cand_list_sorted[0]
                trip_df = trips_grouped_local[trip_id]
                # tentar confirmar pelo próximo stop
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
                            next_stop.get('stop_lat', next_stop.get('latitude')),
                            next_stop.get('stop_lon', next_stop.get('longitude')),
                            next_stop_time, gps_after, time_tol=init_time_tol,
                            dist_threshold_m=dist_threshold_m)
                        if next_row is not None:
                            sched_delta = (next_stop_time - (order_start + stop_row['arrival_td'])).total_seconds()
                            gps_delta = (next_row['datetime'] - start_time).total_seconds()
                            score = abs(gps_delta - sched_delta)
                            trip_scores.append((trip_id, score, {
                                'matched_stop_index': si, 'first_dist': d,
                                'second_dist': next_d, 'sched_delta': sched_delta, 'gps_delta': gps_delta
                            }))
                            continue
                score = 1e9 + d
                trip_scores.append((trip_id, score, {'matched_stop_index': si, 'first_dist': d}))

            trip_scores_sorted = sorted(trip_scores, key=lambda x: x[1])
            chosen_trip_id, chosen_score, chosen_info = trip_scores_sorted[0]

            # 5) casar stops da trip escolhida
            trip_df = trips_grouped_local[chosen_trip_id]
            last_matched_time = start_time
            assigned_trips.add(chosen_trip_id)

            for si, stop in trip_df.iterrows():
                stop_td = stop.get('arrival_td', None)
                stop_lat = stop.get('stop_lat') if 'stop_lat' in stop.index else stop.get('latitude')
                stop_lon = stop.get('stop_lon') if 'stop_lon' in stop.index else stop.get('longitude')

                if pd.isna(stop_td):
                    results_local.append({
                        'order': order_id, 'trip_id': chosen_trip_id,
                        'stop_sequence': stop.get('stop_sequence', si),
                        'stop_id': stop.get('stop_id', None),
                        'stop_lat': stop_lat, 'stop_lon': stop_lon,
                        'arrival_previsto': pd.NaT, 'arrival_real': pd.NaT,
                        'atraso_min': None, 'matched_distance_m': None
                    })
                    continue

                stop_time = order_start + stop_td
                gps_search_set = gps_order[gps_order['datetime'] > last_matched_time]
                if gps_search_set.empty:
                    gps_search_set = gps_order

                tol = pd.Timedelta("5min")
                matched = None
                while tol <= per_stop_max_tol:
                    row_match, dist_m, time_diff = find_nearest_gps_in_set(
                        stop_lat, stop_lon, stop_time, gps_search_set, time_tol=tol,
                        dist_threshold_m=dist_threshold_m)
                    if row_match is not None:
                        matched = (row_match, dist_m, time_diff)
                        break
                    tol += pd.Timedelta("5min")

                if matched is None:
                    results_local.append({
                        'order': order_id, 'trip_id': chosen_trip_id,
                        'stop_sequence': stop.get('stop_sequence', si),
                        'stop_id': stop.get('stop_id', None),
                        'stop_lat': stop_lat, 'stop_lon': stop_lon,
                        'arrival_previsto': stop_time, 'arrival_real': pd.NaT,
                        'atraso_min': None, 'matched_distance_m': None
                    })
                else:
                    gps_row, dist_m, time_diff = matched
                    arrival_real = gps_row['datetime']
                    atraso_min = (arrival_real - stop_time).total_seconds() / 60.0
                    results_local.append({
                        'order': order_id, 'trip_id': chosen_trip_id,
                        'stop_sequence': stop.get('stop_sequence', si),
                        'stop_id': stop.get('stop_id', None),
                        'stop_lat': stop_lat, 'stop_lon': stop_lon,
                        'arrival_previsto': stop_time, 'arrival_real': arrival_real,
                        'atraso_min': atraso_min, 'matched_distance_m': dist_m
                    })
                    last_matched_time = arrival_real

            # mover ponteiro
            idxs_after = gps_order[gps_order['datetime'] > last_matched_time].index
            if len(idxs_after) > 0:
                gps_pointer_idx = int(idxs_after[0])
            else:
                break

    except Exception as e:
        logging.error(f"Exception in process_order_worker for order {order_id}: {e}")
        logging.error(traceback.format_exc())
    return results_local

# ------------------ Função de diagnóstico (ignora tempo) ------------------
def diagnostic_min_distances_for_trip(trip_id, gps_df, trips_grouped_local):
    """
    Calcula a menor distância (ignorando tempo) entre cada order e os stops da trip.
    Útil para ver quais orders estão espacialmente próximos.
    Retorna DataFrame com ordem, min_dist, mean_dist, n_points_within_500m.
    """
    if trip_id not in trips_grouped_local:
        raise KeyError(f"Trip {trip_id} não encontrada no GTFS.")
    trip_df = trips_grouped_local[trip_id]
    # extrair stops coords
    stop_coords = []
    for _, s in trip_df.iterrows():
        lat = s.get('stop_lat') if 'stop_lat' in s.index else s.get('latitude')
        lon = s.get('stop_lon') if 'stop_lon' in s.index else s.get('longitude')
        if pd.notna(lat) and pd.notna(lon):
            stop_coords.append((float(lat), float(lon)))
    orders = gps_df['order'].unique()
    rows = []
    for order in orders:
        gps_order = gps_df[gps_df['order'] == order]
        if gps_order.empty:
            continue
        # vetorizar: para cada gps point, calcular min dist to any stop
        gps_lats = gps_order['latitude'].values.astype(float)
        gps_lons = gps_order['longitude'].values.astype(float)
        # para cada stop, compute distances -> we can compute pairwise quickly
        min_dists = []
        for glat, glon in zip(gps_lats, gps_lons):
            d_to_stops = [haversine_m(glat, glon, slat, slon) for (slat, slon) in stop_coords]
            min_dists.append(min(d_to_stops) if d_to_stops else np.nan)
        min_dists = np.array(min_dists, dtype=float)
        if len(min_dists) == 0:
            continue
        rows.append({
            'order': order,
            'min_dist_m': float(np.nanmin(min_dists)),
            'mean_dist_m': float(np.nanmean(min_dists)),
            'pct_within_250m': float((min_dists <= 250).sum() / len(min_dists)) if len(min_dists) > 0 else 0.0,
            'n_gps_points': len(min_dists)
        })
    return pd.DataFrame(rows).sort_values('min_dist_m')

# ------------------ EXECUÇÃO FOCADA ------------------
if TARGET_TRIP_ID is not None:
    logging.info(f"Running focused matching for trip {TARGET_TRIP_ID}")
    # preparar debug list
    debug_candidates = []
    selected_orders = gps['order'].unique()  # ainda consideramos todas orders; diagnostic abaixo ajuda a filtrar

    # primeiro diagnóstico: ver ordens mais próximas espacialmente (ignora tempo)
    diag = diagnostic_min_distances_for_trip(TARGET_TRIP_ID, gps, trips_grouped)
    diag.to_csv(OUTPUT_DIAG_FILE, index=False)
    logging.info("Diagnostic saved to %s (orders sorted by min distance)", OUTPUT_DIAG_FILE)
    print(diag.head(30))

    # se apenas diagnóstico, salva e sai
    if DIAGNOSTIC_ONLY:
        print("DIAGNOSTIC_ONLY=True -> aborting matching run (diagnostic saved).")
        raise SystemExit(0)

    # reduzir orders a somente aqueles com min_dist < e.g. 2000m para acelerar tentativa
    close_orders = diag[diag['min_dist_m'] <= 2000]['order'].values if not diag.empty else []
    if len(close_orders) == 0:
        logging.warning("Nenhuma ordem com min_dist <= 2000m encontrada para essa trip. Vou tentar todas mesmo.")
        close_orders = selected_orders

    # rodar por cada order (não paralelo por padrão para debug)
    all_results = []
    if FORCE_SINGLE_THREAD:
        for o in close_orders:
            res = process_order_worker(o, gps, trips_grouped,
                                       DIST_THRESHOLD_M, INIT_TIME_TOL, PER_STOP_MAX_TOL,
                                       target_trip_id=TARGET_TRIP_ID, debug_list=debug_candidates)
            logging.info(f"Order {o} processed. Rows -> {len(res)}")
            all_results.extend(res)
    else:
        with ProcessPoolExecutor(max_workers=NUM_WORKERS) as exe:
            futures = {exe.submit(process_order_worker, o, gps, trips_grouped,
                                  DIST_THRESHOLD_M, INIT_TIME_TOL, PER_STOP_MAX_TOL,
                                  TARGET_TRIP_ID, None): o for o in close_orders}
            for fut in as_completed(futures):
                o = futures[fut]
                try:
                    res = fut.result()
                    logging.info(f"Order {o} done. Rows returned: {len(res)}")
                    all_results.extend(res)
                except Exception as e:
                    logging.error(f"Order {o} failed: {e}")

    # salvar debug candidates e resultados
    df_dbg = pd.DataFrame(debug_candidates)
    if not df_dbg.empty:
        df_dbg.to_csv(OUTPUT_DEBUG_FILE, index=False)
        logging.info("Debug candidates saved to %s (rows=%d)", OUTPUT_DEBUG_FILE, len(df_dbg))
    df_res = pd.DataFrame(all_results)
    out_file = f"match_results_trip_{TARGET_TRIP_ID}.csv"
    df_res.to_csv(out_file, index=False)
    logging.info("Match results saved to %s (rows=%d)", out_file, len(df_res))

    # mostrar sumário simples
    if not df_res.empty:
        matched = df_res[df_res['matched_distance_m'].notna()]['matched_distance_m']
        print("Resumo matching for", TARGET_TRIP_ID)
        print("Total stops (rows):", len(df_res))
        print("Stops matched:", len(matched))
        print("Stops unmatched:", len(df_res) - len(matched))
        if len(matched) > 0:
            print("Distância média dos matches: {:.1f} m".format(matched.mean()))
            print("Distância máxima dos matches: {:.1f} m".format(matched.max()))
            print("Top-20 maiores matched_distance_m (m):")
            print(matched.sort_values(ascending=False).head(20).values)
    else:
        logging.warning("Nenhum resultado de matching para trip %s (ver diagnóstico).", TARGET_TRIP_ID)
        # já salvamos o diagnóstico; sair
else:
    # comportamento original: processar todas as orders (mantive, mas com logs)
    logging.info("TARGET_TRIP_ID not set -> processing all orders (may be expensive).")
    all_results = []
    orders = gps['order'].unique()
    if FORCE_SINGLE_THREAD:
        for o in orders:
            res = process_order_worker(o, gps, trips_grouped, DIST_THRESHOLD_M, INIT_TIME_TOL, PER_STOP_MAX_TOL, target_trip_id=None)
            logging.info(f"Order {o} processed. Rows -> {len(res)}")
            all_results.extend(res)
    else:
        with ProcessPoolExecutor(max_workers=NUM_WORKERS) as exe:
            futures = {exe.submit(process_order_worker, order_id, gps, trips_grouped,
                                  DIST_THRESHOLD_M, INIT_TIME_TOL, PER_STOP_MAX_TOL): order_id
                       for order_id in orders}
            for fut in as_completed(futures):
                order_id = futures[fut]
                try:
                    res = fut.result()
                    all_results.extend(res)
                    logging.info(f"Order {order_id} done. Rows returned: {len(res)}")
                except Exception as e:
                    logging.error(f"Order {order_id} failed with exception: {e}")

    df_res = pd.DataFrame(all_results)
    if 'matched_distance_m' in df_res.columns:
        df_res['matched_distance_m'] = pd.to_numeric(df_res['matched_distance_m'], errors='coerce')
    df_res.to_csv(OUTPUT_FILE, index=False)
    logging.info("Done. Results saved to %s. Rows: %d", OUTPUT_FILE, len(df_res))
