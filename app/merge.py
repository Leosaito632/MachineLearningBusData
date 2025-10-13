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

# NOVOS PARÂMETROS PARA A HEURÍSTICA DE SEQUÊNCIA
SEQUENCE_MATCH_LENGTH = 5 # Quantas paradas à frente vamos verificar?
SEQUENCE_MATCH_TIME_TOL = pd.Timedelta("20min") # Tolerância de tempo para cada parada na sequência


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
                         dist_threshold_m, init_time_tol, per_stop_max_tol,
                         # Adicionamos os novos parâmetros aqui
                         sequence_length, sequence_time_tol):
    """
    Processa um único order (ônibus) com heurística de pareamento de sequência.
    Retorna lista de dicts (linhas resultantes).
    """
    results_local = []
    gps_order = gps_df[gps_df['order'] == order_id].sort_values('datetime').reset_index(drop=True)
    if gps_order.empty:
        return results_local

    # MUDANÇA: Não usamos mais uma âncora fixa para o dia todo.
    # O 'order_start' agora é calculado dinamicamente para cada viagem.

    assigned_trips = set()
    gps_pointer_idx = 0

    while gps_pointer_idx < len(gps_order):
        start_point = gps_order.loc[gps_pointer_idx]
        start_time = start_point['datetime']
        start_lat = float(start_point['latitude'])
        start_lon = float(start_point['longitude'])

        # 1) Busca de candidatos (Lógica de âncora dinâmica)
        candidates = []
        for trip_id, trip_df in trips_grouped_local.items():
            if trip_id in assigned_trips:
                continue
            for si, stop in trip_df.iterrows():
                stop_td = stop.get('arrival_td')
                if pd.isna(stop_td): continue

                # MUDANÇA CENTRAL: Em vez de usar uma âncora fixa, calculamos uma âncora hipotética
                # para este pareamento específico.
                hypothetical_anchor = start_time - stop_td
                
                # O horário previsto para a parada, baseado NESTA âncora, é o próprio `start_time`.
                # Verificamos se outras paradas da viagem fazem sentido com esta âncora.
                # A tolerância `init_time_tol` agora age como uma verificação de consistência.
                # Ex: se o ônibus ligou às 4h mas a 1a parada é às 6h, a âncora seria ~6h.
                
                try:
                    d = distance_m(start_lat, start_lon, float(stop['stop_lat']), float(stop['stop_lon']))
                except Exception:
                    continue
                
                if d <= dist_threshold_m:
                    # Guardamos a âncora hipotética junto com o candidato
                    candidates.append({
                        'trip_id': trip_id,
                        'stop_index': si,
                        'stop_row': stop,
                        'distance': d,
                        'hypothetical_anchor': hypothetical_anchor
                    })

        if not candidates:
            gps_pointer_idx += 1
            continue

        # 2) Desempate por PAREAMENTO DE SEQUÊNCIA (lógica nova e mais robusta)
        trip_scores = []
        
        # Agrupa candidatos pela trip_id
        candidates_by_trip = {}
        for cand in candidates:
            candidates_by_trip.setdefault(cand['trip_id'], []).append(cand)

        for trip_id, cand_list in candidates_by_trip.items():
            # Para cada trip, pegamos o melhor candidato inicial (menor distância)
            best_cand = min(cand_list, key=lambda x: x['distance'])
            
            trip_df = trips_grouped_local[trip_id]
            start_stop_index = best_cand['stop_index']
            anchor = best_cand['hypothetical_anchor']
            
            # Agora, tentamos parear as próximas N paradas
            matches_in_sequence = 0
            total_dist_in_sequence = best_cand['distance'] # A primeira já "match"
            last_match_time = start_time
            
            # Verificamos até N paradas à frente
            for i in range(1, sequence_length):
                next_stop_index = start_stop_index + i
                if next_stop_index >= len(trip_df):
                    break
                
                next_stop = trip_df.loc[next_stop_index]
                next_stop_td = next_stop.get('arrival_td')
                if pd.isna(next_stop_td):
                    continue

                # Usamos a ÂNCORA DINÂMICA para prever o horário da próxima parada
                predicted_time = anchor + next_stop_td
                
                # Buscamos no GPS a partir do último ponto pareado
                gps_search_set = gps_order[gps_order['datetime'] > last_match_time]
                if gps_search_set.empty:
                    break

                row, dist, _ = find_nearest_gps_in_set(
                    next_stop['stop_lat'], next_stop['stop_lon'], 
                    predicted_time, gps_search_set, time_tol=sequence_time_tol
                )

                if row is not None and dist <= dist_threshold_m * 1.5: # um pouco mais tolerante na sequência
                    matches_in_sequence += 1
                    total_dist_in_sequence += dist
                    last_match_time = row['datetime']

            # O score prioriza quem teve mais paradas pareadas na sequência.
            # A distância é o critério de desempate.
            # Um número grande (1e9) é usado para penalizar fortemente sequências com menos matches.
            score = (sequence_length - matches_in_sequence) * 1e9 + total_dist_in_sequence
            trip_scores.append((score, trip_id, anchor))

        if not trip_scores:
            gps_pointer_idx += 1
            continue

        # Escolhe a trip com o menor score (melhor sequência)
        chosen_score, chosen_trip_id, chosen_anchor = min(trip_scores, key=lambda x: x[0])
        
        # 3) Percorrer e casar TODAS as paradas da trip escolhida
        trip_df = trips_grouped_local[chosen_trip_id]
        last_matched_time = start_time
        assigned_trips.add(chosen_trip_id)

        # Coletar dados para o score de confiança
        trip_results = []
        stops_matched_count = 0
        total_match_distance = 0.0

        for si, stop in trip_df.iterrows():
            stop_td = stop.get('arrival_td', None)
            stop_data = {
                'order': order_id, 'trip_id': chosen_trip_id,
                'stop_sequence': stop.get('stop_sequence', si), 'stop_id': stop['stop_id'],
                'stop_lat': stop['stop_lat'], 'stop_lon': stop['stop_lon'],
                'arrival_previsto': pd.NaT, 'arrival_real': pd.NaT,
                'atraso_min': None, 'matched_distance_m': None
            }

            if pd.isna(stop_td):
                trip_results.append(stop_data)
                continue

            # A previsão é sempre baseada na ÂNCORA DINÂMICA da viagem
            stop_time_previsto = chosen_anchor + stop_td
            stop_data['arrival_previsto'] = stop_time_previsto
            
            # A busca no GPS começa a partir do último tempo real pareado
            gps_search_set = gps_order[gps_order['datetime'] >= last_matched_time]
            if gps_search_set.empty: gps_search_set = gps_order
            
            row_match, dist_m, _ = find_nearest_gps_in_set(
                stop['stop_lat'], stop['stop_lon'], stop_time_previsto, 
                gps_search_set, time_tol=per_stop_max_tol # Usamos a tolerância maior aqui
            )

            if row_match is not None and dist_m is not None and dist_m <= dist_threshold_m:
                arrival_real = row_match['datetime']
                atraso_min = (arrival_real - stop_time_previsto).total_seconds() / 60.0
                
                stop_data.update({
                    'arrival_real': arrival_real,
                    'atraso_min': atraso_min,
                    'matched_distance_m': dist_m
                })
                
                last_matched_time = arrival_real
                stops_matched_count += 1
                total_match_distance += dist_m
            
            trip_results.append(stop_data)

        # 4) Adicionar o Score de Confiança aos resultados da viagem
        total_stops = len(trip_df)
        match_pct = (stops_matched_count / total_stops) if total_stops > 0 else 0
        avg_dist = (total_match_distance / stops_matched_count) if stops_matched_count > 0 else None

        for res in trip_results:
            res['trip_match_pct'] = round(match_pct, 2)
            res['trip_avg_distance_m'] = round(avg_dist, 2) if avg_dist is not None else None
        
        results_local.extend(trip_results)

        # Mover ponteiro do GPS para depois da última correspondência
        idxs_after = gps_order[gps_order['datetime'] > last_matched_time].index
        gps_pointer_idx = int(idxs_after[0]) if len(idxs_after) > 0 else len(gps_order)

    return results_local

# ------------------ Executando em paralelo ------------------
# ------------------ Executando em paralelo ------------------
all_results = []
print(f"Starting parallel processing with {NUM_WORKERS} workers...")

with ProcessPoolExecutor(max_workers=NUM_WORKERS) as exe:
    # MUDANÇA AQUI: passe os novos parâmetros para a função worker
    futures = {exe.submit(process_order_worker, order_id, gps, trips_grouped,
                          DIST_THRESHOLD_M, INIT_TIME_TOL, PER_STOP_MAX_TOL,
                          SEQUENCE_MATCH_LENGTH, SEQUENCE_MATCH_TIME_TOL): order_id
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
