import pandas as pd


def filtrar_linha(linha, arquivo_saida, gtfs_pasta):

    # Caminho para o GTFS
    arquivo_saida = "app/output/"+arquivo_saida+".csv"
    gtfs_pasta = "app/datasets/"+gtfs_pasta+"/"

    # === 1. Ler os arquivos  ===
    routes = pd.read_csv(gtfs_pasta+'routes.txt')
    trips = pd.read_csv(gtfs_pasta+'trips.txt')
    stop_times = pd.read_csv(gtfs_pasta+'stop_times.txt')
    stops = pd.read_csv(gtfs_pasta+'stops.txt')

    # === 2. Filtrar rota desejada ===
    rota_id = routes.loc[routes['route_short_name']
                         == linha, 'route_id'].iloc[0]  # type: ignore
    print(f"Rota selecionada: {rota_id}")

    # === 3. Filtrar trips e stop_times ===
    trips_rota = trips[trips['route_id'] == rota_id]
    stop_times_rota = stop_times.merge(trips_rota, on='trip_id')

    # === 4. Juntar stops ===
    gtfs_rota = stop_times_rota.merge(stops, on='stop_id')

    # === 5. Ordenar ===
    gtfs_rota = gtfs_rota.sort_values(
        by=['trip_id', 'stop_sequence']).reset_index(drop=True)

    # === 6. Salvar resultado ===
    gtfs_rota.to_csv(arquivo_saida, index=False)
    print("Arquivo salvo: "+arquivo_saida)


filtrar_linha("1012-10", "gtfs-sao-paulo-merge", "gtfs_sao_paulo")
