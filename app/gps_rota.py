import pandas as pd


def filtrar_linha(linha, arquivo_saida, arquivo_entrada, dia=None):
    arquivo_saida = "app/output/"+arquivo_saida+".csv"
    arquivo_entrada = "app/datasets/gps/"+arquivo_entrada
    # Lista para acumular pedaços
    chunks = []

    # Lendo em pedaços de 1 milhão de linhas (ajuste se necessário)
    for chunk in pd.read_csv(arquivo_entrada, chunksize=1_000_000):
        # filtra pela linha desejada
        filtered = chunk[(chunk['line'] == linha)]
        chunks.append(filtered)

    # Junta tudo em um único dataframe menor
    df = pd.concat(chunks)

    # Converte data + hora em datetime
    df['datetime'] = pd.to_datetime(
        df['date'] + " " + df['time'], format="%m-%d-%Y %H:%M:%S")

    if dia:
        df = df[df['datetime'].dt.date == pd.to_datetime(dia).date()]

    # Ordena pelos critérios desejados
    df = df.sort_values(by=['line', 'order', 'datetime'])

    # Salva resultado em novo CSV
    df.to_csv(arquivo_saida, index=False)

    print(f"Arquivo salvo em: {arquivo_saida}")
    print(df.head())


filtrar_linha(371, "gps_371", "treatedBusDataOnlyRoute.csv", "2019-01-25")
