import pandas as pd
import glob

arquivo_saida = "app/output/clima/estacoes.csv"
# Lista de arquivos das 4 estações
arquivos_clima = [
    "marambaia.csv",
    "vila_militar.csv",
    "jacarepagua.csv",
    "copacabana.csv"
]
pos_estacoes = {
    "marambaia": (-23.05027777, -43.59555555),
    "vila_militar": (-22.87, -43.41),
    "jacarepagua": (-22.98, -43.36),
    "copacabana": (-22.99, -43.19)
}


dfs = []
for arq in arquivos_clima:
    nome_estacao = arq.replace('.csv', '')
    lat, lon = pos_estacoes[nome_estacao]

    df = pd.read_csv(f"app/datasets/clima/hora/{arq}", header=9)
    df["estacao"] = nome_estacao
    df["latitude"] = lat
    df["longitude"] = lon
    df["datetime"] = pd.to_datetime(
        df["Data Medicao"] + " " + df["Hora Medicao"].astype(str).str.zfill(4),
        format="%Y-%m-%d %H%M",
        errors="coerce"
    )
    df.drop('Data Medicao', axis=1, inplace=True)
    df.drop('Hora Medicao', axis=1, inplace=True)
    dfs.append(df)

clima_unificado = pd.concat(dfs, ignore_index=True)
clima_unificado = clima_unificado.dropna(subset=["datetime"])
clima_unificado.to_csv(arquivo_saida, index=False)
print(clima_unificado.head())
