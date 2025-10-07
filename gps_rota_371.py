import pandas as pd

# Caminho para o seu arquivo gigante
input_file = "rota_371.csv"
output_file = "rota_371.csv"

# Defina a rota que você quer
target_line = 371.0  
# Lista para acumular pedaços
chunks = []

# Lendo em pedaços de 1 milhão de linhas (ajuste se necessário)
for chunk in pd.read_csv(input_file, chunksize=1_000_000):
    filtered = chunk[(chunk['line'] == target_line)]
    chunks.append(filtered)

# Junta tudo em um único dataframe menor
df = pd.concat(chunks)

# Converte data + hora em datetime
df['datetime'] = pd.to_datetime(df['date'] + " " + df['time'], format="%m-%d-%Y %H:%M:%S")

dia = "2019-01-25"
df = df[df['datetime'].dt.date == pd.to_datetime(dia).date()]

# Ordena pelos critérios desejados
df = df.sort_values(by=['line', 'order', 'datetime'])

# Salva resultado em novo CSV
df.to_csv(output_file, index=False)

print(f"Arquivo salvo em: {output_file}")
print(df.head())

