import lzma
import pandas as pd
from scipy import stats

file = lzma.open(
    "./data/veiculos/2023_04_17_veiculos.json.xz", mode="rt").read()

df = pd.read_json(file, lines=True)
df = df[df['COD_LINHA'] == '022']
df["LAT"].replace(",", ".", regex=True, inplace=True)
df["LAT"] = df["LAT"].apply(pd.to_numeric)
df["LON"].replace(",", ".", regex=True, inplace=True)
df["LON"] = df["LON"].apply(pd.to_numeric)
df["DTHR"] = pd.to_datetime(
    df["DTHR"], format="%d/%m/%Y %H:%M:%S").dt.tz_localize("America/Sao_Paulo")

print(df)
