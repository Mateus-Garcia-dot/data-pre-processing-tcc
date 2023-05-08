import lzma
from elasticsearch import helpers, Elasticsearch
import geojson
import pandas as pd

file = lzma.open(
    "./data/veiculos/2023_04_17_veiculos.json.xz", mode="rt").read()

df = pd.read_json(file, lines=True)
df["LAT"].replace(",", ".", regex=True, inplace=True)
df["LAT"] = df["LAT"].apply(pd.to_numeric)
df["LON"].replace(",", ".", regex=True, inplace=True)
df["LON"] = df["LON"].apply(pd.to_numeric)
df["DTHR"] = pd.to_datetime(
    df["DTHR"], format="%d/%m/%Y %H:%M:%S").dt.tz_localize("America/Sao_Paulo")

es = Elasticsearch(
    hosts=["https://elastic.tccurbstads.com:443"],
    basic_auth=("elastic", "!@ContaElastic"),
)

docs = (
    {
        "shape": geojson.Point((x["LON"], x["LAT"])),
        "VEIC": x["VEIC"],
        "time": x["DTHR"],
        "linha": x["COD_LINHA"],
    }
    for x in df.to_dict("records")
)

index = "veiculos-localizacoes"

mapping = {"properties": {"VEIC": {"type": "keyword"},
                          "shape": {"type": "geo_shape"}}}

es.indices.create(index=index, mappings=mapping)

es.indices.refresh(index=index)

thing = ({"_op_type": "create", "_source": doc} for doc in docs)

rs = list(helpers.parallel_bulk(es, thing, index=index))
print(rs)
