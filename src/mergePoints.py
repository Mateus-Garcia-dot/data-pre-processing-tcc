import lzma
import pandas as pd
from geopy import Point, distance
import geojson
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
from elasticsearch import Elasticsearch, helpers
import util
from pandarallel import pandarallel
import numpy as np


index = "testdumpveiculos3"

pandarallel.initialize(progress_bar=True, nb_workers=5)

es = Elasticsearch(
    hosts=["http://192.168.1.15:9200"],
    basic_auth=("elastic", "!@ContaElastic"),
)


def isPointInShape(row):
    response = es.search(index='test123456789', query={
        "bool": {
            "must": [{
                "match": {
                    "COD": row['COD_LINHA']
                }
            }],
            "filter": {
                "geo_distance": {
                    "distance": "300m",
                    "coordinate": {
                        "lat": row['LAT'], "lon": row['LON']
                    }
                }
            }
        }})

    if response['hits']['total']['value'] <= 0:
        return

    hits = [hit['_source'] for hit in response['hits']['hits']]
    if len(hits) <= 1:
        return

    df = pd.DataFrame(hits)

    df['distance'] = df.apply(lambda row1: distance.geodesic(
        (row1['coordinate']['coordinates'][1], row1['coordinate']['coordinates'][0]), (row['LAT'], row['LON'])).m, axis=1)

    df.sort_values(by=['distance'], inplace=True, ascending=True)

    df = df.groupby(['SHP']).head(2)

    df['closestDirection'] = df.apply(
        lambda row1: row1['direction'] - row['direction'], axis=1).abs().mean()

    df.sort_values(by=['closestDirection'], inplace=True, ascending=True)
    df = df[df['SHP'] == df['SHP'].iloc[0]]

    p3 = np.array([row['LAT'], row['LON']])

    if (len(df) <= 1):
        return

    p1 = np.array([df['coordinate'].iloc[0]['coordinates']
                   [1], df['coordinate'].iloc[0]['coordinates'][0]])

    p2 = np.array([df['coordinate'].iloc[1]['coordinates']

                   [1], df['coordinate'].iloc[1]['coordinates'][0]])

    l2 = np.sum((p1-p2)**2)
    t = np.sum((p3 - p1) * (p2 - p1)) / l2
    projection = p1 + t * (p2 - p1)

    percentage = np.sqrt(np.sum((projection - p1)**2)) / np.sqrt(l2)
    finalPercentage = df['percentage'].iloc[0] * \
        (1-percentage) + df['percentage'].iloc[1] * percentage

    return finalPercentage


def mergePoints(cod):
    file = lzma.open(
        "./data/veiculos/2023_04_17_veiculos.json.xz", mode="rt").read()

    df = pd.read_json(file, lines=True)

    df = df[df['COD_LINHA'] == cod]

    df["LAT"].replace(",", ".", regex=True, inplace=True)
    df["LAT"] = df["LAT"].apply(pd.to_numeric)
    df["LON"].replace(",", ".", regex=True, inplace=True)
    df["LON"] = df["LON"].apply(pd.to_numeric)
    df["DTHR"] = pd.to_datetime(
        df["DTHR"], format="%d/%m/%Y %H:%M:%S").dt.tz_localize("America/Sao_Paulo")

    df['point'] = df.apply(lambda row: Point(
        latitude=row['LAT'], longitude=row['LON']), axis=1)

    df['point_next'] = df.groupby(['COD_LINHA'])[
        'point'].shift(1)
    df.loc[df['point_next'].isna(), 'point_next'] = None

    df['direction'] = df.apply(lambda row: util.calc_bearing(
        (row['point'].latitude, row['point'].longitude), (row['point_next'].latitude, row['point_next'].longitude)) if row['point_next'] is not None else float(0), axis=1)

    df.drop(columns=['point_next'], inplace=True)

    df["percentage"] = df.parallel_apply(
        lambda row: isPointInShape(row), axis=1)

    df = df[df['percentage'].notna()]

    df['point'] = df.apply(lambda row: geojson.Point((
        row['LAT'], row['LON'])), axis=1)

    dictThing = df.to_dict(orient="records")

    es = Elasticsearch(
        hosts=["https://elastic.tccurbstads.com:443"],
        basic_auth=("elastic", "!@ContaElastic")
    )

    list(helpers.parallel_bulk(es, dictThing, index=index))


mapping = {
    "properties": {
        "VEIC": {"type": "keyword"},
        "LAT": {"type": "float"},
        "LON": {"type": "float"},
        "DTHR": {"type": "date"},
        "COD_LINHA": {"type": "keyword"},
        "point": {"type": "geo_point"},
        "direction": {"type": "float"},
        "percentage": {"type": "float"}
    }
}

# es.indices.create(index=index, mappings=mapping)

es.indices.refresh(index=index)

file = lzma.open(
    "./data/veiculos/2023_04_17_veiculos.json.xz", mode="rt").read()
df = pd.read_json(file, lines=True)
df = df['COD_LINHA'].unique()

for cod in df:
    mergePoints(cod)
