import pandas as pd
from geopy import Point, distance
import geojson
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
from elasticsearch import Elasticsearch, helpers
import util
import pymongo


def parseShapeFile(filePath):
    df = pd.read_json(filePath)

    df["LAT"].replace(",", ".", regex=True, inplace=True)
    df["LAT"] = df["LAT"].apply(pd.to_numeric)
    df["LON"].replace(",", ".", regex=True, inplace=True)
    df["LON"] = df["LON"].apply(pd.to_numeric)

    df['point'] = df.apply(lambda row: Point(
        latitude=row['LAT'], longitude=row['LON']), axis=1)

    df.drop('LON', axis=1, inplace=True)
    df.drop('LAT', axis=1, inplace=True)

    df['point_next'] = df.groupby(['COD', 'SHP'])[
        'point'].shift(1)
    df.loc[df['point_next'].isna(), 'point_next'] = None
    df['distance_m'] = df.apply(lambda row: distance.geodesic(
        row['point'], row['point_next']).m if row['point_next'] is not None else float(0), axis=1)

    print(df)

    df['point_last'] = df.groupby(['COD', 'SHP'])[
        'point'].shift(-1)
    df.loc[df['point_last'].isna(), 'point_last'] = None
    df['distance_m_last'] = df.apply(lambda row: distance.geodesic(
        row['point'], row['point_last']).m if row['point_last'] is not None else float(0), axis=1)

    df = df[(
        (np.abs(stats.zscore(df['distance_m'])) < 6) &
        (np.abs(stats.zscore(df['distance_m_last'])) < 6)
    )]

    df['direction'] = df.apply(lambda row: util.calc_bearing(
        (row['point'].latitude, row['point'].longitude), (row['point_next'].latitude, row['point_next'].longitude)) if row['point_next'] is not None else float(0), axis=1)

    df.drop('point_next', axis=1, inplace=True)
    df.drop('point_last', axis=1, inplace=True)

    df = df.assign(order=df.groupby(['COD', 'SHP']).cumcount())

    df['totalSize'] = df.groupby(['COD', 'SHP']).transform('size')
    df['percentage'] = df.apply(
        lambda row: 100 * (row['order'])/(row['totalSize']-1), axis=1)

    df.drop('totalSize', axis=1, inplace=True)

    print(df.groupby(['COD', 'SHP']).first())
    print(df.groupby(['COD', 'SHP']).last())

    geojson.geometry.DEFAULT_PRECISION = 16
    df['coordinate'] = df.apply(
        lambda row: geojson.Point(
            (row['point'].longitude, row['point'].latitude)),
        axis=1)

    df.drop('point', axis=1, inplace=True)
    df.drop('distance_m_last', axis=1, inplace=True)
    df.drop('distance_m', axis=1, inplace=True)

    dictThing = df.to_dict(orient='records')

    es = Elasticsearch(
        hosts=["https://elastic.tccurbstads.com:443"],
        basic_auth=("elastic", "!@ContaElastic")
    )

    index = "test12345678910"

    mapping = {
        "properties": {
            "COD": {"type": "keyword"},
            "SHP": {"type": "keyword"},
            "coordinate": {"type": "geo_shape"},
            "order": {"type": "integer"},
            "direction": {"type": "double"},
            "percentage": {"type": "double"}
        }
    }

    es.indices.create(index=index, mappings=mapping)

    es.indices.refresh(index=index)

    helpers.bulk(es, dictThing, index=index)


def parseShapeFile1(filePath):
    df = pd.read_json(filePath)

    df["LAT"].replace(",", ".", regex=True, inplace=True)
    df["LAT"] = df["LAT"].apply(pd.to_numeric)
    df["LON"].replace(",", ".", regex=True, inplace=True)
    df["LON"] = df["LON"].apply(pd.to_numeric)

    df['point'] = df.apply(lambda row: Point(
        latitude=row['LAT'], longitude=row['LON']), axis=1)

    df.drop('LON', axis=1, inplace=True)
    df.drop('LAT', axis=1, inplace=True)

    df['point_next'] = df.groupby(['COD', 'SHP'])[
        'point'].shift(1)
    df.loc[df['point_next'].isna(), 'point_next'] = None
    df['distance_m'] = df.apply(lambda row: distance.geodesic(
        row['point'], row['point_next']).m if row['point_next'] is not None else float(0), axis=1)

    print(df)

    df['point_last'] = df.groupby(['COD', 'SHP'])[
        'point'].shift(-1)
    df.loc[df['point_last'].isna(), 'point_last'] = None
    df['distance_m_last'] = df.apply(lambda row: distance.geodesic(
        row['point'], row['point_last']).m if row['point_last'] is not None else float(0), axis=1)

    df = df[(
        (np.abs(stats.zscore(df['distance_m'])) < 6) &
        (np.abs(stats.zscore(df['distance_m_last'])) < 6)
    )]

    df['direction'] = df.apply(lambda row: util.calc_bearing(
        (row['point'].latitude, row['point'].longitude), (row['point_next'].latitude, row['point_next'].longitude)) if row['point_next'] is not None else float(0), axis=1)

    df.drop('point_next', axis=1, inplace=True)
    df.drop('point_last', axis=1, inplace=True)

    geojson.geometry.DEFAULT_PRECISION = 16
    df['coordinate'] = df.apply(
        lambda row: geojson.Point(
            (row['point'].longitude, row['point'].latitude)),
        axis=1)

    df = df.groupby(['COD', 'SHP']).coordinate.apply(list).reset_index()

    df['coordinate'] = df.apply(
        lambda row: geojson.LineString(row["coordinate"]), axis=1)

    print(df)

    dictThing = df.to_dict(orient='records')

    client = pymongo.MongoClient(
        "mongodb://contaMongo:!%40MongoConta@tccurbstads.com:27017/?authMechanism=DEFAULT")

    db = client["urbs"]

    collection = db["shapes"]

    collection.insert_many(dictThing)

    # es = Elasticsearch(
    #     hosts=["https://elastic.tccurbstads.com:443"],
    #     basic_auth=("elastic", "!@ContaElastic")
    # )

    # index = "testshape"

    # mapping = {
    #     "properties": {
    #         "COD": {"type": "keyword"},
    #         "SHP": {"type": "keyword"},
    #         "coordinate": {"type": "geo_shape"}
    #     }
    # }

    # es.indices.create(index=index, mappings=mapping)

    # es.indices.refresh(index=index)

    # helpers.bulk(es, dictThing, index=index)


parseShapeFile1('./data/shape.json')
