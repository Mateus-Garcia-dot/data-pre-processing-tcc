from elasticsearch import Elasticsearch
from elasticsearch import helpers
from process import parseShapeFile1
from plot import splitCordinatesOnOutliers

es = Elasticsearch(
    hosts=["https://elastic.tccurbstads.com:443"],
    basic_auth=("elastic", "!@ContaElastic")
)

index = "outlier-prone-shapes"

shapesParsed = parseShapeFile1('./data/shape.json')


mapping = {
    "properties": {
        "COD": {"type": "keyword"},
        "shape": {"type": "geo_shape"}
    }
}

# es.indices.create(index=index, mappings=mapping)

# es.indices.refresh(index=index)

# for x in range(len(shapesParsed)):
#     shapesParsed[x]['shape']['coordinates'] = splitCordinatesOnOutliers(
#         shapesParsed[x]['shape']['coordinates'])

# thing = ({"_op_type": "create", "_source": doc} for doc in shapesParsed)

# helpers.bulk(es, thing, index=index)
