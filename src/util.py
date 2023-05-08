import math


def calc_bearing(pointA, pointB):
    deg2rad = math.pi / 180
    latA = pointA[0] * deg2rad
    latB = pointB[0] * deg2rad
    lonA = pointA[1] * deg2rad
    lonB = pointB[1] * deg2rad

    delta_ratio = math.log(
        math.tan(latB / 2 + math.pi / 4) / math.tan(latA / 2 + math.pi / 4))
    delta_lon = abs(lonA - lonB)

    delta_lon %= math.pi
    bearing = math.atan2(delta_lon, delta_ratio)/deg2rad
    return bearing
