import numpy as np
from process import parseShapeFile1
import matplotlib.pyplot as plt
from geopy import distance


def calculateOutliers(arr, factor):
    q1, q3 = np.percentile(arr, [25, 75])
    iqr = q3 - q1
    k = factor
    lower_bound = q1 - k*iqr
    upper_bound = q3 + k*iqr
    outliers = [x for x in arr if x < lower_bound or x > upper_bound]
    return [(i, val) for i, val in enumerate(arr) if val in outliers]


def splitCordinatesOnOutliers(coordinates):
    meters = cauculateDistance(coordinates)
    outliers = calculateOutliers(meters, 8)
    return list(x.tolist() for x in np.split(coordinates, list(x[0]+1 for x in outliers)) if len(x) != 1)


def cauculateDistance(coordinates):
    meters = list()
    for first_coord, second_coord in zip(coordinates, coordinates[1:]):
        meters.append(distance.geodesic(first_coord, second_coord).m)
    return meters


def plotShapeOutliers(shapes, cod):
    coordinates = next(x['shape']['coordinates']
                       for x in shapes if x['COD'] == cod)
    meters = cauculateDistance(coordinates)
    outliers = calculateOutliers(meters, 8)
    x_vals, y_vals = zip(*enumerate(meters))
    if outliers:
        outlier_xvals, outlier_yvals = zip(*outliers)
        plt.scatter(outlier_xvals, outlier_yvals, marker='x', color='red')
    plt.scatter(x_vals, y_vals, marker='.', color='blue')
    plt.show()


def plotFixedShape(shapes, cod):
    coordinates = next(splitCordinatesOnOutliers(
        x['shape']['coordinates']) for x in shapes if x['COD'] == cod)
    meters = []
    for coordinate in coordinates:
        meters.extend(cauculateDistance(coordinate))
    outliers = calculateOutliers(meters, 8)
    x_vals, y_vals = zip(*enumerate(meters))
    if outliers:
        outlier_xvals, outlier_yvals = zip(*outliers)
        plt.scatter(outlier_xvals, outlier_yvals, marker='x', color='red')
    plt.scatter(x_vals, y_vals, marker='.', color='blue')
    plt.show()


if (__name__ == '__main__'):
    shapesParsed = parseShapeFile1('./data/shape.json')
    cod = input("Whats the cod: ")
    plotShapeOutliers(shapesParsed, cod)
    plotFixedShape(shapesParsed, cod)
