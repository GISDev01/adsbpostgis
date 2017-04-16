from math import radians, cos, sin, asin, sqrt


def haversine_distance_meters(lon1, lat1, lon2, lat2):
    """
    Temporary stand-in function until implementing PostGIS distance via REST API, which is more accurate
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # TODO: Verify input lat/long pairs are valid

    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    diff_lon = lon2 - lon1
    diff_lat = lat2 - lat1

    calc = sin(diff_lat / 2.0) ** 2 + cos(lat1) * cos(lat2) * sin(diff_lon / 2.0) ** 2
    straight = 2 * asin(sqrt(calc))
    # Radius of earth in meters
    radius_meters = 6371000

    return straight * radius_meters
