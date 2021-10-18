# -*- coding: utf-8 -*-


from functools import partial
import shapely
import pyproj
from osgeo import ogr

geom_type_mapping = {"LinearRing": ogr.wkbLinearRing,
                     "LineString": ogr.wkbLineString,
                     "MultiLineString": ogr.wkbMultiLineString,
                     "MultiPoint": ogr.wkbMultiPoint,
                     "MultiPolygon": ogr.wkbMultiPolygon,
                     "Point": ogr.wkbPoint,
                     "Polygon": ogr.wkbPolygon,
                     "GeometryCollection": ogr.wkbGeometryCollection}
geom_type_mapping.update({v: k for k,v in geom_type_mapping.items()})


to_2D = partial(shapely.ops.transform, lambda *geom_coords: geom_coords[:2])

def get_transform_func(epsg_in, epsg_out):
    crs_in = pyproj.CRS("EPSG:{}".format(epsg_in))
    crs_out = pyproj.CRS("EPSG:{}".format(epsg_out))
    project = pyproj.Transformer.from_crs(crs_in, crs_out,
                                          always_xy=True).transform
    return partial(shapely.ops.transform, project)

def unchanged_geom(geom):
    return geom
