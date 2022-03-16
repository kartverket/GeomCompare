# -*- coding: utf-8 -*-


from functools import partial
from typing import Union
from collections.abc import Callable

from shapely.geometry import (
    LinearRing,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
)
import shapely.ops
import pyproj

geom_type_mapping = {"LinearRing": 101, # ogr.wkbLinearRing
                     "LineString": 2, # ogr.wkbLineString
                     "MultiLineString": 5, #ogr.wkbMultiLineString
                     "MultiPoint": 4, # ogr.wkbMultiPoint
                     "MultiPolygon": 6, # ogr.wkbMultiPolygon
                     "Point": 1, # ogr.wkbPoint
                     "Polygon": 3, # ogr.wkbPolygon
                     "GeometryCollection": 7, # ogr.wkbGeometryCollection
                     }
geom_type_mapping.update({v: k for k,v in geom_type_mapping.items()})

#: Type for :ref:`shapely geometrical objects <shapely:objects>`.
GeomObject = Union[
    LinearRing,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
]


to_2D = partial(shapely.ops.transform, lambda *geom_coords: geom_coords[:2])

def get_transform_func(
    epsg_in: int, epsg_out: int
) -> Callable[[GeomObject], GeomObject]:
    crs_in = pyproj.CRS("EPSG:{}".format(epsg_in))
    crs_out = pyproj.CRS("EPSG:{}".format(epsg_out))
    project = pyproj.Transformer.from_crs(crs_in, crs_out,
                                          always_xy=True).transform
    return partial(shapely.ops.transform, project)

def unchanged_geom(geom: GeomObject) -> GeomObject:
    return geom
