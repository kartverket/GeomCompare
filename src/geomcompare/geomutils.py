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

_geom_type_mapping = {"LinearRing": 101, # ogr.wkbLinearRing
                      "LineString": 2, # ogr.wkbLineString
                      "MultiLineString": 5, #ogr.wkbMultiLineString
                      "MultiPoint": 4, # ogr.wkbMultiPoint
                      "MultiPolygon": 6, # ogr.wkbMultiPolygon
                      "Point": 1, # ogr.wkbPoint
                      "Polygon": 3, # ogr.wkbPolygon
                      "GeometryCollection": 7, # ogr.wkbGeometryCollection
                      }
_geom_type_mapping.update({v: k for k,v in _geom_type_mapping.items()})

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


_to_2D: Callable[[GeomObject], GeomObject] = partial(
    shapely.ops.transform, lambda *geom_coords: geom_coords[:2]
)

# Wrap _to_2D function to add documentation and type hints for
# end-users.
def to_2D(geom: GeomObject) -> GeomObject:
    """Remove the third dimension of a geometrical object's coordinates.

    Parameters
    ----------
    geom : `GeomObject`
        Shapely geometrical object with XYZ-coordinates.

    Returns
    -------
    `GeomObject`
        Geometrical object with its Z-coordinates removed.
    """
    return _to_2D(geom)

def get_transform_func(
    epsg_in: int, epsg_out: int
) -> Callable[[GeomObject], GeomObject]:
    """Get function to transform a geometrical object to another SRS.

    Create and return a function that transforms the XY-coordinates of
    `GeomObject` instances from one spatial reference system to
    another. The function identifies input and output spatial
    reference systems by the EPSG code.

    Parameters
    ----------
    epsg_in : `int`
        EPSG code of the input spatial reference system.
    epsg_out : `int`
        EPSG code of the output spatial reference system.

    Returns
    -------
    `callable`
        Function that takes one `GeomObject` as positional argument
        and returns the `GeomObject` with its XY-coordinates
        transformed to the output spatial reference system.
    """
    crs_in = pyproj.CRS(f"EPSG:{epsg_in}")
    crs_out = pyproj.CRS(f"EPSG:{epsg_out}")
    project = pyproj.Transformer.from_crs(crs_in, crs_out,
                                          always_xy=True).transform
    return partial(shapely.ops.transform, project)

def _unchanged_geom(geom: GeomObject) -> GeomObject:
    return geom
