# -*- coding: utf-8 -*-

import inspect
import itertools
import logging
import os
import sys

# from collections import defaultdict
from collections.abc import Iterable, Sequence, Generator
from numbers import Integral
from typing import Literal, NamedTuple, Optional, TypeVar

try:
    from osgeo import ogr, osr

    ogr.UseExceptions()
except ImportError:
    pass
import psycopg2
from shapely import wkb
from shapely.geometry import (
    LinearRing,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
)
from shapely.geometry.base import BaseGeometry

from .geomutils import geom_type_mapping, get_transform_func, unchanged_geom


def setup_logger(
    name: Optional[str] = None, level: int = logging.INFO, show_pid: bool = False
) -> logging.Logger:
    """Setup the logging configuration for a Logger.

    Return a ready-configured logging.Logger instance which will write
    to 'stdout'.


    Keyword arguments:

    name: name of the logging.Logger instance to get. Default is the
    filename where the function is called.
    level: logging level to set to the returned logging.Logger
    instance. Default is logging.INFO.
    show_pid: show the process ID in the log records. Default is
    False.
    """
    if name is None:
        name = os.path.basename(inspect.stack()[1].filename)
    ## Get logger.
    logger = logging.getLogger(name)
    ## Remove existing handlers.
    for handler in logger.handlers:
        logger.removeHandler(handler)
    if level is None:
        logger.disabled = True
        return logger
    ## Set basic logging configuration.
    if show_pid:
        logger.show_pid = True
        pid = f"(PID: {os.getpid()}) "
    else:
        logger.show_pid = False
        pid = ""
    if level <= logging.DEBUG:
        fmt = (
            f"%(asctime)s - %(levelname)s - %(name)s {pid}in %(funcName)s "
            "(l. %(lineno)d) - %(message)s"
        )
    else:
        fmt = "%(asctime)s - %(levelname)s " f"- %(name)s {pid}- %(message)s"
    formatter = logging.Formatter(fmt)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.setLevel(level)
    return logger


def update_logger(logger: logging.Logger, **kwargs) -> None:
    """Update the configuration of a logging.Logger instance.


    Positional arguments:

    logger: logging.Logger instance to be updated.

    Keyword arguments:

    Configuration parameters of the logging.Logger instance to update,
    such as "level" or "show_pid". See function "setup_logger".
    """
    level = kwargs.get("level", logger.getEffectiveLevel())
    if level is None:
        logger.disabled = True
        return
    elif "level" in kwargs.keys():
        logger.disabled = False
    ## Set basic logging configuration.
    if not hasattr(logger, "show_pid"):
        logger.show_pid = False
    show_pid = kwargs.get("show_pid", logger.show_pid)
    if show_pid:
        pid = f"(PID: {os.getpid()}) "
    else:
        pid = ""
    if level <= logging.DEBUG:
        fmt = (
            f"%(asctime)s - %(levelname)s - %(name)s {pid}in %(funcName)s "
            "(l. %(lineno)d) - %(message)s"
        )
    else:
        fmt = "%(asctime)s - %(levelname)s " f"- %(name)s {pid}- %(message)s"
    formatter = logging.Formatter(fmt)
    for handler in logger.handlers:
        handler.setFormatter(formatter)
    logger.setLevel(level)


class ConnectionParameters(NamedTuple):
    """Parameters to open a connection to a PostGIS database."""

    host: str
    dbname: str
    user: str
    password: str
    port: int


class SchemaTableColumn(NamedTuple):
    """Location of a geometry column in a PostGIS database."""

    schema: str
    table: str
    column: str


def fetch_geoms_from_pg(
    conn: Optional[psycopg2.extensions.connection] = None,
    conn_params: Optional[ConnectionParameters] = None,
    sql_query: Optional[str] = None,
    geoms_col_loc: Optional[SchemaTableColumn] = None,
    aoi: Optional[BaseGeometry] = None,
    aoi_epsg: Optional[int] = None,
    output_epsg: Optional[int] = None,
):
    """Fetch geometrical features from a PostGIS database.

    Generator function which connects or uses an existing connection
    to a PostGIS database, and yields geometrical features from
    specified geometry column (within a given area or not), or based
    on a user-defined SQL query. If the connection to the database is
    opened by the function, it will be closed automatically after the
    last geometrical feature is yielded.


    Keyword parameters:

    conn: pre-opened connection to the PostGIS database
    ("psycopg2.extensions.connection" instance). Default is None.
    conn_params: connection parameters ("ConnectionParameters"
    instance) to open a connection to the PostGIS database. Default is
    None.
    sql_query: SQL query to use to extract geometrical features from
    the PostGIS database. Note that if this parameter is specified by
    the user, it will override and ignore all other keyword parameters
    except from "conn" and "conn_params". Default is None.
    geoms_col_loc: geometry column location ("SchemaTableColumn"
    instance) within the PostGIS database. Default is None.
    aoi: area of interest, where the geometrical features lies. This
    area must be defined by an instance of the
    "shapely.geometry.base.BaseGeometry" class. Default is None.
    aoi_epsg: EPSG code of the area of interest geometry/ies. Default
    is None.
    output_epsg: EPSG code of the yielded geometrical features. This
    parameter can be used to transform the yielded geometries to a
    different Spatial Reference System from the one used in the
    PostGIS database. Default is None.
    """
    if conn is None:
        if conn_params is None:
            raise ValueError("'conn' and 'conn_params' cannot both be passed None!")
        conn = psycopg2.connect(**conn_params._asdict())
        close_conn = True
    else:
        close_conn = False
    cursor = conn.cursor()
    if sql_query is None:
        if geoms_col_loc is None:
            raise ValueError(
                "'sql_query' and 'geoms_col_loc' cannot both be passed None!"
            )
        if aoi is not None or output_epsg is not None:
            cursor.execute(
                f"SELECT Find_SRID('{geoms_col_loc.schema}', "
                f"'{geoms_col_loc.table}', "
                f"'{geoms_col_loc.column}');"
            )
            pg_epsg = int(cursor.fetchone()[0])
        where_filter = f"WHERE {geoms_col_loc.column} IS NOT NULL"
        if aoi is not None:
            if aoi_epsg is not None and int(aoi_epsg) != pg_epsg:
                transform_aoi = get_transform_func(aoi_epsg, pg_epsg)
                aoi = transform_aoi(aoi)
            spatial_filter = (
                f" AND ST_Intersects({geoms_col_loc.column}, "
                f"ST_GeomFromText('{aoi.wkt}', {pg_epsg}));"
            )
        else:
            spatial_filter = ";"
        if output_epsg is not None and int(output_epsg) != pg_epsg:
            geoms_col_loc = geoms_col_loc._replace(
                column=(f"ST_Transform({geoms_col_loc.column}, " f"{output_epsg})")
            )
        sql_query = (
            f"SELECT ST_AsBinary({geoms_col_loc.column}) "
            f"FROM {geoms_col_loc.schema}.{geoms_col_loc.table} "
            f"{where_filter}{spatial_filter}"
        )
    cursor.execute(sql_query)
    for row in cursor:
        yield wkb.loads(row[0].tobytes())
    if close_conn:
        conn = None


def _get_layer_epsg(layer):
    """Extract and return the EPSG code of an ogr.Layer. Return None
    if not found.
    """
    lyr_srs = layer.GetSpatialRef()
    if lyr_srs is not None and lyr_srs.AutoIdentifyEPSG() == 0:
        return int(lyr_srs.GetAuthorityCode(None))
    else:
        return None


## Type for identifying layers.
LayerID = TypeVar("LayerID", str, int)

## Type for specifing a sequence of layers.
Layers = Sequence[LayerID]


class LayerFilter(NamedTuple):
    """Filter for extraction of geometrical features from file.

    Instances of this class are intended to be used as parameter for
    the "extract_geoms_from_file" function, for filtering and choosing
    the geometrical features to extract.

    Attributes:

    layer_id: LayerID type to identify which layer the filter will be
    applied to. If set to None (default), the filter will be applied
    on all layers.
    aoi: area of interest, where the geometrical features lies. All
    features lying outside the area of interest will be filtered out
    (not extracted). This area must be defined by an instance of the
    "shapely.geometry.base.BaseGeometry" class. Default is None.
    aoi_epsg: EPSG code of the area of interest geometry/ies. Default
    is None (the same Spatial Reference System as the layer will be
    used).
    attr_filter: valid string representation of an attribute filter
    (e.g. "attr_name = 'value'"). Default is None.
    fids: IDs of the features to extract from the layer. This
    parameter will be ignored if either the "aoi" or the "attr_filter"
    parameters are specified by the user.
    """

    layer_id: Optional[LayerID] = None
    aoi: Optional[BaseGeometry] = None
    aoi_epsg: Optional[int] = None
    attr_filter: Optional[str] = None
    fids: Optional[Sequence[int]] = None


## Type for specifying a sequence of LayerFilter instances.
Filters = Sequence[LayerFilter]


def extract_geoms_from_file(
    filename: str,
    driver_name: str,
    layers: Optional[Layers] = None,
    layer_filters: Optional[Filters] = None,
):
    """Extract geometrical features from a GDAL/OGR-readable file.

    Generator function which opens a file located on disk, with one of
    the existing GDAL/OGR drivers, and yields geometrical features,
    from one or several layers. The function also permits the use of
    filters to allow for fine-grained extraction of the geometrical
    features.


    Positional parameters:

    filename: path to the file to extract the geometrical features from.
    driver_name: name of the GDAL/OGR driver to use for opening the
    file. See https://gdal.org/drivers/vector/index.html for a list of
    the supported drivers.

    Keyword parameters:

    layers: Layers type to specify a sequence of layers (identified by
    the "LayerID" type) from which the geometrical features will be
    extracted. Default is None (geometrical features will be extracted
    from all layers).
    layer_filters: Filters type to specify a sequence of filters (see
    "LayerFilter" class) to apply to the layer(s) when extracting the
    geometrical features.
    """
    logger = setup_logger()
    try:
        from osgeo import ogr

        ogr.UseExceptions()
    except ImportError:
        raise NotImplementedError(
            "You must install GDAL/OGR and its Python "
            "bindings to call "
            f"{inspect.stack()[0].function!r}!"
        )
    if not os.path.exists(filename):
        raise ValueError(f"The file {filename!r} does not exist!")
    driver = ogr.GetDriverByName(driver_name)
    if driver is None:
        raise ValueError(
            f"The driver {driver_name!r} is not available or does not exist!"
        )
    ds = driver.Open(filename)
    if layers is not None:
        if not isinstance(layers, Sequence) or isinstance(layers, str):
            raise ValueError(
                "'layers' must be passed an iterable of layer names/indices!"
            )
    else:
        layers = range(ds.GetLayerCount())
    filters_mapping = dict()
    if layer_filters is not None:
        for lf in layer_filters:
            filters_mapping[lf.layer_id] = lf
    for lyr in layers:
        lyr_obj = ds.GetLayer(lyr)
        lyr_filter = filters_mapping.get(lyr, filters_mapping.get(None))
        if lyr_filter is None:
            lyr_aoi = None
            lyr_aoi_epsg = None
            lyr_attr_filter = None
            lyr_fids = None
        else:
            lyr_aoi = lyr_filter.aoi
            lyr_aoi_epsg = lyr_filter.aoi_epsg
            lyr_attr_filter = lyr_filter.attr_filter
            lyr_fids = lyr_filter.fids
        if lyr_aoi is not None:
            if lyr_aoi_epsg is not None:
                lyr_aoi_epsg = int(lyr_aoi_epsg)
                lyr_epsg = _get_layer_epsg(lyr_obj)
                if lyr_epsg is not None and lyr_epsg != lyr_aoi_epsg:
                    transform_aoi = get_transform_func(lyr_aoi_epsg, lyr_epsg)
                    lyr_aoi = transform_aoi(lyr_aoi)
            lyr_obj.SetSpatialFilter(ogr.CreateGeometryFromWkt(lyr_aoi.wkt))
        if lyr_attr_filter is not None:
            lyr_obj.SetAttributeFilter(lyr_attr_filter)
        if lyr_aoi is None and lyr_attr_filter is None and lyr_fids is not None:
            for fid in lyr_fids:
                feature = lyr_obj.GetFeature(fid)
                geom = feature.GetGeometryRef()
                yield wkb.loads(bytes(geom.ExportToWkb()))
        else:
            for feature in lyr_obj:
                geom = feature.GetGeometryRef()
                yield wkb.loads(bytes(geom.ExportToWkb()))
    ds = None


## Type for specifying an Iterable of geometrical features.
GeometryIterable = Iterable[BaseGeometry]


def write_geoms_to_file(
    geoms_iter: GeometryIterable,
    geoms_epsg: int,
    filename: str,
    driver_name: str,
    layer: Optional[LayerID] = None,
    mode: Literal["update", "overwrite"] = "update",
):
    """Write multiple geometrical features to disk.

    The function takes as input an iterable of geometrical features
    and writes them to disk using one of the existing GDAL/OGR
    drivers.


    Positional parameters:

    geoms_iter: iterable of geometrical features
    ("shapely.geometry.base.BaseGeometry" instances).
    geoms_epsg: EPSG code of the input geometrical features. If the
    Spatial Reference System of the input geometrical features is
    different from that of the layer they will written to (in case of
    an update, see "mode" parameter), the coordinates of the
    geometries will be reprojected to the layer's Spatial Reference
    System.
    filename: path to the output file where the geometrical features
    will be written to.

    Keyword parameters:

    layer: layer name on which to write the input geometries. In case
    of a file update (see "mode" parameter), the index of an existing
    layer can be passed as argument. If layer is set to "None"
    (default), the geometrical features will be written, in "update"
    mode, on the first layer available (at index 0), if any. If no
    layer is available, as well as in "overwrite" mode, the layer
    parameter set to "None" will result in the function writing the
    input geometries to a layer named "default" (if the driver
    supports named layers).
    mode: one of "update" or "overwrite". If set to "update", the
    function will update an existing file, or will create it if it
    does not exist. If set to "overwrite", the function will delete
    any file at the path set to the "filename" parameter, and will
    create a new file at this same location.
    """
    logger = setup_logger()
    try:
        from osgeo import ogr, osr

        ogr.UseExceptions()
    except ImportError:
        raise NotImplementedError(
            "You must install GDAL/OGR and its Python bindings to call "
            f"{inspect.stack()[0].function!r}!"
        )
    driver = ogr.GetDriverByName(driver_name)
    geoms_epsg = int(geoms_epsg)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(geoms_epsg)
    geoms_iter = iter(geoms_iter)
    #    geoms_list = iter(geoms_iter)
    #    if not len(set(g.__class__ for g in geoms_list)) == 1:
    #        raise ValueError("Cannot process input geometries of different types!")
    first_geom = next(geoms_iter)
    geom_type = geom_type_mapping[first_geom.geom_type]
    geoms_iter = itertools.chain([first_geom], geoms_iter)
    if not mode in ("update", "overwrite"):
        raise ValueError(
            "Wrong value for the 'mode' argument: must be either 'update' or "
            "'overwrite'!"
        )
    if mode == "update":
        _update_geoms_file(
            geoms_iter, geom_type, geoms_epsg, srs, filename, driver, layer, logger
        )
    else:
        _write_geoms_file(geoms_iter, geom_type, srs, filename, driver, layer, logger)


def _update_geoms_file(
    geoms_iter, geom_type, geoms_epsg, srs, filename, driver, layer, logger
):
    """Update or a create a new file on disk and add input geometries."""
    ds = driver.Open(filename, 1)
    if ds is None:
        _write_geoms_file(
            geoms_iter, geom_type, srs, filename, driver, layer_name, logger
        )
        return
    if layer is not None:
        lyr_obj = ds.GetLayer(layer)
        if lyr_obj is None and isinstance(layer, Integral):
            raise ValueError(f"The layer with index {layer!r} does not exist.")
    else:
        layer = "default"
        lyr_obj = ds.GetLayer()
    transform_geom = unchanged_geom
    if lyr_obj is None:
        lyr_obj = ds.CreateLayer(layer, srs=srs, geom_type=geom_type)
        lyr_def = lyr_obj.GetLayerDefn()
    else:
        lyr_def = lyr_obj.GetLayerDefn()
        lyr_epsg = _get_layer_epsg(lyr_obj)
        if lyr_epsg is not None and lyr_epsg != geoms_epsg:
            logger.info(
                f"The spatial reference system of the output file {filename!r}, "
                f"layer {layer!r}, is different from that of the input geometry "
                "features. The geometry features will be reprojected before being "
                "added to the file."
            )
            transform_geom = get_transform_func(geoms_epsg, lyr_epsg)
        else:
            logger.info(
                f"The spatial reference system of the output file {filename!r}, "
                f"layer {layer!r}, could not be found or identified. Input geometry "
                "features will be added to the file without transformation."
            )
    for geom in geoms_iter:
        feature = ogr.Feature(lyr_def)
        feature.SetGeometry(ogr.CreateGeometryFromWkt(transform_geom(geom).wkt))
        print(lyr_obj.CreateFeature(feature))
        feature = None
    ds = None


def _write_geoms_file(geoms_iter, geom_type, srs, filename, driver, layer, logger):
    """Delete any existing path at given, create a new file on disk
    and add input geometries.
    """
    if isinstance(layer, Integral):
        raise TypeError(
            "You cannot create a new layer by index, you must specify a layer name."
        )
    if os.path.exists(filename):
        driver.DeleteDataSource(filename)
    ds = driver.CreateDataSource(filename)
    if layer is None:
        layer = "default"
    lyr_obj = ds.CreateLayer(layer, srs=srs, geom_type=geom_type)
    lyr_def = lyr_obj.GetLayerDefn()
    for geom in geoms_iter:
        feature = ogr.Feature(lyr_def)
        feature.SetGeometry(ogr.CreateGeometryFromWkt(geom.wkt))
        lyr_obj.CreateFeature(feature)
        feature = None
    ## Close the output file.
    ds = None
