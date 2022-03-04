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


def _setup_logger(
    name: Optional[str] = None, level: int = logging.INFO, show_pid: bool = False
) -> logging.Logger:
    """Setup the logging configuration for a Logger.

    Return a ready-configured logging.Logger instance which will write
    to 'stdout'.

    Parameters
    ----------
    name : `str`, optional
        Name of the logging.Logger instance to get. Default is the filename
        where the function is called.
    level : `int`, default: ``logging.INFO``
        Logging level to set to the returned logging.Logger instance.
    show_pid : `bool`, default: ``False``
        Show the process ID in the log records.

    Returns
    -------
    `logging.Logger`
        Ready-configured Logger
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


def _update_logger(logger: logging.Logger, **kwargs) -> None:
    """Update the configuration of a logging.Logger instance.

    Parameters
    ----------
    logger : logging.Logger
        Logger instance to be updated.

    Keyword arguments:

    Configuration parameters of the logging.Logger instance to update,
    such as "level" or "show_pid". See function "_setup_logger".
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
    """Parameters to open a connection to a PostGIS database.

    Instances of this class are intended to be used as parameter for
    the :py:func:`fetch_geoms_from_pg` function.

    Attributes
    ----------
    .. host : `str`
           Database host address.
    .. dbname : `str`
           Database name.
    .. user : `str`
           User name used to authenticate.
    .. password : `str`
           Password used to authenticate.
    .. port : `int`, default: ``5432``
           Connection port number.
    """
    #: Database host address.
    host: str
    #: Database name.
    dbname: str
    #: User name used to authenticate.
    user: str
    #: Password used to authenticate.
    password: str
    #: Connection port number.
    port: int = 5432


class SchemaTableColumn(NamedTuple):
    """Location of a geometry column in a PostGIS database.

    Instances of this class are intended to be used as parameter for
    the :py:func:`fetch_geoms_from_pg` function.

    Attributes
    ----------
    .. schema : `str`
           Schema name of the PostGIS database, where the table
           containing the geometrical features is located.
    .. table : `str`
           Table name, where the geometrical features can be found.
    .. column : `str`
           Column name, where the geometrical features can be found.
    """
    #: Schema name of the PostGIS database, where the table containing
    #: the geometrical features is located.
    schema: str
    #: Table name, where the geometrical features can be found.
    table: str
    #: Column name, where the geometrical features can be found.
    column: str


def fetch_geoms_from_pg(
    conn: Optional[psycopg2.extensions.connection] = None,
    conn_params: Optional[ConnectionParameters] = None,
    sql_query: Optional[str] = None,
    geoms_col_loc: Optional[SchemaTableColumn] = None,
    aoi: Optional[BaseGeometry] = None,
    aoi_epsg: Optional[int] = None,
    output_epsg: Optional[int] = None,
) -> Generator[BaseGeometry]:
    """Fetch geometrical features from a PostGIS database.

    Generator function which connects or uses an existing connection to a
    PostGIS database, and yields geometrical features from specified
    geometry column (within a given area or not), or based on a
    user-defined SQL query. If the connection to the database is opened by
    the function, it will be closed automatically after the last
    geometrical feature is yielded.

    Parameters
    ----------
    conn : `psycopg2.extensions.connection`, optional
        Pre-opened connection to the PostGIS database.
    conn_params : `ConnectionParameters`, optional
        Parameters to open a connection to the PostGIS database.
    sql_query : `str`, optional
        SQL query to use to extract geometrical features from the PostGIS database.
    geoms_col_loc : `SchemaTableColumn`, optional
        Geometry column location within the PostGIS database.
    aoi : `shapely.geometry.base.BaseGeometry`, optional
        *Area of interest*, where the geometrical features lies.
    aoi_epsg : `int`, optional
        EPSG code of the *area of interest* geometry/ies.
    output_epsg : `int`, optional
        EPSG code of the yielded geometrical features. This parameter can
        be used to reproject the yielded geometries to a different Spatial
        Reference System from the one used in the PostGIS database.

    Yields
    ------
    shapely.geometry.base.BaseGeometry
        Geometrical/Geographical features from the PostGIS database.

    Raises
    ------
    ValueError
        If both ``conn`` and ``conn_params`` parameters are not passed
        an argument different from `None`.
    ValueError
        If both ``sql_query`` and ``geoms_col_loc`` parameters are not
        passed an argument different from `None`.

    Notes
    -----
    In the case where the ``sql_query`` parameter is given, the parameters
    ``geoms_col_loc``, ``aoi``, ``aoi_epsg`` and ``output_epsg`` will be
    ignored, as SQL queries can include filtering and reprojection.
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


#: Type for identifying layers.
LayerID = TypeVar("LayerID", str, int)


class LayerFilter(NamedTuple):
    """Filter for extraction of geometrical features from file.

    Instances of this class are intended to be used as parameter for
    the :py:func:`extract_geoms_from_file` function, for filtering and
    choosing the geometrical features to extract.

    Attributes
    ----------
    .. layer_id : `LayerID`, optional
           Name or index of the layer the filter will be applied to. If set
           to `None`, the filter will be applied on all layers.
    .. aoi : `shapely.geometry.base.BaseGeometry`, optional
           *Area of interest*, where the geometrical features lies. All
           features lying outside the *area of interest* will be filtered
           out (not extracted).
    .. aoi_epsg : `int`, optional
           EPSG code of the *area of interest* geometry/ies. If set to
           `None`, the same Spatial Reference System as the layer will be
           used.
    .. attr_filter : `str`, optional
           Valid string representation of an attribute filter
           (e.g. ``"attr_name = 'value'"``).
    .. fids : sequence of `int`, optional
           IDs of the features to extract from the layer. This parameter
           will be ignored if either the ``aoi`` or the ``attr_filter``
           parameters are specified by the user.
    """
    #: Name or index of the layer the filter will be applied to. If set to
    #: `None`, the filter will be applied on all layers.
    layer_id: Optional[LayerID] = None

    #: shapely.geometry.base.BaseGeometry, optional: *Area of interest*,
    #: where the geometrical features lies. All features lying outside
    #: the *area of interest* will be filtered out (not extracted).
    aoi: Optional[BaseGeometry] = None

    #: EPSG code of the *area of interest* geometry/ies. If set to `None`,
    #: the same Spatial Reference System as the layer will be used.
    aoi_epsg: Optional[int] = None

    #: Valid string representation of an attribute filter
    #: (e.g. ``"attr_name = 'value'"``).
    attr_filter: Optional[str] = None

    #: IDs of the features to extract from the layer. This parameter will
    #: be ignored if either the ``aoi`` or the ``attr_filter`` parameters
    #: are specified by the user.
    fids: Optional[Sequence[int]] = None


def extract_geoms_from_file(
    filename: str,
    driver_name: str,
    layers: Optional[Layers] = None,
    layer_filters: Optional[Filters] = None,
):
    """Extract geometrical features from a GDAL/OGR-readable file.

    Generator function which opens a file located on disk, with one of the
    existing `GDAL/OGR drivers
    <https://gdal.org/drivers/vector/index.html>`_, and yields geometrical
    features, from one or several layers. The function also permits the use
    of filters to allow for fine-grained extraction of the geometrical
    features.

    Parameters
    ----------
    filename : `str`
        Path to the file to extract the geometrical features from.
    driver_name : str
        Name of the GDAL/OGR driver to use for opening the file.
    layers : sequence of `LayerID`, optional
        Layers from which the geometrical features will be extracted. If
        set to `None` (default), geometrical features will be extracted
        from all layers.
    layer_filters: sequence of `LayerFilter`, optional
        Filters to apply to the layer(s) when extracting the geometrical
        features.

    Yields
    ------
    `shapely.geometry.base.BaseGeometry`
        Geometrical/Geographical features from the file.

    Raises
    ------
    NotImplementedError
        If GDAL/OGR is not installed or not importable.
    """
    logger = _setup_logger()
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
    filename: str,
    driver_name: str,
    geoms_iter: GeometryIterable,
    geoms_epsg: Optional[int] = None,
    layer: Optional[LayerID] = None,
    mode: Literal["update", "overwrite"] = "update",
) -> None:
    """Write multiple geometrical features to disk.

    The function takes as input an iterable of geometrical features
    and writes them to disk using one of the existing `GDAL/OGR drivers
    <https://gdal.org/drivers/vector/index.html>`_.

    Parameters
    ----------
    filename : `str`
        Path to the output file where the geometrical features will be
        written to.
    driver_name : `str`
        Name of the GDAL/OGR driver to use for writing the file.
    geoms_iter : iterable of `shapely.geometry.base.BaseGeometry`
        Iterable of the geometrical/geographical features to write.
    geoms_epsg : `int`, optional
        EPSG code of the input geometrical features. If the Spatial
        Reference System of the input geometrical features is
        specified and differs from that of the layer they will
        written to (in case of an update, see``mode`` parameter), the
        coordinates of the geometries will be reprojected to the
        layer's Spatial Reference System. It is set to `None` as
        default (no Spatial Reference System).
    layer : `LayerID`, optional
        Layer name/index on which to write the input geometries. In
        case of a file update (see ``mode`` parameter), the index of
        an existing layer can be passed as argument. If layer is set
        to `None` (default), the geometrical features will be written,
        in ``update`` mode, on the first layer available (at index 0),
        if any. If no layer is available, as well as in ``overwrite``
        mode, the layer parameter set to `None` will result in the
        function writing the input geometries to a layer named
        ``default`` (if the driver supports named layers).
    mode : {``"update"``, ``"overwrite"``}
        If set to ``"update"``, the function will update an existing
        file, or will create it if it does not exist. If set to
        ``"overwrite"``, the function will delete any file at the path
        set to the ``filename`` parameter, and will create a new file
        at this same location.

    Returns
    -------
    `None`
    """
    logger = _setup_logger()
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
