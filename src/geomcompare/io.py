# -*- coding: utf-8 -*-

import os
from collections.abc import Sequence
from numbers import Integral
import itertools
import sys
import logging
import inspect

from osgeo import ogr, osr
ogr.UseExceptions()
from shapely import wkb
from shapely.geometry import (LinearRing, LineString, MultiLineString,
                              MultiPoint, MultiPolygon, Point, Polygon)
import shapely.ops
import pyproj
import psycopg2

from .geomutils import geom_type_mapping, get_transform_func


def setup_logger(name=None, level=logging.INFO, show_pid=False):
    """Setup the logging configuration for a Logger.

    Return a ready-configured logging.Logger instance which will write
    to 'stdout'.


    Keyword arguments:

    name: name of the logging.Logger instance to get. Default is the
    filename where the calling function is defined.
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
        fmt = ("%(asctime)s - %(levelname)s "
               f"- %(name)s {pid}in %(funcName)s (l. %(lineno)d) - "
               "%(message)s")
    else:
        fmt = ("%(asctime)s - %(levelname)s "
               f"- %(name)s {pid}- %(message)s")
    formatter = logging.Formatter(fmt)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.setLevel(level)
    return logger


def update_logger(logger, **kwargs):
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
        fmt = ("%(asctime)s - %(levelname)s "
               f"- %(name)s {pid}in %(funcName)s (l. %(lineno)d) - "
               "%(message)s")
    else:
        fmt = ("%(asctime)s - %(levelname)s "
               f"- %(name)s {pid}- %(message)s")        
    formatter = logging.Formatter(fmt)
    for handler in logger.handlers:
        handler.setFormatter(formatter)
    logger.setLevel(level)


def fetch_geoms_from_pg(schema, table, column, conn=None, host=None,
                        dbname=None, user=None, password=None, port=None,
                        aoi=None, aoi_epsg=None, output_epsg=None):
    if conn is None:
        for k,v in locals().items():
            if k not in ("conn", "aoi", "aoi_epsg", "output_epsg") and v is None:
                raise ValueError(f"Argument {k!r} must be passed a value "
                                 "different from None!")
        conn = psycopg2.connect(host=host, dbname=dbname, user=user,
                                password=password, port=port)
    else:
        for arg in ("schema", "table", "column"):
            if locals()[arg] is None:
                raise ValueError(f"Argument {arg!r} must be passed a value "
                                 "different from None!")
    cursor = conn.cursor()
    if aoi is not None or output_epsg is not None:
        cursor.execute(f"SELECT Find_SRID('{schema}', '{table}', '{column}');")
        pg_epsg = int(cursor.fetchone()[0])
    where_filter = f"WHERE {column} IS NOT NULL"
    if aoi is not None:
        if  aoi_epsg is not None and int(aoi_epsg) != pg_epsg:
            transform_aoi = get_transform_func(aoi_epsg, pg_epsg)
            aoi = transform_aoi(aoi)
        spatial_filter = (f" AND ST_Intersects({column}, "
                          f"ST_GeomFromText('{aoi.wkt}', {pg_epsg}));")
    else:
        spatial_filter = ";"
    if output_epsg is not None and int(output_epsg) != pg_epsg:
        column = f"ST_Transform({column}, {output_epsg})"
    sql_query = (f"SELECT ST_AsBinary({column}) FROM {schema}.{table} "
                 f"{where_filter}{spatial_filter}")
    cursor.execute(sql_query)
    for row in cursor:
        yield wkb.loads(row[0].tobytes())
    conn = None    


def extract_geoms_from_file(filename, driver_name, layers=None, FIDs=None):
    logger = setup_logger()
    if not os.path.exists(filename):
        raise ValueError(f"The file {filename!r} does not exist!")
    driver = ogr.GetDriverByName(driver_name)
    if driver is None:
        raise ValueError(f"The driver {driver_name!r} is not available or does "
                         "not exist!")
    ds = driver.Open(filename)
    if layers is not None:
        if not isinstance(layers, Sequence) or isinstance(layers, str):
            raise ValueError("'layers' must be passed an iterable of layer "
                             "names/indices!")
    else:
        layers = range(ds.GetLayerCount())
    if FIDs is not None:
        try:
            FIDs = dict(FIDs)
            for FID_seq in FIDs.values():
	            assert isinstance(FID_seq, Sequence)
	            assert all(isinstance(fid, Integral) for fid in FID_seq)
        except (TypeError, AssertionError):
            raise ValueError("Wrong format of the data passed to the "
                             "FIDs argument!")
    if FIDs is not None:
        for lyr in layers:
            lyr_obj = ds.GetLayer(lyr)
            for feature in FIDs[lyr]:
                geom = feature.GetGeometryRef()
                yield wkb.loads(bytes(geom.ExportToWkb()))
    else:
        for lyr in layers:
            lyr_obj = ds.GetLayer(lyr)
            for feature in lyr_obj:
                geom = feature.GetGeometryRef()
                yield wkb.loads(bytes(geom.ExportToWkb()))        
    ds = None
 

def write_geoms_to_file(geoms_iter, geoms_epsg, filename, driver_name,
                        layer_name, mode="update"):
    logger = setup_logger()
    driver = ogr.GetDriverByName(driver_name)
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
        raise ValueError("Wrong value for the 'mode' argument: must be either "
                         "'update' or 'overwrite'!")
    if mode == "update":
        _update_geoms_file(geoms_iter, geom_type, geoms_epsg, srs, filename, driver,
                           layer_name, logger)
    else:
        _write_geoms_file(geoms_iter, geom_type, srs, filename, driver,
                          layer_name, logger)


def _update_geoms_file(geoms_iter, geom_type, geoms_epsg, srs, filename, driver,
                       layer_name, logger):
    ds = driver.Open(filename, 1)
    if ds is None:
        _write_geoms_file(geoms_iter, geom_type, srs, filename, driver,
                          layer_name)
        return
    lyr_obj = ds.GetLayer(layer_name)
    transform = False                 
    if lyr_obj is None:
        lyr_obj = ds.CreateLayer(layer_name, srs=srs, geom_type=geom_type)
        lyr_def = lyr_obj.GetLayerDefn()        
    else:
        lyr_def = lyr_obj.GetLayerDefn()
        lyr_srs = lyr_obj.GetSpatialRef()
        if lyr_srs is not None and lyr_srs.AutoIdentifyEPSG() == 0:
            lyr_epsg = int(lyr_srs.GetAuthorityCode(None))
            if lyr_epsg != geoms_epsg:
                logger.info("The spatial reference system of the output file "
                            f"{filename!r}, layer {layer_name!r}, is different "
                            "from that of the input geometry features. The "
                            "geometry features will be reprojected before being "
                            "added to the file.")
                input_crs = pyproj.CRS(f"EPSG:{geoms_epsg}")
                output_crs = pyproj.CRS(f"EPSG:{lyr_epsg}")
                project = pyproj.Transformer.from_crs(input_crs, output_crs,
                                                      always_xy=True).transform
                transform = True
        else:
            logger.info("The spatial reference system of the output file "
                        f"{filename!r}, layer {layer_name!r}, could not be "
                        "found or identified. Input geometry features will be "
                        "added to the file without transformation.")
            transform = False
    if transform:
        for geom in geoms_iter:
            feature = ogr.Feature(lyr_def)
            feature.SetGeometry(ogr.CreateGeometryFromWkt(
                shapely.ops.transform(project, geom).wkt))
            lyr_obj.CreateFeature(feature)
            feature = None        
    else:
        for geom in geoms_iter:
            feature = ogr.Feature(lyr_def)
            feature.SetGeometry(ogr.CreateGeometryFromWkt(geom.wkt))
            lyr_obj.CreateFeature(feature)
            feature = None        
    ds = None


def _write_geoms_file(geoms_iter, geom_type, srs, filename, driver,
                      layer_name, logger):
    if os.path.exists(filename):
        driver.DeleteDataSource(filename)
    ds = driver.CreateDataSource(filename)
    lyr_obj = ds.CreateLayer(layer_name, srs=srs, geom_type=geom_type)
    lyr_def = lyr_obj.GetLayerDefn()
    for geom in geoms_iter:
        feature = ogr.Feature(lyr_def)
        feature.SetGeometry(ogr.CreateGeometryFromWkt(geom.wkt))
        lyr_obj.CreateFeature(feature)
        feature = None
    ## Close the output file.
    ds = None
