#!/usr/bin/env python
# -*- coding: utf-8 -*-


import inspect
import logging
import multiprocessing as mp
import os
import sqlite3
import time
import uuid
from tempfile import NamedTemporaryFile
from typing import Optional, Literal
from collections.abc import Generator, Callable

import psycopg2
import pyproj
import rtree
import shapely.ops
from shapely.geometry.base import BaseGeometry
from pyproj.exceptions import CRSError
from shapely import speedups, wkb

from .comparefunc import geoms_always_match
from .geomrefdb_abc import GeomRefDB
from .geomutils import geom_type_mapping, get_transform_func, to_2D, unchanged_geom
from .io import _setup_logger, _update_logger, GeometryIterable
from .misc import SharedIterator, split_iter_to_lists


class PostGISGeomRefDB(GeomRefDB):
    def __init__(self, PG_params, PG_schema, PG_table, PG_geoms_column):

        self.PG_params = PG_params
        self.PG_conn = psycopg2.connect(**PG_params)
        self.PG_schema = PG_schema
        self.PG_table = PG_table
        self.PG_geoms_column = PG_geoms_column

    def __del__(self):
        self.PG_conn = None

    def __getstate__(self):
        attrs = self.__dict__.copy()
        attrs["PG_conn"] = None
        return attrs

    def __setstate__(self, state):
        self.__dict__ = state
        self.PG_conn = psycopg2.connect(**self.PG_params)

    def get_PG_geoms_EPSG(self):
        PG_cursor = self.PG_conn.cursor()
        PG_cursor.execute(
            f"SELECT Find_SRID('{self.PG_schema}', '{self.PG_table}', "
            f"'{self.PG_geoms_column}')"
        )
        res = PG_cursor.fetchone()
        PG_cursor = None
        return int(res[0])

    def true_positives(self, geoms_iter, geoms_EPSG, same_geoms_func):
        self.logger = _setup_logger()
        self.logger.info("Searching true positive geometries...")
        PG_geoms_EPSG = self.get_PG_geoms_EPSG()
        transform = PG_geoms_EPSG != int(geoms_EPSG)
        path2table = ".".join([self.PG_schema, self.PG_table])
        if transform:
            SQL_query_template = (
                f"SELECT ST_AsBinary(ST_Transform({self.PG_geoms_column},"
                f"{geoms_EPSG})) "
                f"FROM {path2table} "
                f"WHERE ST_Intersects({self.PG_geoms_column}, "
                f"ST_Transform(ST_GeomFromText('{{geom.wkt}}', {geoms_EPSG}), "
                f"{PG_geoms_EPSG}))"
            )
        else:
            SQL_query_template = (
                f"SELECT ST_AsBinary({self.PG_geoms_column}) "
                f"FROM {path2table} "
                f"WHERE ST_Intersects({self.PG_geoms_column}, "
                f"ST_GeomFromText('{{geom.wkt}}', {geoms_EPSG}))"
            )
        PG_cursor = self.PG_conn.cursor()
        for geom in geoms_iter:
            PG_cursor.execute(SQL_query_template.format(geom=geom))
            for row in PG_cursor:
                PG_geom = wkb.loads(row[0].tobytes())
                if same_geoms_func(geom, PG_geom):
                    yield geom
                    break
        PG_cursor = None
        logger.info("Done searching true positive geometries.")

    def false_positives(self, geoms_iter, geoms_EPSG, same_geoms_func):
        logger = _setup_logger()
        logger.info("Searching false positive geometries...")
        PG_geoms_EPSG = self.get_PG_geoms_EPSG()
        transform = PG_geoms_EPSG != int(geoms_EPSG)
        path2table = ".".join([self.PG_schema, self.PG_table])
        if transform:
            SQL_query_template = (
                f"SELECT ST_AsBinary(ST_Transform({self.PG_geoms_column},"
                f"{geoms_EPSG})) "
                f"FROM {path2table} "
                f"WHERE ST_Intersects({self.PG_geoms_column}, "
                f"ST_Transform(ST_GeomFromText('{{geom.wkt}}', "
                f"{geoms_EPSG}), {PG_geoms_EPSG}))"
            )
        else:
            SQL_query_template = (
                f"SELECT ST_AsBinary({self.PG_geoms_column}) "
                f"FROM {path2table} "
                f"WHERE ST_Intersects({self.PG_geoms_column}, "
                f"ST_GeomFromText('{{geom.wkt}}', {geoms_EPSG}))"
            )
        PG_cursor = self.PG_conn.cursor()
        for geom in geoms_iter:
            PG_cursor.execute(SQL_query_template.format(geom=geom))
            false_positive = True
            for row in PG_cursor:
                PG_geom = wkb.loads(row[0].tobytes())
                if same_geoms_func(geom, PG_geom):
                    false_positive = False
                    break
            if false_positive:
                yield geom
        PG_cursor = None
        logger.info("Done searching false positive geometries.")

    def missing_geometries(self, geoms_iter, AOI_geom, geoms_EPSG, same_geoms_func):
        logger = _setup_logger()
        logger.info("Searching missing geometries...")
        PG_geoms_EPSG = self.get_PG_geoms_EPSG()
        transform = PG_geoms_EPSG != int(geoms_EPSG)
        path2table = ".".join([self.PG_schema, self.PG_table])
        if transform:
            PG_crs = pyproj.CRS(f"EPSG:{PG_geoms_EPSG}")
            AOI_crs = pyproj.CRS(f"EPSG:{geoms_EPSG}")
            project = pyproj.Transformer.from_crs(
                AOI_crs, PG_crs, always_xy=True
            ).transform
            aoi_wkt = shapely.ops.transform(project, AOI_geom).wkt
            SQL_query = (
                f"SELECT ST_AsBinary(ST_Transform({self.PG_geoms_column},"
                f"{geoms_EPSG})) "
                f"FROM {path2table} "
                f"WHERE ST_Intersects({self.PG_geoms_column}, "
                f"ST_GeomFromText('{aoi_wkt}', {PG_geoms_EPSG}))"
            )
        else:
            SQL_query = (
                f"SELECT ST_AsBinary({self.PG_geoms_column}) "
                f"FROM {path2table} "
                f"WHERE ST_Intersects({self.PG_geoms_column}, "
                f"ST_GeomFromText('{AOI_geom.wkt}', {PG_geoms_EPSG}))"
            )
        PG_cursor = self.PG_conn.cursor()
        PG_cursor.execute(SQL_query)
        index = rtree.index.Index()
        for i, geom in enumerate(geoms_iter):
            index.insert(i, geom.bounds, obj=geom)
        for row in PG_cursor:
            PG_geom = wkb.loads(row[0].tobytes())
            if not any(
                same_geoms_func(el.object, PG_geom)
                for el in index.intersection(PG_geom.bounds, objects=True)
            ):
                yield PG_geom
        PG_cursor = None
        logger.info("Done searching missing geometries.")


class RtreeGeomRefDB(GeomRefDB):
    def __init__(self, geoms_iter, geoms_EPSG):
        self.index = rtree.index.Index()
        for i, geom in enumerate(geoms_iter):
            self.index.insert(i, geom.bounds, obj=geom)
        self.EPSG = geoms_EPSG

    def true_positives(self, geoms_iter, geoms_EPSG, same_geoms_func):
        logger = _setup_logger()
        logger.info("Searching true positive geometries...")
        transform = geoms_EPSG != self.EPSG
        if transform:
            geoms_crs = pyproj.CRS(f"EPSG:{geoms_EPSG}")
            ref_DB_crs = pyproj.CRS(f"EPSG:{self.EPSG}")
            project = pyproj.Transformer.from_crs(
                geoms_crs, ref_DB_crs, always_xy=True
            ).transform
            for geom in geoms_iter:
                geom_reproj = shapely.ops.transform(project, geom)
                if any(
                    same_geoms_func(geom_reproj, el.object)
                    for el in self.index.intersection(geom_reproj.bounds, objects=True)
                ):
                    yield geom
        else:
            for geom in geoms_iter:
                if any(
                    same_geoms_func(geom, el.object)
                    for el in self.index.intersection(geom.bounds, objects=True)
                ):
                    yield geom
        logger.info("Done searching true positive geometries.")

    def false_positives(self, geoms_iter, geoms_EPSG, same_geoms_func):
        logger = _setup_logger()
        logger.info("Searching false positive geometries...")
        transform = geoms_EPSG != self.EPSG
        if transform:
            geoms_crs = pyproj.CRS(f"EPSG:{geoms_EPSG}")
            ref_DB_crs = pyproj.CRS(f"EPSG:{self.EPSG}")
            project = pyproj.Transformer.from_crs(
                geoms_crs, ref_DB_crs_crs, always_xy=True
            ).transform
            for geom in geoms_iter:
                geom_reproj = shapely.ops.transform(project, geom)
                if not any(
                    same_geoms_func(geom_reproj, el.object)
                    for el in self.index.intersection(geom_reproj.bounds, objects=True)
                ):
                    yield geom
        else:
            for geom in geoms_iter:
                if not any(
                    same_geoms_func(geom, el.object)
                    for el in self.index.intersection(geom.bounds, objects=True)
                ):
                    yield geom
        logger.info("Done searching false positive geometries.")

    def intersecting_idx_geoms(self, poly=None, bounds=None):
        if poly is not None:
            for el in self.index.intersection(poly.bounds, objects=True):
                idx_geom = el.object
                if poly.intersects(idx_geom):
                    yield idx_geom
        else:
            if bounds is None:
                bounds = self.index.bounds
            for el in self.index.intersection(bounds, objects=True):
                yield el.object

    def missing_geometries(self, geoms_iter, AOI_geom, geoms_EPSG, same_geoms_func):
        logger = _setup_logger()
        logger.info("Searching missing geometries...")
        index = rtree.index.Index()
        for i, geom in enumerate(geoms_iter):
            index.insert(i, geom.bounds, obj=geom)
        transform = geoms_EPSG != self.EPSG
        if transform:
            geoms_crs = pyproj.CRS(f"EPSG:{geoms_EPSG}")
            ref_DB_crs = pyproj.CRS(f"EPSG:{self.EPSG}")
            project = pyproj.Transformer.from_crs(
                ref_DB_crs, geoms_crs, always_xy=True
            ).transform
            if AOI_geom is not None:
                project_AOI = pyproj.Transformer.from_crs(
                    geoms_crs, ref_DB_crs, always_xy=True
                ).transform
                AOI_geom = shapely.ops.transform(project_AOI, AOI_geom)
            for ref_geom in self.intersecting_idx_geoms(poly=AOI_geom):
                ref_geom = shapely.ops.transform(project, ref_geom)
                if not any(
                    same_geoms_func(el.object, ref_geom)
                    for el in index.intersection(ref_geom.bounds, objects=True)
                ):
                    yield ref_geom
        else:
            for ref_geom in self.intersecting_idx_geoms(poly=AOI_geom):
                if not any(
                    same_geoms_func(el.object, ref_geom)
                    for el in index.intersection(ref_geom.bounds, objects=True)
                ):
                    yield ref_geom
        logger.info("Done searching missing geometries.")


class SQLiteGeomRefDB(GeomRefDB):
    """Concrete implementation of the GeomRefDB ABC using SQLite.

    SQLiteGeomRefDB is a concrete implementation of the interface
    defined by the GeomRefDB abstract base class. It enables to load
    an existing (or create a new) SQLite database, where geometry
    datasets can be stored and can be compared (based on geometry
    similarity functions) with other geometrical features from an
    external dataset. This class makes use of the spatialite extension
    of SQLite, and as such, spatialite must be installed and available
    in order to work with instances of this class.
    """

    @property
    def supported_geom_types(self):
        return [
            "Point",
            "LineString",
            "Polygon",
            "MultiPoint",
            "MultiLineString",
            "MultiPolygon",
            "GeometryCollection",
        ]

    def __init__(
        self,
        db_path=None,
        default_epsg=None,
        geoms_iter=None,
        geoms_tab_name=None,
        geom_type=None,
        geoms_epsg=None,
        in_ram=True,
        logger=None,
        logger_name=None,
        logging_level=logging.INFO,
    ):
        if db_path is not None:
            self.db_path = os.path.abspath(db_path)
        else:
            self.db_path = None
        if default_epsg is not None:
            try:
                default_epsg = int(default_epsg)
                _ = pyproj.CRS(default_epsg)
            except (CRSError, ValueError, TypeError):
                raise ValueError("{!r} ('default_epsg') is not a valid EPSG code!")
            else:
                self.default_epsg = default_epsg
        else:
            self.default_epsg = None
        self.in_ram = in_ram
        if logger is not None:
            self.logger = logger
        else:
            if logger_name is None:
                logger_name = type(self).__name__
            self._logger = _setup_logger(name=logger_name, level=logging_level)

        if db_path is not None and not os.path.isfile(db_path):
            new_db = True
        else:
            new_db = False
        if db_path is None:
            if not in_ram:
                raise ValueError(
                    "The 'db_path' and 'in_ram' parameters cannot both be None!"
                )
            else:
                self.conn = sqlite3.connect(":memory:")
                self.conn.enable_load_extension(True)
                self.conn.load_extension("mod_spatialite")
                cursor = self.conn.cursor()
                cursor.execute("SELECT InitSpatialMetaData();")
                self.conn.commit()
                self.logger.info("New database created in RAM.")
        elif in_ram:
            self.conn = sqlite3.connect(":memory:")
            self.conn.enable_load_extension(True)
            self.conn.load_extension("mod_spatialite")
            if not new_db:
                disk_conn = sqlite3.connect(db_path)
                disk_conn.backup(self.conn)
                disk_conn.close()
                self.logger.info(
                    f"Database file {db_path!r} successfully loaded in RAM."
                )
            else:
                cursor = conn.cursor()
                cursor.execute("SELECT InitSpatialMetaData();")
                self.conn.commit()
                self.logger.info(
                    f"File {dp_path!r} does not exist, new database created in RAM."
                )
        else:
            self.conn = sqlite3.connect(db_path)
            self.conn.enable_load_extension(True)
            self.conn.load_extension("mod_spatialite")
            if new_db:
                cursor = conn.cursor()
                cursor.execute("SELECT InitSpatialMetaData();")
                self.conn.commit()
                self.logger.info(f"New database created at {db_path!r}.")
        if geoms_iter is not None:
            try:
                self.add_geometries(
                    geoms_iter,
                    geom_type=geom_type,
                    geoms_epsg=geoms_epsg,
                    geoms_tab_name=geoms_tab_name,
                )
            except Exception:
                if new_db and not in_ram:
                    self.logger.error(
                        "An error occurred while adding geometries to the database, "
                        f"deleting file {db_path!r}..."
                    )
                    os.remove(db_path)
                else:
                    self.logger.error(
                        "An error occurred while adding geometries to the database..."
                    )
                raise

    def __del__(self):
        self.conn.close()
        if hasattr(self, "db_tf") and os.path.isfile(self.db_tf):
            try:
                os.remove(self.db_tf)
            except PermissionError:
                pass

    def __getstate__(self):
        db_tf = NamedTemporaryFile(suffix=".db", delete=False)
        db_tf.close()
        _update_logger(self.logger, level=None)
        self.save_db(db_tf.name)
        _update_logger(self.logger, level=self.logger.getEffectiveLevel())
        attrs = self.__dict__.copy()
        attrs["db_tf"] = db_tf.name
        attrs["conn"] = None
        return attrs

    def __setstate__(self, state):
        self.__dict__ = state
        if self.in_ram:
            disk_conn = sqlite3.connect(self.db_tf)
            self.conn = sqlite3.connect(":memory:")
            disk_conn.backup(self.conn)
            disk_conn.close()
        else:
            self.conn = sqlite3.connect(self.db_tf)
        self.conn.enable_load_extension(True)
        self.conn.load_extension("mod_spatialite")

    def save_db(self, path, overwrite=True):
        if os.path.isfile(path):
            if overwrite:
                self.logger.info(f"File {path!r} already exists. Removing file...")
                os.remove(path)
            else:
                self.logger.info(
                    f"File {path!r} already exists. Saving database aborted."
                )
                return
        disk_conn = sqlite3.connect(path)
        self.conn.backup(disk_conn)
        disk_conn.close()
        self.logger.info("Database was saved successfully.")

    def add_geometries(
        self, geoms_iter, geom_type=None, geoms_epsg=None, geoms_tab_name=None
    ):
        self.logger.info("Adding geometries to the database...")
        db_info = self.db_geom_info()
        ## Coordinates of input geometries are not transformed by
        ## default. Return the input geometry unchanged.
        transform_geom = unchanged_geom
        cursor = self.conn.cursor()
        if geom_type is not None and not geom_type in self.supported_geom_types:
            raise ValueError(
                f"{geom_type!r} is not a valid value for the 'geom_type' argument! "
                "Supported geometry types are: {}.".format(
                    ", ".join([f"{gt!r}" for gt in self.supported_geom_types])
                )
            )
        if geoms_epsg is not None:
            try:
                geoms_epsg = int(geoms_epsg)
                _ = pyproj.CRS(geoms_epsg)
            except (CRSError, ValueError, TypeError):
                raise ValueError(f"{geoms_epsg!r} is not a valid EPSG code!")
        if geoms_tab_name is None:
            geoms_tab_name = "default_table"
        tab_info = db_info.get(geoms_tab_name, None)
        if tab_info is None:
            new_tab = True
        else:
            new_tab = False
        if new_tab:
            if geom_type is None:
                raise ValueError(
                    "'geom_type' cannot be passed None for new databases, or if no "
                    "existing geometry table name ('geoms_tab_name') was passed as "
                    "parameter!"
                )
            if geoms_epsg is None:
                if self.default_epsg is not None:
                    geoms_epsg = self.default_epsg
                else:
                    raise ValueError(
                        "'geoms_epsg' cannot be passed None if no default EPSG has "
                        "been set!"
                    )
            cursor.execute(
                f"CREATE TABLE {geoms_tab_name} "
                "(r_id INTEGER PRIMARY KEY AUTOINCREMENT);"
            )
            cursor.execute(
                f"SELECT AddGeometryColumn ('{geoms_tab_name}', "
                f"'geometry', {geoms_epsg}, '{geom_type}', 'XY', 1);"
            )
            cursor.execute(
                f"SELECT CreateSpatialIndex('{geoms_tab_name}', 'geometry');"
            )
            self.conn.commit()

        else:  # if existing table
            if geom_type is not None and geom_type != tab_info["geom_type"]:
                raise ValueError(
                    f"The {geom_type!r} geometry type does not match the geometry type "
                    f"of the {geoms_tab_name!r} table!"
                )
            if geoms_epsg is None:
                geoms_epsg = tab_info["srid"]
            elif geoms_epsg != tab_info["srid"]:
                transform_geom = get_transform_func(geoms_epsg, tab_info["srid"])
                geoms_epsg = tab_info["srid"]
        for geom in geoms_iter:
            cursor.execute(
                f"INSERT INTO {geoms_tab_name} (geometry) VALUES "
                f"(GeomFromText('{transform_geom(to_2D(geom)).wkt}', "
                f"{geoms_epsg}));"
            )
        self.conn.commit()

    def db_geom_info(self, to_stdout=False):

        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM geometry_columns")
        info = {
            tab[0]: {"geom_type": geom_type_mapping[tab[2]], "srid": tab[4]}
            for tab in cursor.fetchall()
        }
        if not to_stdout:
            return info
        elif not info:
            return None
        else:
            f1 = "TABLE"
            f1_width = max(max(len(tab_name) for tab_name in info.keys()), len(f1))
            f2 = "GEOMETRY"
            f2_width = max(max(len(info[k]["geom_type"]) for k in info.keys()), len(f2))
            f3 = "EPSG"
            f3_width = max(max(len(str(info[k]["srid"])) for k in info.keys()), len(f3))
            line_tmp = f"{{:<{f1_width}}} | {{:<{f2_width}}} | {{:>{f3_width}}}\n"
            head = line_tmp.format(f1, f2, f3)
            line_width = len(head) - 1
            table = f"\n{head}{'-' * line_width}\n"
            for k, v in info.items():
                table += line_tmp.format(k, v["geom_type"], str(v["srid"]))
            print(table)

    @staticmethod
    def _get_spatial_query(spatial_index=True, only_within_aoi=False, within_aoi=False):
        query = "SELECT AsBinary(geometry) FROM {{table}}"
        if not spatial_index:
            return query.format() + ";"
        else:
            query += " {spatial_filter};"
        sf = " WHERE {{table}}.ROWID IN {aoi}{geom}"
        if only_within_aoi or within_aoi:
            aoi = (
                "(SELECT ROWID "
                " FROM SpatialIndex "
                " WHERE f_table_name = '{table}' "
                "   AND search_frame = GeomFromText('{aoiwkt}', {epsg})) "
                "AND Intersects(geometry, GeomFromText('{aoiwkt}', {epsg}))"
            )
        else:
            aoi = ""
        if only_within_aoi:
            geom = ""
            return query.format(spatial_filter=sf.format(**locals()))
        else:
            geom = (
                "(SELECT ROWID "
                " FROM SpatialIndex "
                " WHERE f_table_name = '{table}' "
                "   AND search_frame = GeomFromText('{geomsfwkt}', {epsg}))"
            )
        if within_aoi:
            aoi += " AND {table}.ROWID IN "
        return query.format(spatial_filter=sf.format(**locals()))

    def _geoms_generator(
        self,
        geoms_iter,
        transform_geom,
        geoms_match,
        get_search_frame,
        query,
        query_kwargs,
        matching_geoms=True,
    ):
        cursor = self.conn.cursor()
        if matching_geoms:
            for geom in geoms_iter:
                geom_reproj = transform_geom(to_2D(geom))
                search_frame = get_search_frame(geom_reproj)
                query_kwargs["geomsfwkt"] = search_frame.wkt
                cursor.execute(query.format(**query_kwargs))
                if any(geoms_match(geom_reproj, wkb.loads(row[0])) for row in cursor):
                    yield geom
        else:
            for geom in geoms_iter:
                geom_reproj = transform_geom(to_2D(geom))
                search_frame = get_search_frame(geom_reproj)
                query_kwargs["geomsfwkt"] = search_frame.wkt
                cursor.execute(query.format(**query_kwargs))
                if not any(
                    geoms_match(geom_reproj, wkb.loads(row[0])) for row in cursor
                ):
                    yield geom

    def true_positives(
        self,
        geoms_iter,
        aoi_geom=None,
        geoms_epsg=None,
        geoms_tab_name=None,
        geoms_match=None,
        get_search_frame=None,
        ncores=None,
    ):  # , **kwargs):
        self.logger.info("Searching true positive geometries...")
        query_kwargs = dict()
        ## Set default behaviors.
        ## If no function is passed to the 'geoms_match' parameter,
        ## the "test geometry" will always match the "reference
        ## geometry". This way, the user can still use the spatial
        ## index of the internal database and make simple query against
        ## bounding boxes of the stored geometries, without further
        ## assessment of whether geometries match each others.
        if geoms_match is None:
            geoms_match = geoms_always_match
        ## Default search frame is the input geometry itself.
        if get_search_frame is None:
            get_search_frame = unchanged_geom
        ## Coordinates of input geometries are not transformed by
        ## default. Return the input geometry unchanged.
        transform_geom = unchanged_geom
        if geoms_tab_name is None:
            geoms_tab_name = "default_table"
        db_info = self.db_geom_info()
        tab_info = db_info.get(geoms_tab_name, None)
        if tab_info is None:
            raise RuntimeError("No {!r} table was found in the database!")
        else:
            query_kwargs["table"] = geoms_tab_name
        if geoms_epsg is None:
            geoms_epsg = tab_info["srid"]
        else:
            try:
                geoms_epsg = int(geoms_epsg)
                _ = pyproj.CRS(geoms_epsg)
            except (CRSError, ValueError, TypeError):
                raise ValueError(f"{geoms_epsg!r} is not a valid EPSG code!")
            if geoms_epsg != tab_info["srid"]:
                transform_geom = get_transform_func(geoms_epsg, tab_info["srid"])
                if aoi_geom is not None:
                    aoi_geom = transform_geom(aoi_geom)
        query_kwargs["epsg"] = geoms_epsg
        if aoi_geom is not None:
            query_kwargs["aoiwkt"] = aoi_geom.wkt
        if ncores is not None:
            try:
                ncores = int(ncores)
                max_cores = mp.cpu_count() - 1
                if ncores > max_cores:
                    self.logger.info(
                        f"Value {ncores} passed to the 'ncores' argument is too high "
                        "and may result in performance penalty, setting it down to "
                        f"{max_cores}."
                    )
                    ncores = max_cores
                elif ncores < 2:
                    ncores = None
            except (ValueError, TypeError):
                raise ValueError(
                    f"{ncores!r} is not a valid value for the 'ncores' argument!"
                )
        query = self._get_spatial_query(within_aoi=aoi_geom is not None)
        if ncores is not None:
            tp_gen = self._parallelized_method(
                ncores,
                geoms_iter,
                transform_geom,
                geoms_match,
                get_search_frame,
                query,
                query_kwargs,
                matching_geoms=True,
                method_name="_geoms_generator",
            )
        else:
            tp_gen = self._geoms_generator(
                geoms_iter,
                transform_geom,
                geoms_match,
                get_search_frame,
                query,
                query_kwargs,
                matching_geoms=True,
            )
        yield from tp_gen
        self.logger.info("Done searching true positive geometries.")

    def false_positives(
        self,
        geoms_iter,
        aoi_geom=None,
        geoms_epsg=None,
        geoms_tab_name=None,
        geoms_match=None,
        get_search_frame=None,
        ncores=None,
    ):  # , **kwargs):
        self.logger.info("Searching false positive geometries...")
        query_kwargs = dict()
        ## Set default behaviors.
        ## If no function is passed to the 'geoms_match' parameter,
        ## the "test geometry" will always match the "reference
        ## geometry". This way, the user can still use the spatial
        ## index of the internal database and make simple query against
        ## bounding boxes of the stored geometries, without further
        ## assessment of whether geometries match.
        if geoms_match is None:
            geoms_match = geoms_always_match
        ## Default search frame is the input geometry itself.
        if get_search_frame is None:
            get_search_frame = unchanged_geom
        ## Coordinates of input geometries are not transformed by
        ## default. Return the input geometry unchanged.
        transform_geom = unchanged_geom
        if geoms_tab_name is None:
            geoms_tab_name = "default_table"
        db_info = self.db_geom_info()
        tab_info = db_info.get(geoms_tab_name, None)
        if tab_info is None:
            raise RuntimeError("No {!r} table was found in the database!")
        else:
            query_kwargs["table"] = geoms_tab_name
        if geoms_epsg is None:
            geoms_epsg = tab_info["srid"]
        else:
            try:
                geoms_epsg = int(geoms_epsg)
                _ = pyproj.CRS(geoms_epsg)
            except (CRSError, ValueError):
                raise ValueError(f"{geoms_epsg!r} is not a valid EPSG code!")
            if geoms_epsg != tab_info["srid"]:
                transform_geom = get_transform_func(geoms_epsg, tab_info["srid"])
                if aoi_geom is not None:
                    aoi_geom = transform_geom(aoi_geom)
        query_kwargs["epsg"] = geoms_epsg
        if aoi_geom is not None:
            query_kwargs["aoiwkt"] = aoi_geom.wkt
        if ncores is not None:
            try:
                ncores = int(ncores)
                max_cores = mp.cpu_count() - 1
                if ncores > max_cores:
                    self.logger.info(
                        f"Value {ncores} passed to the 'ncores' argument is too high "
                        "and may result in performance penalty, setting it down to "
                        f"{max_cores}."
                    )
                    ncores = max_cores
                elif ncores < 2:
                    ncores = None
            except (ValueError, TypeError):
                raise ValueError(
                    f"{ncores!r} is not a valid value for the 'ncores' argument!"
                )
        query = self._get_spatial_query(within_aoi=aoi_geom is not None)
        if ncores is not None:
            fp_gen = self._parallelized_method(
                ncores,
                geoms_iter,
                transform_geom,
                geoms_match,
                get_search_frame,
                query,
                query_kwargs,
                matching_geoms=False,
                method_name="_geoms_generator",
            )
        else:
            fp_gen = self._geoms_generator(
                geoms_iter,
                transform_geom,
                geoms_match,
                get_search_frame,
                query,
                query_kwargs,
                matching_geoms=False,
            )
        yield from fp_gen
        self.logger.info("Done searching false positive geometries.")

    def missing_geometries(
        self,
        geoms_iter,
        geom_type=None,
        aoi_geom=None,
        geoms_epsg=None,
        geoms_tab_name=None,
        geoms_match=None,
        get_search_frame=None,
        ncores=None,
    ):  # , **kwargs):
        self.logger.info("Searching missing geometries...")
        query_kwargs = dict()
        if geoms_tab_name is None:
            geoms_tab_name = "default_table"
        db_info = self.db_geom_info()
        tab_info = db_info.get(geoms_tab_name, None)
        if tab_info is None:
            raise RuntimeError("No {!r} table was found in the database!")
        if geom_type is None:
            self.logger.info(
                "No geometry type was passed to the 'geom_type' argument. Assuming "
                "that all input geometries have the same geometry type as the ones "
                f"stored in the {geoms_tab_name!r} table."
            )
            geom_type = tab_info["geom_type"]
        query_kwargs["table"] = geoms_tab_name
        db_epsg = tab_info["srid"]
        query_kwargs["epsg"] = db_epsg
        if geoms_epsg is None:
            geoms_epsg = db_epsg
        else:
            try:
                geoms_epsg = int(geoms_epsg)
                _ = pyproj.CRS(geoms_epsg)
            except (CRSError, ValueError, TypeError):
                raise ValueError(f"{geoms_epsg!r} is not a valid EPSG code!")
            if geoms_epsg != db_epsg and aoi_geom is not None:
                transform_aoi = get_transform_func(geoms_epsg, db_epsg)
                aoi_geom = transform_aoi(aoi_geom)
        if aoi_geom is not None:
            query_kwargs["aoiwkt"] = aoi_geom.wkt
        logger_conf = dict()
        if ncores is not None:
            try:
                ncores = int(ncores)
                max_cores = mp.cpu_count() - 1
                if ncores > max_cores:
                    self.logger.info(
                        f"Value {ncores} passed to the 'ncores' argument is too high "
                        "and may result in performance penalty, setting it down to "
                        f"{max_cores}."
                    )
                    ncores = max_cores
                elif ncores < 2:
                    ncores = None
            except (ValueError, TypeError):
                raise ValueError(
                    f"{ncores!r} is not a valid value for the 'ncores' argument!"
                )
        if ncores is None:
            logger_conf.update(
                {"logger_name": str(uuid.uuid1()), "logging_level": None}
            )
        else:
            logger_conf["logger"] = self.logger

        input_geoms_db = SQLiteGeomRefDB(
            geoms_iter=geoms_iter,
            geom_type=geom_type,
            geoms_epsg=geoms_epsg,
            **logger_conf,
        )
        query = self._get_spatial_query(only_within_aoi=aoi_geom is not None)
        cursor = self.conn.cursor()
        cursor.execute(query.format(**query_kwargs))
        geoms_iter = (wkb.loads(row[0]) for row in cursor)
        if ncores is not None:
            mg_gen = input_geoms_db._parallelized_method(
                ncores,
                geoms_iter,
                method_name="false_positives",
                geoms_epsg=db_epsg,
                geoms_match=geoms_match,
                get_search_frame=get_search_frame,
            )
        else:

            mg_gen = input_geoms_db.false_positives(
                geoms_iter,
                geoms_epsg=db_epsg,
                geoms_match=geoms_match,
                get_search_frame=get_search_frame,
            )
        yield from mg_gen
        del input_geoms_db
        self.logger.info("Done searching missing geometries.")

    def _parallelized_method(
        self, ncores, geoms_iter, *method_args, method_name="", **method_kwargs
    ):
        if not method_name:
            method_name = inspect.stack()[1][3]
        method_obj = getattr(self, method_name, None)
        if method_obj is None:
            raise ValueError(
                f"{method_name!r} is not a valid method name for "
                f"the class {cls.__name__!r}!"
            )
        logger_conf = {
            "name": self.logger.name,
            "level": self.logger.getEffectiveLevel(),
        }
        method_kwargs["logger_conf"] = logger_conf
        geoms_iters = split_iter_to_lists(geoms_iter, ncores)
        shared_results_iter = SharedIterator()
        nprocs_done = mp.Value("i", 0)
        procs = list()
        for i in range(ncores):
            procs.append(
                mp.Process(
                    target=self._wrap_method_return,
                    args=[
                        method_obj,
                        geoms_iters[i],
                        shared_results_iter,
                        nprocs_done,
                        *method_args,
                    ],
                    kwargs=method_kwargs,
                )
            )
        try:
            for p in procs:
                p.start()
                self.logger.info(f"New process spawned (PID: {p.pid}).")
            ## Must check sentinel value, join cannot be called as the
            ## spawned processes use a queue object (see:
            ## https://docs.python.org/3/library/multiprocessing.html#programming-guidelines).
            while not nprocs_done.value == ncores:
                time.sleep(0.2)
        except KeyboardInterrupt:
            for p in procs:
                p.terminate()
                p.join()
            del geoms_iters
            raise
        else:
            return shared_results_iter

    def _wrap_method_return(
        self,
        method_obj,
        geoms_iter,
        shared_results_iter,
        nprocs_done,
        *method_args,
        logger_conf=None,
        **method_kwargs,
    ):
        if logger_conf is None:
            logger = _setup_logger(name=str(uuid.uuid1()), level=None)
        else:
            logger_conf["name"] = (
                logger_conf.get("name", type(self).__name__) + f" (PID: {os.getpid()})"
            )
            logger = _setup_logger(**logger_conf)
        logger.info("Start worker function.")
        res = list(method_obj(geoms_iter, *method_args, **method_kwargs))
        n_geoms = len(res)
        if n_geoms > 1:
            logger.info(f"{n_geoms} geometries found.")
        else:
            logger.info(f"{n_geoms} geometry found.")
        shared_results_iter.put_iter(res)
        with nprocs_done.get_lock():
            nprocs_done.value += 1
        logger.info("Worker function completed.")
