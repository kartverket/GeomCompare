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
from pyproj.exceptions import CRSError
from shapely import speedups, wkb

from .comparefunc import geoms_always_match
from ._geomrefdb_abc import GeomRefDB
from .geomutils import _geom_type_mapping, get_transform_func, _to_2D, _unchanged_geom, GeomObject
from .io import _setup_logger, _update_logger, GeometryIterable
from ._misc import SharedIterator, split_iter_to_lists


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

#: Geometry types supported by the `SQLiteGeomRefDB` class.
SpatialiteGeomType = Literal[
    "Point",
    "LineString",
    "Polygon",
    "MultiPoint",
    "MultiLineString",
    "MultiPolygon",
    "GeometryCollection",
]

class SQLiteGeomRefDB(GeomRefDB):
    """Concrete implementation of the GeomRefDB ABC using SQLite.

    SQLiteGeomRefDB is a concrete implementation of the interface
    defined by the GeomRefDB abstract base class. It enables to load
    an existing (or create a new) SQLite database, where geometry
    datasets can be stored and can be compared (based on geometry
    similarity functions) with other geometrical features from an
    external dataset. Instances of this class can handle
    simultaneously multiple reference datasets, with various geometry
    types (see :attr:`supported_geom_types`) and spatial reference
    systems.

    Parameters
    ----------
    filename : `str`, optional
        Path to an existing spatialite database.
    default_epsg : `int`, optional
        Default EPSG code of the geometrical features that will be
        added to the database. If specified, the EPSG code will be
        default value of the ``geoms_epsg`` parameter for any
        subsequent call of the :meth:`add_geometries` method.
    geoms_iter : iterable of `.GeomObject`, optional
        Iterable of the geometrical features to add to this
        `SQLiteGeomRefDB` instance. Such features can also be added
        later to the class instance with the :meth:`add_geometries`
        method.
    geoms_tab_name : `str`, optional
        Name of the table where the geometrical features are to be
        stored. If the ``geoms_iter`` parameter is not given,
        ``geoms_tab_name`` will be ignored.
    geom_type : `SpatialiteGeomType`, optional
        Geometry type of the geometrical features passed as argument
        to the ``geoms_iter`` parameter.
    geoms_epsg : `int`, optional
        EPSG code of the geometrical features passed as argument to
        the ``geoms_iter`` parameter. If specified, it overrides the
        ``default_epsg`` parameter during the instance construction.
    in_ram : bool, default: ``True``
        Set to ``True`` to create/load the database in RAM for faster
        access. Set to ``False`` for larger-than-RAM databases.
    logger : `logging.Logger`, optional
        Logger instance to use for logging outputs.
    logger_name : `str`, optional
        Name of the `logging.Logger` object to create for logging
        outputs. This parameter will be ignored if a Logger instance is
        passed to the ``logger`` parameter.
    logging_level : `int`, default: ``logging.INFO``
        Logging level of the logging output. For mor information, please
        see the documentation of the `logging` module.

    Raises
    ------
    ValueError
        If ``in_ram=False`` and ``filename=None``.

    Notes
    -----
    This class makes use of the spatialite extension of SQLite, and as
    such, spatialite must be installed and available in order to work with
    instances of this class.
    """

    def __init__(
        self,
        filename: Optional[str] = None,
        default_epsg: Optional[int] = None,
        geoms_iter: Optional[GeometryIterable] = None,
        geoms_tab_name: Optional[str] = None,
        geom_type: Optional[SpatialiteGeomType] = None,
        geoms_epsg: Optional[int] = None,
        in_ram: bool = True,
        logger: Optional[logging.Logger] = None,
        logger_name: Optional[str] = None,
        logging_level: int = logging.INFO,
    ) -> None:
        if filename is not None:
            self._filename = os.path.abspath(filename)
        else:
            self._filename = None
        if default_epsg is not None:
            try:
                default_epsg = int(default_epsg)
                _ = pyproj.CRS(default_epsg)
            except (CRSError, ValueError, TypeError):
                raise ValueError("{!r} ('default_epsg') is not a valid EPSG code!")
            else:
                self._default_epsg = default_epsg
        else:
            self._default_epsg = None
        self._in_ram = in_ram
        if logger is not None:
            self._logger = logger
        else:
            if logger_name is None:
                logger_name = type(self).__name__
            self._logger = _setup_logger(name=logger_name, level=logging_level)

        if filename is not None and not os.path.isfile(filename):
            new_db = True
        else:
            new_db = False
        if filename is None:
            if not in_ram:
                raise ValueError(
                    "The 'filename' cannot be set to None if 'in_ram' is set to False!"
                )
            else:
                self._conn = sqlite3.connect(":memory:")
                self._conn.enable_load_extension(True)
                self._conn.load_extension("mod_spatialite")
                cursor = self._conn.cursor()
                cursor.execute("SELECT InitSpatialMetaData();")
                self._conn.commit()
                self._logger.info("New database created in RAM.")
        elif in_ram:
            self._conn = sqlite3.connect(":memory:")
            self._conn.enable_load_extension(True)
            self._conn.load_extension("mod_spatialite")
            if not new_db:
                disk_conn = sqlite3.connect(filename)
                disk_conn.backup(self._conn)
                disk_conn.close()
                self._logger.info(
                    f"Database file {filename!r} successfully loaded in RAM."
                )
            else:
                cursor = conn.cursor()
                cursor.execute("SELECT InitSpatialMetaData();")
                self._conn.commit()
                self._logger.info(
                    f"File {dp_path!r} does not exist, new database created in RAM."
                )
        else:
            self._conn = sqlite3.connect(filename)
            self._conn.enable_load_extension(True)
            self._conn.load_extension("mod_spatialite")
            if new_db:
                cursor = conn.cursor()
                cursor.execute("SELECT InitSpatialMetaData();")
                self._conn.commit()
                self._logger.info(f"New database created at {filename!r}.")
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
                    self._logger.error(
                        "An error occurred while adding geometries to the database, "
                        f"deleting file {filename!r}..."
                    )
                    os.remove(filename)
                else:
                    self._logger.error(
                        "An error occurred while adding geometries to the database..."
                    )
                raise

    @classmethod
    @property
    def supported_geom_types(cls) -> list[SpatialiteGeomType]:
        """`list` of supported geometry types: Types supported by
        `SQLiteGeomRefDB`.
        """
        return [
            "Point",
            "LineString",
            "Polygon",
            "MultiPoint",
            "MultiLineString",
            "MultiPolygon",
            "GeometryCollection",
        ]

    @property
    def filename(self) -> Optional[str]:
        """Path of the opened database file. The attribute is set to
        `None` if a new database was created in RAM for this instance.
        """
        return self._filename

    @property
    def in_ram(self) -> bool:
        """``True`` if the database is created/loaded in RAM. ``False``
        if the instance is connected to database file on disk.
        """
        return self._in_ram

    @property
    def default_epsg(self) -> int:
        """Default EPSG code of the geometrical features that are
        added to the database.
        """
        return self._default_epsg

    @default_epsg.setter
    def default_epsg(self, value: int) -> None:
        self.logger.info(f"The 'default_epsg' is now set to {default_epsg}.")
        self._default_epsg = value

    @property
    def logger(self) -> logging.Logger:
        """Ready configured Logger instance used for logging outputs.
        """
        return self._logger

    def __del__(self):
        """Close the connection to the SQLite database and do some
        cleanup if the instance has been pickled.
        """
        self._conn.close()
        if hasattr(self, "db_tf") and os.path.isfile(self.db_tf):
            try:
                os.remove(self.db_tf)
            except PermissionError:
                pass

    def __getstate__(self):
        """Get state when pickling the instance.
        """
        db_tf = NamedTemporaryFile(suffix=".db", delete=False)
        db_tf.close()
        _update_logger(self.logger, level=None)
        self.save_db(db_tf.name)
        _update_logger(self.logger, level=self.logger.getEffectiveLevel())
        attrs = self.__dict__.copy()
        attrs["db_tf"] = db_tf.name
        attrs["_conn"] = None
        return attrs

    def __setstate__(self, state):
        """Set state when unpickling an instance.
        """
        self.__dict__ = state
        if self.in_ram:
            disk_conn = sqlite3.connect(self.db_tf)
            self._conn = sqlite3.connect(":memory:")
            disk_conn.backup(self._conn)
            disk_conn.close()
        else:
            self._conn = sqlite3.connect(self.db_tf)
        self._conn.enable_load_extension(True)
        self._conn.load_extension("mod_spatialite")

    def save_db(self, filename: str, overwrite: bool = True) -> None:
        """Save the internal SQLite database to disk.

        The function saves the internal SQLite database, together with
        all the geometrical features added with
        :meth:`add_geometries`, to disk. The path of the resulting
        output file can later be passed to the ``filename`` argument
        of the `SQLiteGeomRefDB` class' constructor to load the saved
        database with all its features. This function is useful only
        to save loaded-in-RAM databases, as the geometrical features
        added to a :class:`SQLiteGeomRefDB` instance, with an opened
        connections to databases that reside on disk, will be saved
        automatically even after the instance destruction.

        Parameters
        ----------
        filename : `str`
            Path of the output database file.
        overwrite : `bool`, default: ``True``
            ``True`` if the output file should overwrite any existing file
            at path ``filename``, else ``False``.

        Returns
        -------
        `None`
        """
        if os.path.isfile(filename):
            if overwrite:
                self.logger.info(f"File {filename!r} already exists. Removing file...")
                os.remove(filename)
            else:
                self.logger.info(
                    f"File {filename!r} already exists. Saving database aborted."
                )
                return
        disk_conn = sqlite3.connect(filename)
        self._conn.backup(disk_conn)
        disk_conn.close()
        self.logger.info("Database was saved successfully.")

    def add_geometries(
        self,
        geoms_iter: GeometryIterable,
        geom_type: Optional[SpatialiteGeomType] = None,
        geoms_epsg: Optional[int] = None,
        geoms_tab_name: Optional[str] = None,
    ):
        """Add geometrical features to the internal SQLite database.

        The function adds geometrical features to the internal SQLite
        database, which can then be used as a "reference dataset" when
        running other public methods of the `SQLiteGeomRefDB`
        instance.

        Parameters
        ----------
        geoms_iter : iterable of `.GeomObject`
            Iterable of the geometrical features to add to
            this `SQLiteGeomRefDB` instance.
        geom_type : `SpatialiteGeomType`, optional
            Geometry type of the input geometrical features. If the
            ``geom_type`` is not specified by the user, the function
            will assume that the input features have the same geometry
            type as the features already stored in the destination
            table.
        geoms_epsg : `int`, optional
            EPSG code of the input geometrical features. If the
            ``geoms_epsg`` is not specified by the user, the function
            will assume that the input features are in the same
            spatial reference system as the features already stored in
            the destination table. Also, if the input features are to
            be stored in a new table of the database and the
            ``geoms_epsg`` is omitted, the `SSQLiteGeomRefDB` instance
            will use the EPSG code stored in the :attr:`default_epsg`
            attribute (if set).
        geoms_tab_name : `str`, optional
            Name of the table where the input geometrical features are
            to be stored in the internal SQLite database. If no
            argument is passed to the ``geoms_tab_name`` parameter,
            the function will try to store the input geometrical
            features into a table named *default_table*. The
            *default_table* table will be created if it does not
            already exist in the database.

        Raises
        ------
        ValueError
            If ``geom_type`` is not specified, in the case of a new
            database/table.
        ValueError
            If ``geoms_epsg`` is not specified, in the case of a new
            database/table and if the :attr:`default_epsg` attribute
            is not set.
        ValueError
            If the argument passed to the ``geom_type`` parameter does not
            match the geometry type of the features already stored in the
            destination table.

        Warnings
        --------
        The *geometry type* must be the same for all input features as they
        are to be stored in the same *geometry column* of the same table,
        and spatialite does not allow *geometry columns* to have mixed
        *geometry types*.

        Returns
        -------
        None
        """
        self.logger.info("Adding geometries to the database...")
        db_info = self.db_geom_info()
        ## Coordinates of input geometries are not transformed by
        ## default. Return the input geometry unchanged.
        transform_geom = _unchanged_geom
        cursor = self._conn.cursor()
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
                    "'geom_type' cannot be passed None for new databases/tables, "
                    "or if no existing geometry table name ('geoms_tab_name') "
                    "was passed as parameter!"
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
            self._conn.commit()

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
                f"(GeomFromText('{transform_geom(_to_2D(geom)).wkt}', "
                f"{geoms_epsg}));"
            )
        self._conn.commit()

    def db_geom_info(
        self, to_stdout: bool = False, count_features: bool = False
    ) -> Optional[dict]:
        """Get information on features stored in the internal SQLite database.

        Get information on the geometrical features such as the name
        of the table(s) where they are stored, their geometry type(s),
        spatial reference system(s) and the number of features per
        table. This information can be returned as `dict` instance, or
        printed to *stdout*.

        Parameters
        ----------
        to_stdout : bool, default: ``False``
            If set to ``False``, the information is returned as a
            `dict`. If set to ``True``, the information is written to
            *stdout*.
        count_features : bool, default: ``False``
            If set to ``True``, the function will also return the number of
            features/rows per table. If set to ``False``, the features will
            not be counted.

        Returns
        -------
        `dict` or `None`
            If ``to_stdout=False``, returns a `dict` which keys are the
            table name(s), and which values are information on the
            individual table(s). This information is itself structured as a
            `dict`, which key/value pairs indicate for each table the
            geometry type (key: *geom_type*), the spatial reference system
            (key: *srid*), and optionally (if ``count_features=True``) the
            features count (key: *count*). The function returns `None` if
            ``to_stdout=True``.
        """
        cursor = self._conn.cursor()
        cursor.execute("SELECT * FROM geometry_columns")
        info = {
            tab[0]: {"geom_type": _geom_type_mapping[tab[2]], "srid": tab[4]}
            for tab in cursor.fetchall()
        }
        if count_features:
            for tab in info.keys():
                cursor.execute(f"SELECT COUNT(*) FROM {tab};")
                info[tab]["count"] = cursor.fetchone()[0]
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
            line_tmp = f"{{f1:<{f1_width}}} | {{f2:<{f2_width}}} | {{f3:>{f3_width}}}"
            if count_features:
                f4 = "COUNT"
                f4_width = max(max(len(str(info[k]["count"])) for k in info.keys()), len(f4))
                line_tmp += f" | {{f4:>{f4_width}}}"
            line_tmp += "\n"
            head = line_tmp.format(**locals())
            line_width = len(head) - 1
            table = f"\n{head}{'-' * line_width}\n"
            if count_features:
                for k, v in info.items():
                    table += line_tmp.format(
                        f1=k, f2=v["geom_type"], f3=str(v["srid"]), f4=str(v["count"])
                    )
            else:
                for k, v in info.items():
                    table += line_tmp.format(f1=k, f2=v["geom_type"], f3=str(v["srid"]))
            print(table)

    @staticmethod
    def _get_spatial_query(spatial_index: bool = True, only_within_aoi: bool = False,
                           within_aoi: bool = False) -> str:
        """Builds SQL spatial query templates for accessing the database's features.

        This function builds SQL spatial query template for fast access to
        the features stored in the database. It can create templates where
        the queries use the internal spatial index to search for features
        within a *search frame*, and/or where the features returned must
        lie within an *area of interest*.

        Parameters
        ----------
        spatial_index : bool, default: ``True``
            If set to ``True``, create a template for using the spatial
            index and search for features that lie within a *search frame*
            and/or an *area of interest*. If set to ``False``, create a
            template for returning all features of table, without filtering
            (overrinding both ``only_within_aoi`` and ``within_aoi``).
        only_within_aoi : bool, default: ``False``
            If set to ``True``, create a template that will use the spatial
            index for searching for features that only intersect with an
            *area of interest*, without further filtering. If set to
            ``False``, create a template that may use the spatial index for
            searching for features lying within an *area of interest*
            and/or within a *search frame*
        within_aoi : bool, default: ``False``
            If set to ``True``, create a template that will use the spatial
            index for searching for features that intersect with an *area
            of interest*. If set to ``False``, the created template may not
            filter out features based on a *area of interest* (depending on
            the value passed to the ``only_within_aoi`` parameter).

        Returns
        -------
        ``str``
            Template of the SQL query.
        """
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
    ) -> Generator[GeomObject]:
        """Yield (non-)matching features resulting from the SQL query.

        Function generator that formats a SQL query template (from
        :meth:`_get_spatial_query`), and executes the resulting query
        for each input feature. The feature(s) resulting from the
        queries is/are yielded by the function, depending on whether
        its/their geometries match the geometry of the input features
        or not.
        """
        cursor = self._conn.cursor()
        if matching_geoms:
            for geom in geoms_iter:
                geom_reproj = transform_geom(_to_2D(geom))
                search_frame = get_search_frame(geom_reproj)
                query_kwargs["geomsfwkt"] = search_frame.wkt
                cursor.execute(query.format(**query_kwargs))
                if any(geoms_match(geom_reproj, wkb.loads(row[0])) for row in cursor):
                    yield geom
        else:
            for geom in geoms_iter:
                geom_reproj = transform_geom(_to_2D(geom))
                search_frame = get_search_frame(geom_reproj)
                query_kwargs["geomsfwkt"] = search_frame.wkt
                cursor.execute(query.format(**query_kwargs))
                if not any(
                    geoms_match(geom_reproj, wkb.loads(row[0])) for row in cursor
                ):
                    yield geom

    def true_positives(
        self,
        geoms_iter: GeometryIterable,
        aoi_geom: Optional[GeomObject] = None,
        geoms_epsg: Optional[int] = None,
        geoms_tab_name: Optional[str] = None,
        geoms_match: Callable[[GeomObject, GeomObject], bool] = None,
        get_search_frame: Callable[[GeomObject], GeomObject] = None,
        ncores: Optional[int] = None,
    ) -> Generator[GeomObject]:  # , **kwargs):
        """Identidy *matching* **input** geometries.

        The function takes as input geometrical features, and searches
        for *reference features* in one table of the internal database
        which geometries are considered to *match* that of the *input
        features*. All *input features* that have a geometry that
        *matches* the geometry of at least one of the *reference
        features* will be yielded back by the function.

        Parameters
        ----------
        geoms_iter : iterable of `.GeomObject`
            Iterable of input geometrical features to compare to the
            features of the internal SQLite database.
        aoi_geom : `.GeomObject`, optional
            *Area of interest*, within which the database's features must
            lie.
        geoms_epsg : `int`, optional
            EPSG code of the input geometrical features (including
            ``aoi_geom`` if specified). If the ``geoms_epsg`` is not
            specified by the user, the function will assume that the
            *input features* are in the same spatial reference system
            as the *reference features*.
        geoms_tab_name : `str`, optional
            Name of the table where database's features that will be used
            as reference are stored. If no argument is passed to the
            ``geoms_tab_name`` parameter, the function will search for
            *reference features* in a table named *default_table*.
        geoms_match : `callable`, optional
            Comparison function that takes two positional arguments:

            - ``gtest``: *input* geometry (`.GeomObject`)
            - ``gref``: *reference* geometry (`.GeomObject`)

            The function returns ``True`` if it finds that both
            geometries *match*, else returns ``False``. If this
            parameter is omitted, the *input* geometrical feature will
            always be considered as a *match* in the case where its
            *search frame* (see ``get_search_frame`` parameter)
            interesects with one of the feature from the database's
            table.
        get_search_frame : `callable`, optional
            Function that takes as single argument an *input* geometry
            (`.GeomObject`) and returns its *search frame*
            (`.GeomObject`). If this parameter is omitted, the *search
            frame* will be the same as the *input* geometry.
        ncores : `int`, optional
            Number of cores to use for running the function. If
            unspecified, the function will run in a single process

        Yields
        ------
        `.GeomObject`
            *Matching input* geometrical features.

        Notes
        -----
        If the *spatial reference system* of the *input* geometrical
        features is different from that of the database's features,
        the *input features*' coordinates are reprojected on-the-fly,
        before being compared to features stored in the database. If
        an *input feature* is considered to be a *match*, it is
        yielded back unchanged (its coordinates in the original
        *spatial reference system*).
        """
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
            get_search_frame = _unchanged_geom
        ## Coordinates of input geometries are not transformed by
        ## default. Return the input geometry unchanged.
        transform_geom = _unchanged_geom
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
        geoms_iter: GeometryIterable,
        aoi_geom: Optional[GeomObject] = None,
        geoms_epsg: Optional[int] = None,
        geoms_tab_name: Optional[str] = None,
        geoms_match: Callable[[GeomObject, GeomObject], bool] = None,
        get_search_frame: Callable[[GeomObject], GeomObject] = None,
        ncores: Optional[int] = None,
    ) -> Generator[GeomObject]:  # , **kwargs):
        """Identify *non-matching* **input** geometries.

        The function takes as input geometrical features, and searches
        for *reference features* in one table of the internal database
        which geometries are considered to *match* that of the *input
        features*. All *input features* that **DO NOT** have a
        geometry that matches the geometry of any *reference features*
        will be yielded back by the function.

        Parameters
        ----------
        geoms_iter : iterable of `.GeomObject`
            Iterable of input geometrical features to compare to the
            features of the internal SQLite database.
        aoi_geom : `.GeomObject`, optional
            *Area of interest*, within which the database's features must
            lie.
        geoms_epsg : `int`, optional
            EPSG code of the input geometrical features (including
            ``aoi_geom`` if specified). If the ``geoms_epsg`` is not
            specified by the user, the function will assume that the
            *input features* are in the same spatial reference system
            as the *reference features*.
        geoms_tab_name : `str`, optional
            Name of the table where database's features that will be used
            as reference are stored. If no argument is passed to the
            ``geoms_tab_name`` parameter, the function will search for
            *reference features* in a table named *default_table*.
        geoms_match : `callable`, optional
            Comparison function that takes two positional arguments:

            - ``gtest``: *input* geometry (`.GeomObject`)
            - ``gref``: *reference* geometry (`.GeomObject`)

            The function returns ``True`` if it finds that both
            geometries *match*, else returns ``False``. If this
            parameter is omitted, the *input* geometrical feature will
            always be considered as a *match* in the case where its
            *search frame* (see ``get_search_frame`` parameter)
            interesects with one of the features from the database's
            table.
        get_search_frame : `callable`, optional
            Function that takes as single argument an *input* geometry
            (`.GeomObject`) and returns its *search frame*
            (`.GeomObject`). If this parameter is omitted, the *search
            frame* will be the same as the *input* geometry.
        ncores : `int`, optional
            Number of cores to use for running the function. If
            unspecified, the function will run in a single process

        Yields
        ------
        `.GeomObject`
            *Non-matching input* geometrical features.

        Notes
        -----
        If the *spatial reference system* of the *input* geometrical
        features is different from that of the database's features,
        the *input features*' coordinates are reprojected on-the-fly,
        before being compared to features stored in the database. If
        an *input feature* is **NOT** considered to be a *match*, it
        is yielded back unchanged (its coordinates in the original
        *spatial reference system*).
        """
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
            get_search_frame = _unchanged_geom
        ## Coordinates of input geometries are not transformed by
        ## default. Return the input geometry unchanged.
        transform_geom = _unchanged_geom
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
        geoms_iter: GeometryIterable,
        geom_type: Optional[SpatialiteGeomType] = None,
        aoi_geom: Optional[GeomObject] = None,
        geoms_epsg: Optional[int] = None,
        geoms_tab_name: Optional[str] = None,
        geoms_match: Callable[[GeomObject, GeomObject], bool] = None,
        get_search_frame: Callable[[GeomObject], GeomObject] = None,
        ncores: Optional[int] = None,
    ) -> Generator[GeomObject]:  # , **kwargs):
        """Identify (missing) *non-matching* **reference** geometries.

        The function takes as input geometrical features, and searches
        for *reference features* in one table of the internal database
        which geometries are **NOT** considered to *match* the
        geometry of any feature from the input set. All *reference
        features* that **DO NOT** have a geometry that *matches* the
        geometry of any *input features* will be yielded by the
        function.

        Parameters
        ----------
        geoms_iter : iterable of `.GeomObject`
            Iterable of input geometrical features to compare to the
            features of the internal SQLite database.
        geom_type : `SpatialiteGeomType`, optional
            Geometry type of the input geometrical features. If the
            ``geom_type`` is not specified by the user, the function
            will assume that the *input features* have the same
            ``geom_type`` as the *reference features*.
        aoi_geom : `.GeomObject`, optional
            *Area of interest*, within which the database's features must
            lie.
        geoms_epsg : `int`, optional
            EPSG code of the input geometrical features (including
            ``aoi_geom`` if specified). If the ``geoms_epsg`` is not
            specified by the user, the function will assume that the
            *input features* are in the same spatial reference system
            as the *reference features*.
        geoms_tab_name : `str`, optional
            Name of the table where database's features that will be used
            as reference are stored. If no argument is passed to the
            ``geoms_tab_name`` parameter, the function will search for
            *reference features* in a table named *default_table*.
        geoms_match : `callable`, optional
            Comparison function that takes two positional arguments:

            - ``gtest``: *input* geometry (`.GeomObject`)
            - ``gref``: *reference* geometry (`.GeomObject`)

            The function returns ``True`` if it finds that both
            geometries *match*, else returns ``False``. If this
            parameter is omitted, the *input* geometrical feature will
            always be considered as a *match* in the case where its
            *search frame* (see ``get_search_frame`` parameter)
            interesects with one of the features from the database's
            table.
        get_search_frame : `callable`, optional
            Function that takes as single argument an *input* geometry
            (`.GeomObject`) and returns its *search frame*
            (`.GeomObject`). If this parameter is omitted, the *search
            frame* will be the same as the *input* geometry.
        ncores : `int`, optional
            Number of cores to use for running the function. If
            unspecified, the function will run in a single process

        Yields
        ------
        `.GeomObject`
            *Non-matching reference* geometrical features.

        Notes
        -----
        If the *spatial reference system* of the *input* geometrical
        features is different from that of the database's features,
        the *input features*' coordinates are reprojected on-the-fly,
        before being compared to features stored in the database.
        """
        self.logger.info("Searching missing geometries...")
        query_kwargs = dict()
        if geoms_tab_name is None:
            geoms_tab_name = "default_table"
        db_info = self.db_geom_info()
        tab_info = db_info.get(geoms_tab_name, None)
        if tab_info is None:
            raise RuntimeError(
                f"No {geoms_tab_name!r} table was found in the database!"
            )
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
        query = self._get_spatial_query(
            spatial_index=aoi_geom is not None,
            only_within_aoi=aoi_geom is not None,
        )
        cursor = self._conn.cursor()
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
        self, ncores, iterable, *method_args, method_name="", **method_kwargs
    ):
        """Parallelize function generator methods.

        The function enables to parallelize (use of multiple cores) methods
        which take as input an iterable and return a generator.

        Parameters
        ----------
        ncores : `int`
            Number of cores to use for parallelizing the method.
        iterable : `Iterable`
            Input iterable.

        Notes
        -----
        The parallelized method can take any number of positional and
        keyword arguments, as long as the input iterable is their first
        positional argument.
        """
        if not method_name:
            method_name = inspect.stack()[1][3]
        method_obj = getattr(self, method_name, None)
        if method_obj is None:
            raise ValueError(
                f"{method_name!r} is not a valid method name for "
                f"the class {cls.__name__!r}!"
            )
        # Same logging configuration for the workers as for the parent process.
        logger_conf = {
            "name": self.logger.name,
            "level": self.logger.getEffectiveLevel(),
        }
        method_kwargs["logger_conf"] = logger_conf
        # Split input iterable into ncores lists of equal length. Note: the
        # use of lists is for compatibility with the mutliprocessing
        # module.
        iterables = split_iter_to_lists(iterable, ncores)
        # Object to collect the results from the methods running in parallele.
        shared_results_iter = SharedIterator()
        # Sentinel value for sharing status amongst processes and knowing
        # when all workers are done.
        nprocs_done = mp.Value("i", 0)
        procs = list()
        for i in range(ncores):
            procs.append(
                mp.Process(
                    target=self._wrap_method_return,
                    args=[
                        method_obj,
                        iterables[i],
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
            del iterables
            raise
        else:
            return shared_results_iter

    def _wrap_method_return(
        self,
        method_obj,
        iterable,
        shared_results_iter,
        nprocs_done,
        *method_args,
        logger_conf=None, # must be here!!
        **method_kwargs,
    ):
        """Wraps the generators returned by the parallelized method, and
        appends their elements to a shared object compatible with
        multiprocessing.
        """
        if logger_conf is None:
            logger = _setup_logger(name=str(uuid.uuid1()), level=None)
        else:
            logger_conf["name"] = (
                logger_conf.get("name", type(self).__name__) + f" (PID: {os.getpid()})"
            )
            logger = _setup_logger(**logger_conf)
        logger.info("Start worker function.")
        res = list(method_obj(iterable, *method_args, **method_kwargs))
        n_geoms = len(res)
        if n_geoms > 1:
            logger.info(f"{n_geoms} geometries found.")
        else:
            logger.info(f"{n_geoms} geometry found.")
        shared_results_iter.put_iter(res)
        with nprocs_done.get_lock():
            nprocs_done.value += 1
        logger.info("Worker function completed.")
