# -*- coding: utf-8 -*-
"""
GeomCompare
===========

The *Geomcompare* package provides multiple tools for comparing two
independant sets of geometrical features.

Documentation for *GeomCompare* is available in the form of docstrings
provided with the code, as well as on the project's homepage
`<https://geomcompare.readthedocs.io/en/latest/>`_.

Available submodules
--------------------

geomrefdb
    Defines the main classes of the library used for comparing
    geometry datasets.
io
    Provides a set of tools for I/O operations, extracting geometrical
    features from disk or from a PostGIS database, as well as writing
    a dataset of geometries to disk.
comparefunc
    Defines a few comparison functions to use with the geomrefdb's
    main classes.
geomutils
    Defines a few functions and types to work with :ref:`shapely
    geometrical objects <shapely:objects>`.
stats
    Defines functions for computing classifier metrics (e.g. when
    comparing a result dataset from a machine learning model with a
    reference dataset).
"""


import sys

from .geomrefdb import PostGISGeomRefDB, RtreeGeomRefDB, SQLiteGeomRefDB


if sys.version_info[:2] >= (3, 8):
    # TODO: Import directly (no need for conditional) when `python_requires = >= 3.8`
    from importlib.metadata import PackageNotFoundError, version  # pragma: no cover
else:
    from importlib_metadata import PackageNotFoundError, version  # pragma: no cover

try:
    # Change here if project is renamed and does not equal the package name
    dist_name = __name__
    __version__ = version(dist_name)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"
finally:
    del version, PackageNotFoundError
