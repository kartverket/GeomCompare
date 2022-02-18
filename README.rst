.. These are examples of badges you might want to add to your README:
   please update the URLs accordingly

..    .. image:: https://api.cirrus-ci.com/github/<USER>/GeomCompare.svg?branch=main
..        :alt: Built Status
..        :target: https://cirrus-ci.com/github/<USER>/GeomCompare
..    .. image:: https://readthedocs.org/projects/GeomCompare/badge/?version=latest
..        :alt: ReadTheDocs
..        :target: https://GeomCompare.readthedocs.io/en/stable/
..    .. image:: https://img.shields.io/coveralls/github/<USER>/GeomCompare/main.svg
..        :alt: Coveralls
..        :target: https://coveralls.io/r/<USER>/GeomCompare
..    .. image:: https://img.shields.io/pypi/v/GeomCompare.svg
..        :alt: PyPI-Server
..        :target: https://pypi.org/project/GeomCompare/
..    .. image:: https://img.shields.io/conda/vn/conda-forge/GeomCompare.svg
..        :alt: Conda-Forge
..        :target: https://anaconda.org/conda-forge/GeomCompare



===========
GeomCompare
===========


Compare two sets of geometrical/geographical features.


*GeomCompare* provides multiple tools for comparing two independant
sets of geometrical/geographical features. It can be used to identify
features with similar geometry/geography (based on pre-defined
similarity functions) found in both sets, as well features with
geometry/geography that are found in only one of the
sets. *GeomCompare* defines a few similarity functions, but it
possible for the user to define its own customized similarity
functions.


Requirements
------------

*GeomCompare* requires ``Python >= 3.9``.

In addition, for a fully fledged installation of *GeomCompare* and
to have access to all functionalities provided by the library, the
user need to install the following:

* ``shapely``
* ``numpy``
* ``psycopg2``
* ``rtree``
* ``pyproj``
* ``gdal`` (core libraries and Python bindings)
* ``spatialite``
   .. note::

      ``mod_spatialite`` must be installed and accessible from ``sqlite3``:

      .. code-block:: python

	 import sqlite3
	 conn = sqlite3.connect(":memory:")
	 conn.enable_load_extension(True)
	 conn.load_extension("mod_spatialite")


Installation
------------

PIP
"""

If you use ``pip``, you can install *GeomCompare* with:

  ``pip install geomcompare``


Docker
------

A *Docker* image for *GeomCompare* is also available on *DockerHub*:

   * Run the geomcompare image and start an iPython session inside the container:

     ``docker run -it mtachon/geomcompare``

   * Run the geomcompare image, and mount the current directory into the
     *data* folder of the container:

     ``docker run --entrypoint bash -v `pwd`:/data -w /data -it mtachon/geomcompare``

For more information on *Docker* and command-line arguments, see:
https://docs.docker.com/ and
https://docs.docker.com/engine/reference/run/ .
