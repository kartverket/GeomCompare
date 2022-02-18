===============
Getting started
===============

If you have not installed *GeomCompare* yet, you can follow the
:ref:`instalation instructions <_readme>`.

Load a geometry dataset from disk
"""""""""""""""""""""""""""""""""

.. code-block:: python

   from geomcompare.io import extract_geoms_from_file, LayerFilter


.. code-block:: python

   # Import one of the three main classes of the library, as well as
   # other utility functions
   from geomcompare import SQLiteGeomRefDB
   from geomcompare.io import extract_geoms_from_file, write_geoms_to_file
   from geomcompare.comparefunc import polygons_area_match
