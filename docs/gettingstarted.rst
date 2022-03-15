===============
Getting started
===============

If you have not installed *GeomCompare* yet, you can follow the
:ref:`installation instructions <installation>`.


Input/Output
------------

Load a geometry dataset from disk
"""""""""""""""""""""""""""""""""

**Load geometrical features from a Shapefile:**

.. code-block:: python

   from geomcompare.io import extract_geoms_from_file

   filename = "/path/to/my/file.shp"

   # The names of supported OGR/GDAL drivers for opening files with
   # geometrical features are listed in
   # https://gdal.org/drivers/vector/index.html
   driver_name = "ESRI Shapefile" # driver name for opening shapefiles

   # Get an iterator of the geometries
   geoms = extract_geoms_from_file(filename, driver_name)

   # Note: the file will only be closed after the
   # "extract_geoms_from_file" has yielded the last geometry.
   # If you intend to iterate through "geoms" multiple times, you can
   # store the geometries in a list instead
   geoms_list = list(geoms) # now the file is closed

**Filtering the extracted geometrical features:**

.. code-block:: python

   from geomcompare.io import extract_geoms_from_file, LayerFilter

   filename = "/path/to/my/file.json"
   driver_name = "GeoJSON"

   # Extract only geometrical features from the layer "my_lyr"
   geoms_my_lyr = extract_geoms_from_file(
       filename=filename,
       driver_name=driver_name,
       layers=["my_lyr"],
   )

   # Extract only the first 10 geometrical features from the layer
   # "my_lyr"
   lyr_filter = LayerFilter(fids=list(range(10)))
   geoms_my_lyr_10 = extract_geoms_from_file(
       filename=filename,
       driver_name=driver_name,
       layers=["my_lyr"],
       layer_filters=[lyr_filter],
   )

   # Area of interest
   aoi = list(
       extract_geoms_from_file("/path/to/aoi_poly.shp", "ESRI Shapefile")
   )[0]

   # Extract features from the layer "large_lyr" that are within the
   # aoi polygon, as well as all features from other layers
   lyr_filter = LayerFilter(layer_id="large_lyr", aoi=aoi)
   filtered_geoms = extract_geoms_from_file(
       filename=filename,
       driver_name=driver_name,
       layer_filters=[lyr_filter],
   )

   # Extract only features which "distance" attribute is inferior to
   # 1000
   lyr_filter = LayerFilter(attr_filter="distance < 1000")
   geoms_dist_inf_1000 = extract_geoms_from_file(
       filename=filename,
       driver_name=driver_name,
       layer_filters=[lyr_filter],
   )

Load a geometry dataset from a PostGIS database
"""""""""""""""""""""""""""""""""""""""""""""""

.. code-block:: python

   from geomcompare.io import fetch_geoms_from_pg, ConnectionParameters, SchemaTableColumn

   # Pass the correct values to keyword parameters
   conn_params = ConnectionParameters(
       host="host_name",
       dbname="db_name",
       user="my_user",
       password="my_pwd",
       port=5432,
   )

   # Using some fictive database layout
   geoms_location = SchemaTableColumn(
       schema="building",
       table="public",
       column="geom",
   )

   # Open a connection to the database and get an iterator of the
   # geometries. The connection stays opened until the function has
   # yielded the last geometry at that location in the database.
   geoms = fetch_geoms_from_pg(
       conn_params=conn_params, geoms_col_loc=geoms_location,
   )

   # Store the geometries in a list and close the connection.
   geoms_list = list(geoms)

   # Get the same geometries, but this time using the "sql_query"
   # parameter instead of the "geoms_col_loc" parameter. Any SQL query
   # which return geometrical features can be passed as argument.
   geoms_list = list(fetch_geoms_from_pg(
       conn_params=conn_params,
       sql_query="SELECT geom FROM building.public;",
   ))

   # Area of interest
   aoi = list(
       extract_geoms_from_file("/path/to/aoi_poly.shp", "ESRI Shapefile")
   )[0]
   # Get an iterator of the geometries from the same geometry column,
   # but only those which lie within the aoi polygon. The
   # "output_epsg" parameter can be use to reproject the geometries to
   # the wanted spatial reference system.
   geoms = fetch_geoms_from_pg(
       conn_params=conn_params,
       geoms_col_loc=geoms_location,
       aoi=aoi,
       output_epsg=25833,
   )

Write a geometry dataset to disk
""""""""""""""""""""""""""""""""

.. warning::
   When writing to disk, *GeomCompare* assumes that all geometrical
   features have the same geometry
   type. :py:func:`write_geoms_to_file` will not check for geometry
   type homogeneity and will instead throw an error if the features
   have different geometry types. If the features have different
   geometry types, you can still group them into multiple datasets of
   homogeneous geometry type, and write these datasets to the same
   file on different layers, if the data format supports it, as shown
   below.

**Write a list of geometrical features to Shapefile:**

.. code-block:: python

   from geomcompare.io import write_geoms_to_file

   filename = "/path/to/output/file.shp"
   driver_name = "ESRI Shapefile"

   # "geoms_list" is our list of geometrical features
   write_geoms_to_file(
       filename=filename,
       driver_name=driver_name,
       geoms_iter=geoms_list,
       geoms_epsg=4326, # not required, but good practice if available
   )

**Write two datasets with different geometry types to the same GeoPackage file:**

.. code-block:: python

   from geomcompare.io import write_geoms_to_file

   filename = "/path/to/output/file.gpkg"
   driver_name = "GPKG"

   write_geoms_to_file(
       filename=filename,
       driver_name=driver_name,
       geoms_iter=points_list,
       geoms_epsg=25833,
       layer="my_point_layer",
   )

   write_geoms_to_file(
       filename=filename,
       driver_name=driver_name,
       geoms_iter=polygons_list,
       geoms_epsg=4326,
       layer="my_polygon_layer",
       mode="update",
   )

.. note::
   If the ``geoms_epsg`` parameter is given, and the layer where the
   geometrical/geographical features are to be written on has a
   different Spatial Reference System, the geometries' coordinates
   will be re-projected on-the-fly.
