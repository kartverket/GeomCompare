## v0.2.0 (2022-03-04)

### Refactor

- Renaming geoms_iter -> iterable and geoms_iters -> iterables inside
- Add type and type hints/annotations Add Literal type SUPPORTED_GEOM_TYPE, types hints for signatures of functions in io.py and methods of SQLiteGeomRefDB.
- Prefix with underscore setup_logger and update_logger functions
- Change import statements to support type hints

### Fix

- Change public attributes of SQLiteGeomRefDB to properties

### Feat

- Make "geoms_epsg" optional for io.write_geoms_to_file
- Add information on features count for SQLiteGeomRefDB.db_geom_info()
