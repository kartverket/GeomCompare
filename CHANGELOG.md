## v0.2.2 (2022-03-18)

### Fix

- **misc.py**: rename misc.py -> _misc.py

## v0.2.1 (2022-03-17)

### Refactor

- **geomrefdb.py**: rename SUPPORTED_GEOM_TYPE -> SpatialiteGeomType
- prefix a few private variables/functions with an underscore
- Change LayerID from TypeVar to Union.
- Type for shapely geometrical object and reformatting of docstrings
