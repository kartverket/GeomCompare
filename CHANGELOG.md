# Changelog

## v0.3.0 (2022-03-22)

#### New Features

* (SQLiteGeomRefDB): add the get_geometries method

#### Refactorings

* (comparefunc): rename geoms_always_match -> _geoms_always_match

#### Docs

* change homepage link in README.rst
* add usage section with link to homepage for GitHub repo

#### Others

* remove commitizen section from pyproject.toml
* fix setup.cfg so that version can be found
* import tag and metadata from master

Full set of changes: [`v0.2.2...v0.3.0`](https://github.com/kartverket/GeomCompare/compare/v0.2.2...v0.3.0)

## v0.2.2 (2022-03-18)

#### Refactorings

* prefix a few private variables/functions with an underscore
* Change LayerID from TypeVar to Union.
* Type for shapely geometrical object and reformatting of docstrings

#### Docs

* update CHANGELOG.md
*  Update the README.rst file
* Add content to getting started page.
* Update auto-generated CHANGELOG.md to fix bad formatted commit msg.
* Add shapely to intersphinx_mapping
* "geometrical/geographical" -> "geometrical"
* Fixed table of contents

#### Others

* Add the Dockerfile to the repo.
