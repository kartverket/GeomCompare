# -*- coding: utf-8 -*-

from shapely.geometry import Point, Polygon
import pytest

from geomcompare.geomutils import to_2D, unchanged_geom


@pytest.fixture
def geoms_3D():
    pt3D = Point((0.0, 1, 4.5))
    poly3D = Polygon(((0.0, 0.0, 0.0), (1.2, 0.5, -1), (1.5, 1, -2.1)))
    return [pt3D, poly3D]


def test_to_2D(geoms_3D):
    assert all(g.has_z for g in geoms_3D)
    assert all(not g.has_z for g in map(to_2D, geoms_3D))


def test_unchanged_geom(geoms_3D):
    assert all(g.equals(unchanged_geom(g)) for g in geoms_3D)
