from shapely.geometry import Point

from geomcompare.geomutils import *

def test_to_2D():
    p3D = Point((0.0, 1, 4.5))
    assert p3d.has_z
    p2D = to_2D(p3D)
    assert not p2D.has_z
