# -*- coding: utf-8 -*-



from functools import partial

import numpy as np


dispatch_funcs = list()

def dispatch_function(func):
    dispatch_funcs.append(func.__name__)

def geoms_always_match(gtest, gref):
    return True

def _perc_area_ptest_overlap(gtest, gref, threshold=None):
    intersection = gtest.intersection(gref)
    return (intersection.area / gtest.area) >= threshold

def _perc_area_pref_overlap(gtest, gref, threshold=None):
    intersection = gtest.intersection(gref)
    return (intersection.area / gref.area) >= threshold

def _perc_area_both_overlap(gtest, gref, threshold=None):
    intersection = gtest.intersection(gref)
    return min(intersection.area / gtest.area,
               intersection.area / gref.area) >= threshold

def _mean_perc_area_overlap(gtest, gref, threshold=None):
    intersection = gtest.intersection(gref)
    return np.mean([intersection.area / gtest.area,
                    intersection.area / gref.area]) >= threshold

def _intersection_over_union(gtest, gref, threshold=None):
    intersection = gtest.intersection(gref)
    union = gtest.union(gref)
    return (intersection.area / union.area) >= threshold


_strategy_mapping = {"ptest": _perc_area_ptest_overlap,
                     "pref": _perc_area_pref_overlap,
                     "both": _perc_area_both_overlap,
                     "mean": _mean_perc_area_overlap,
                     "IoU": _intersection_over_union}

@dispatch_function
def polygons_area_match(strategy, threshold):
    if strategy not in _strategy_mapping.keys():
        raise ValueError("The strategy parameter must be passed one of {}!"
                         .format(", ".join([f"{s!r}"
                                            for s in _strategy_mapping.keys()])))
    try:
        assert 0 < float(threshold) <= 1
    except (AssertionError, ValueError):
        raise ValueError("The threshold parameter must passed a floating point "
                         "number between 0.0 (excluded) and 1.0 (included)!")
    return partial(_strategy_mapping[strategy], threshold=float(threshold))

