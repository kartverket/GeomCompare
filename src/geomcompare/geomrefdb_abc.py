# -*- coding: utf-8 -*-


import abc
from collections.abc import Sequence
import multiprocessing as mp
import signal

from .stats import recall_score, precision_score, f1_score


class GeomRefDB(abc.ABC):


    @abc.abstractmethod
    def true_positives(self, geoms_iter, geoms_EPSG, same_geoms_func):
        """ Return an iterable of input geometries that are matching
        geometries of the GeomRefDB instance.
        """
            

    @abc.abstractmethod
    def false_positives(self, geoms_iter, geoms_EPSG, same_geoms_func):
        """ Return an iterable of input geometries that are not matching
        any geometry of the GeomRefDB instance.
        """


    @abc.abstractmethod
    def missing_geometries(self, geoms_iter, AOI_geom, geoms_EPSG,
                           same_geoms_func):
        """ Return an iterable of geometries of the GeomRefDB that are
        not matching any of the input geometries.
        """


    def get_recall_score(self, geoms_iter, AOI_geom, geoms_EPSG,
                         same_geoms_func):
        if not isinstance(geoms_iter, Sequence):
            geoms_iter = list(geoms_iter)
        try:
            tp_num = len(self.true_positives(geoms_iter,
                                             geoms_epsg=geoms_EPSG,
                                             geoms_match=same_geoms_func))
        except TypeError:
            tp_num = sum(1 for _ in self.true_positives(geoms_iter,
                                                        geoms_epsg=geoms_EPSG,
                                                        geoms_match=same_geoms_func))
        try:
            mg_num = len(self.missing_geometries(geoms_iter,
                                                 aoi_geom=AOI_geom,
                                                 geoms_epsg=geoms_EPSG,
                                                 geoms_match=same_geoms_func))
        except TypeError:
            mg_num = sum(1 for _ in self.missing_geometries(geoms_iter,
                                                            aoi_geom=AOI_geom,
                                                            geoms_epsg=geoms_EPSG,
                                                            geoms_match=same_geoms_func))
        return recall_score(tp_num, mg_num)

        
    def get_precision_score(self, geoms_iter, geoms_EPSG, same_geoms_func):
        if not isinstance(geoms_iter, Sequence):
            geoms_iter = list(geoms_iter)
        try:
            tp_num = len(self.true_positives(geoms_iter,
                                             geoms_epsg=geoms_EPSG,
                                             geoms_match=same_geoms_func))
        except TypeError:
            tp_num = sum(1 for _ in self.true_positives(geoms_iter,
                                                        geoms_epsg=geoms_EPSG,
                                                        geoms_match=same_geoms_func))
        try:
            fp_num = len(self.false_positives(geoms_iter,
                                              geoms_epsg=geoms_EPSG,
                                              geoms_match=same_geoms_func))
        except TypeError:
            fp_num = sum(1 for _ in self.false_positives(geoms_iter,
                                                         geoms_epsg=geoms_EPSG,
                                                         geoms_match=same_geoms_func))            
        return precision_score(tp_num, fp_num)


    def get_f1_score(self, geoms_iter, AOI_geom, geoms_EPSG,
                     same_geoms_func):
        if not isinstance(geoms_iter, Sequence):
            geoms_iter = list(geoms_iter)
        try:
            tp_num = len(self.true_positives(geoms_iter,
                                             geoms_epsg=geoms_EPSG,
                                             geoms_match=same_geoms_func))
        except TypeError:
            tp_num = sum(1 for _ in self.true_positives(geoms_iter,
                                                        geoms_epsg=geoms_EPSG,
                                                        geoms_match=same_geoms_func))
        try:
            fp_num = len(self.false_positives(geoms_iter,
                                              geoms_epsg=geoms_EPSG,
                                              geoms_match=same_geoms_func))
        except TypeError:
            fp_num = sum(1 for _ in self.false_positives(geoms_iter,
                                                         geoms_epsg=geoms_EPSG,
                                                         geoms_match=same_geoms_func))
        try:
            mg_num = len(self.missing_geometries(geoms_iter,
                                                 aoi_geom=AOI_geom,
                                                 geoms_epsg=geoms_EPSG,
                                                 geoms_match=same_geoms_func))
        except TypeError:
            mg_num = sum(1 for _ in self.missing_geometries(geoms_iter,
                                                            aoi_geom=AOI_geom,
                                                            geoms_epsg=geoms_EPSG,
                                                            geoms_match=same_geoms_func))
            return f1_score(tp_num, fp_num, mg_num)


    def compare_full(self, geoms_iter, AOI_geom, geoms_EPSG, same_geoms_func):
        if not isinstance(geoms_iter, Sequence):
            geoms_iter = list(geoms_iter)
        results = dict()
        results['true_positives'] = list(self.true_positives(geoms_iter,
                                                            geoms_epsg=geoms_EPSG,
                                                            geoms_match=same_geoms_func))
        results['false_positives'] = list(self.false_positives(geoms_iter,
                                                               geoms_epsg=geoms_EPSG,
                                                               geoms_match=same_geoms_func))
        results['missing_geometries'] = list(self.missing_geometries(geoms_iter,
                                                                      aoi_geom=AOI_geom,
                                                                      geoms_epsg=geoms_EPSG,
                                                                      geoms_match=same_geoms_func))
        tp_num = len(results['true_positives'])
        fp_num = len(results['false_positives'])
        mg_num = len(results['missing_geometries'])
        results['recall'] = recall_score(tp_num, mg_num)
        results['precision'] = precision_score(tp_num, fp_num)
        results['f1'] = f1_score(tp_num, fp_num, mg_num)
        return results
            
