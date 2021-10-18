# -*- coding: utf-8 -*-

def recall_score(tp_num, mg_num):
    return tp_num / (tp_num + mg_num)


def precision_score(tp_num, fp_num):
    return tp_num / (tp_num + fp_num)


def f1_score(tp_num, fp_num, mg_num):
    return tp_num / (tp_num + ((mg_num + fp_num) / 2))
