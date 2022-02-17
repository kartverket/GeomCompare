# -*- coding: utf-8 -*-


def recall_score(tp_num: int, mg_num: int):
    return tp_num / (tp_num + mg_num)


def precision_score(tp_num: int, fp_num: int):
    return tp_num / (tp_num + fp_num)


def f1_score(tp_num: int, fp_num: int, mg_num: int):
    return tp_num / (tp_num + ((mg_num + fp_num) / 2))
