# -*- coding: utf-8 -*-


import queue
import multiprocessing as mp


def gen_from_queue(q, lock):
    try:
        with lock:
            yield q.get(block=False)
    except queue.Empty:
        raise StopIteration

def iter_to_queue(it, q, lock):
    with lock:
        for el in iter(it):
            q.put((el))


class SharedIterator:

    @property
    def lock(self):
        return self._l

    @property
    def size(self):
        with self._s.get_lock():
            size = self._s.value
        return size

    def __init__(self, it=None, lock=None):
        self._q = mp.Queue()
        self._l = mp.Lock() if lock is None else lock
        self._s = mp.Value("i", 0)
        if it is not None:
            for count, el in enumerate(it, 1):
                self._q.put(el)
            self._s.value += count


    def __next__(self):

        while self.size:
            try:
                el = self._q.get(block=False)
            except queue.Empty:
                continue
            with self._s.get_lock():
                self._s.value -= 1
            return el
        raise StopIteration()


    def __iter__(self):
        return self


    def put(self, el):
        self._q.put(el)
        with self._s.get_lock():
            self._s.value += 1

    def put_iter(self, it):
        with self._l:
            for count, el in enumerate(it, 1):
                self._q.put(el)
        with self._s.get_lock():
            self._s.value += count


def split_iter_to_lists(it, nlists):
    lists = [[] for _ in range(nlists)]
    for i,el in enumerate(it):
        lists[i % nlists].append(el)
    return lists
