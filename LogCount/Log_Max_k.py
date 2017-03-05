#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/3/5 15:09
# @Author  : wttttt
# @Site    : 
# @File    : Log_Max_k.py
# @Software: PyCharm Community Edition
# @desc: to find the Top-k visit IP

import heapq
import sys


def heapq_demo():
    """
    @ demo uasge of the library heapq
    :return: None
    """
    heap_test = []  # list
    value = 1   # value = (1, "test") or value = [1, "test"] ==> key-value
    heapq.heappush(heap_test, value)  # push
    # pop, Pop and return the smallest item from the heap.If the heap is empty, IndexError is raised
    heapq.heappop(heap_test)
    # Push item on the heap, then pop and return the smallest item from the heap
    heapq.heappushpop(heap_test, value)


def get_top_k(filename, k):
    """
    :param filename: type str
    :param k: type int
    :return: type list
    """
    heap = []
    with open(filename) as fi:
        for line in fi:
            ip_address, visit_num = line.strip("\n").split("\t")
            visit_num = int(visit_num)
            heapq.heappush(heap, (visit_num, ip_address))
            if len(heap) > k:
                heapq.heappop(heap)
    return heap

if len(sys.argv) < 3:
    print "Usage: python TopKvisit.py <filename> <K>"
filename, k = sys.argv[1], int(sys.argv[2])
li = get_top_k(filename, k)
for ele in li:
    print ele


