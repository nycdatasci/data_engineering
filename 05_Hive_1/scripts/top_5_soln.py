#!/usr/bin/python3

import sys
import bisect

largest = list()
smallest = list()

for line in sys.stdin:
    state, area = line.strip().split('\t', 1)
    area = float(area)
    bisect.insort(largest, (-area, state))
    bisect.insort(smallest, (area, state))
    if len(largest) > 5:
        largest.pop()
    if len(smallest) > 5:
        smallest.pop()
else:
    for area, state in largest:
        print('{}\t{:,}'.format(state, -round(area, 2)))
    for area, state in smallest:
        print('{}\t{:,}'.format(state, round(area, 2)))
