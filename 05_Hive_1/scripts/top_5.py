#!/usr/bin/python3

import sys
import bisect

top_5 = list()

for line in sys.stdin:
    state, area = line.strip().split('\t', 1)
    area = float(area)
    bisect.insort(top_5, (-area, state))
    if len(top_5) > 5:
        top_5.pop()
else:
    for area, state in top_5:
        print('{}\t{:,}'.format(state, -round(area, 2)))

