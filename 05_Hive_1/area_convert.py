#!/usr/bin/python3

import sys

for line in sys.stdin:
    state, area = line.strip().split('\t', 1)
    area = 2.59 * float(area)
    print('{}\t{}'.format(state, area))
