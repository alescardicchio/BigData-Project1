#!/usr/bin/env python3
"""mapper.py"""

import sys

for row in sys.stdin:

    row = row.strip()

    if 'ProductId,' in row:
        continue

    _, _, _, _, _, _, time, _, text = row.split(',')

    words = text.split(" ")

    for word in words:
        print('%s\t%s\t%s' % (time, word, 1))
