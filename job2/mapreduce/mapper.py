#!/usr/bin/env python3
"""mapper.py"""

import sys

for row in sys.stdin:

    row = row.strip()

    if 'ProductId,' in row:
        continue

    product, user, _, _, _, score, _, _, _ = row.split(',')

    
    print('%s\t%s\t%s' % (product, user, score))
