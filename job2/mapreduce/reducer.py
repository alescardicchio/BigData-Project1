#!/usr/bin/env python3
"""reducer.py"""

import sys

user_2_favorite_products = { }

for row in sys.stdin:

    row = row.strip()

    current_product, current_user, current_score = row.split("\t")

    try:
        current_score = int(current_score)
    except ValueError:
        continue

    if current_user not in user_2_favorite_products:
        user_2_favorite_products[current_user] = { }
    
    
    if current_product not in user_2_favorite_products[current_user]:
        user_2_favorite_products[current_user][current_product] = current_score
    
 
sorted_output_by_products = {key: dict(sorted(value.items(), key=lambda item: item[1], reverse=True))
                for key, value in user_2_favorite_products.items()}

sorted_output = dict(sorted(sorted_output_by_products.items(), key=lambda item: item[0]))

for key in sorted_output.keys():
    print('%s\t' % key)
    for item in list(sorted_output[key].keys())[:5]:
        print("%s\t%i" % (item, sorted_output[key][item]))
        