#!/usr/bin/env python3
"""reducer.py"""

import sys
import itertools

user_2_products = {}
user_pair_2_common_products = {}

for row in sys.stdin:

    row = row.strip()

    current_product, current_user = row.split("\t")

    if current_user not in user_2_products:
        user_2_products[current_user] = []
    
    user_2_products[current_user].append(current_product)


for key in list(user_2_products.keys()):
    
    if(len(user_2_products[key]) < 3):
        del user_2_products[key]


for key in itertools.combinations(user_2_products, 2):
    
    common_products = set(user_2_products[key[0]]).intersection(user_2_products[key[1]])

    if(len(common_products) < 3):
        continue

    user_pair_2_common_products[key] = common_products



sorted_output = dict(sorted(user_pair_2_common_products.items(), key=lambda item: item[0][0]))


for key in sorted_output.keys():
    print('%s\t%s\t' % key)
    for item in sorted_output[key]:
        print("%s" % (item))  
