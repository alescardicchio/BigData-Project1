#!/usr/bin/env python3
"""reducer.py"""

import sys
from datetime import datetime

year_2_word_occurrences = { }

for row in sys.stdin:

    row = row.strip()

    current_unixtime, current_word, current_count = row.split("\t")

    try:
        current_count = int(current_count)
        current_unixtime = int(current_unixtime)
        current_year = int(datetime.utcfromtimestamp(current_unixtime).strftime('%Y'))
    except ValueError:
        continue

    if current_year not in year_2_word_occurrences:
        year_2_word_occurrences[current_year] = { }
    
    
    if current_word not in year_2_word_occurrences[current_year]:
        year_2_word_occurrences[current_year][current_word] = 0
    
    year_2_word_occurrences[current_year][current_word] += current_count

 
sorted_output = {key: dict(sorted(value.items(), key=lambda item: item[1], reverse=True))
                for key, value in year_2_word_occurrences.items()}

for key in sorted_output.keys():
    print('%i\t' % key)
    for item in list(sorted_output[key].keys())[:10]:
        print("%s\t%i" % (item, sorted_output[key][item]))
        