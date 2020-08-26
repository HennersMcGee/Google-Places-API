# -*- coding: utf-8 -*-
'''
    File name: run_gmp_data.py
    Author: Henry Letton
    Date created: 2020-08-26
    Python Version: 3.7.3
    Desciption: Program to run for data
'''

from gmp_functions import search_data, get_place_ids, get_extra_info

# Run searches
search_data('college', 'school')


# Get extra data
place_list = get_place_ids(3)

for place in place_list:
    get_extra_info(place)


