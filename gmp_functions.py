# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
'''
    File name: gmp_functions.py
    Author: Henry Letton
    Date created: 2020-08-24
    Python Version: 3.7.3
    Desciption: Functions to extract data, output datasets, and write data to 
                database.
'''

#%% Load required modules
import requests
import pandas as pd
import numpy as np
from time import sleep
import mysql.connector
from datetime import datetime

api_key = ''

#%% Function to search for google places

def search_places(search = '',
                  place_type = '',
                  centre = '51.23,-0.42',
                  radius = '30000',
                  api_key = api_key):
    
    # User inputs are stored and used in the inital api request
    parameters = {"key": api_key, 
                   "location" : centre,
                   "radius" : radius,
                   "keyword" : search,
                   "type" : place_type}
    
    response = requests.post("https://maps.googleapis.com/maps/api/place/nearbysearch/json", params=parameters)
    
    # Print code, 200 is successful
    print(response.status_code)
    
    # Break down response
    info_json = response.json()
    info_list = info_json.get('results')
    info_df = pd.DataFrame(info_list)
    
    # Token for possible next page and escape variable
    token_next_page = info_json['next_page_token']
    total_page_count = 1
    max_num_ratings = np.nanmax(info_df['user_ratings_total'])
    
    while total_page_count < 10 and max_num_ratings >= 0:
        
        # Wait to allow token to become useable 
        sleep(3)
        
        # API request now gets following pages from initial search
        parameters2 = {"key": api_key,
                       "pagetoken" : token_next_page}
        
        response2 = requests.post("https://maps.googleapis.com/maps/api/place/nearbysearch/json", params=parameters2)
        
        # Print code, 200 is successful
        print(response2.status_code)
        
        # Break down response
        info_json2 = response2.json()
        info_list2 = info_json2.get('results')
        info_df2 = pd.DataFrame(info_list2)
        
        # Add places to main list
        info_list = info_list + info_list2
        
        # If there is no next page, then break (max 3 pages of 20 results each)
        try:
            token_next_page = info_json2['next_page_token']
            
        except:
            print('No next page')
            break
            
        total_page_count += 1
        max_num_ratings = np.nanmax(info_df2['user_ratings_total'])
        
    return info_list

#%% Function to convert list into dataframes

def list_to_data(place_list):

    # To data frame
    place_df = pd.DataFrame(place_list)
    
    # Extract long and lat info
    place_df['longitude'] = [d.get('location').get('lng') if d is not None else np.NaN for d in place_df['geometry']]
    place_df['latitude'] = [d.get('location').get('lat') if d is not None else np.NaN for d in place_df['geometry']]
    
    # Keep only stored fields
    place_df2 = place_df[['place_id', 'name', 'business_status', 'vicinity',\
                     'longitude', 'latitude', 'rating', 'user_ratings_total']]
    
    # Data for types
    types_df = place_df.explode('types').reset_index(drop=True)
    types_df.rename(columns={'types':'type'}, inplace=True)
    
    # Keep only stored fields
    types_df2 = types_df[['place_id', 'type']]
    
    return place_df2, types_df2

#%% Function to write data to database

def data_to_db(place_df2, types_df2):

    # Connect to database
    conn = mysql.connector.connect(user='u235764393_HL', 
                                  password='',
                                  host='sql134.main-hosting.eu',
                                  database='u235764393_HLDB')
    cursor = conn.cursor()
    
    # Place data
    # Cleaning
    place_clean = place_df2.astype(str)
    place_clean = place_clean.replace(r"^nan$", r"NULL", regex = True)
    place_clean = place_clean.replace(r"^None$", r"", regex = True)
    place_clean = place_clean.replace(r"\\", r"\\\\", regex = True)
    place_clean = place_clean.replace(r"'", r"\'", regex = True)
    
    # Insert list
    insert_text_p = "REPLACE INTO GMP_Places "\
                   "(place_id, name, business_status, vicinity, "\
                   "longitude, latitude, rating, user_ratings_total) "\
                   "VALUES ('" + place_clean['place_id'] + "', '" + place_clean['name'] + "', '" +\
                   place_clean['business_status'] + "', '" + place_clean['vicinity'] + "', " +\
                   place_clean['longitude'] + ", " + place_clean['latitude'] + ", " +\
                   place_clean['rating'] + "," + place_clean['user_ratings_total'] + ")"
    
    for insert in insert_text_p:
        cursor.execute(insert)
        print('Row inserted')
    
    # Type data
    # Cleaning
    types_clean = types_df2.astype(str)
    types_clean = types_clean.replace(r"^nan$", r"NULL", regex = True)
    types_clean = types_clean.replace(r"^None$", r"", regex = True)
    types_clean = types_clean.replace(r"\\", r"\\\\", regex = True)
    types_clean = types_clean.replace(r"'", r"\'", regex = True)
    
    # Insert list
    insert_text_t = "REPLACE INTO GMP_Types "\
                   "(place_id, type) "\
                   "VALUES ('" + types_clean['place_id'] + "', '" + types_clean['type'] + "')"
    
    for insert in insert_text_t:
        cursor.execute(insert)
        print('Row inserted')
        
            
    # Close connection to database, commit changes first
    conn.commit()
    conn.close()

#%% Get search data
def search_data(search, place_type):
    
    info_list = search_places(search, place_type)
    
    place_df2, types_df2 = list_to_data(info_list)
    
    data_to_db(place_df2, types_df2)

#%% Extra information on place
def get_extra_info(place_id):

    # API request
    parameters = {"key": api_key, 
                  "place_id": place_id}
    
    response = requests.post("https://maps.googleapis.com/maps/api/place/details/json", params=parameters)
    
    #Check successful
    print(response.status_code)
    
    # Get further place details and review detail
    further_details = response.json().get('result')
    review_details = pd.DataFrame(further_details.get('reviews'))
    
    # Connect to database
    conn = mysql.connector.connect(user='u235764393_HL', 
                                  password='',
                                  host='sql134.main-hosting.eu',
                                  database='u235764393_HLDB')
    cursor = conn.cursor()    
    
    # Get the postcode out from the place details
    try:
        postcode = pd.DataFrame(further_details['address_components'])
        postcode2 = postcode.explode('types').reset_index(drop=True)
        postcode3 = postcode2[postcode2['types'] == 'postal_code']['long_name'].iloc[0]
    except:
        postcode3 = ''
    
    # Get other variables
    try:
        fur_addr = further_details['formatted_address']
    except:
        fur_addr = ''
    try:
        form_phone = further_details['formatted_phone_number']
    except:
        form_phone = ''
    
    # Insert query for place, which is then executed
    insert_text_pe = "REPLACE INTO GMP_Places_Extra "\
                    "(place_id, formatted_address, postcode, formatted_phone_number) "\
                    "VALUES ('" + place_id + "', '" + fur_addr + "', '" +\
                    postcode3 + "','" + form_phone + "')"   
    
    cursor.execute(insert_text_pe)
    print('Row inserted')    
    
    # Only try and store reviews if then are any
    if review_details.shape[0] > 0:
    
        # Manipulate review data
        review_details['created_at'] = [datetime.fromtimestamp(date) for date in review_details['time']]
        review_details.rename(columns={'text':'description'}, inplace=True)
        review_details2 = review_details[['author_name','description','rating','created_at']].astype(str)
        review_details2 = review_details2.replace(r"^nan$", r"NULL", regex = True)
        review_details2 = review_details2.replace(r"^None$", r"", regex = True)
        review_details2 = review_details2.replace(r"\\", r"\\\\", regex = True)
        review_details2 = review_details2.replace(r"'", r"\'", regex = True)
    
        # Insert queries for reviews, which are then executed
        insert_text_r = "REPLACE INTO GMP_Reviews "\
                        "(place_id, author_name, description, rating, created_at) "\
                        "VALUES ('" + place_id + "', '" + review_details2['author_name'] + "', '" +\
                        review_details2['description'] + "', " + review_details2['rating'] + ", '" +\
                        review_details2['created_at'] + "')"
        
        for insert in insert_text_r:
            cursor.execute(insert)
            print('Row inserted')
        
            
    # Close connection to database, commit changes first
    conn.commit()
    conn.close()

#%% Get n place ids to get further information
def get_place_ids(n=10):

    # Connect to database
    conn = mysql.connector.connect(user='u235764393_HL', 
                                  password='',
                                  host='sql134.main-hosting.eu',
                                  database='u235764393_HLDB')
    
    query = ("SELECT a.*, b.timestamp_pe "
              "FROM GMP_Places as a "
              "LEFT JOIN GMP_Places_Extra as b "
              "on a.place_id = b.place_id")
    
    sql_table = pd.read_sql_query(query,conn)
    
    # Close connection to database, commit changes first
    conn.commit()
    conn.close()
    
    # Field for whether extra info has been caputured before
    sql_table['extra_info_missing'] = 1
    sql_table['extra_info_missing'][np.isnat(sql_table['timestamp_pe'])] = 0
    
    # Priority: no further details, oldest, more reviews
    
    sql_table.sort_values(by=['extra_info_missing','timestamp_pe','timestamp_p'], inplace=True, ascending=True)
    
    place_id_list = sql_table['place_id'][0:n]
    
    return place_id_list.tolist()



#%% Testing

search_data('college', 'school')

place_list = get_place_ids(3)

for place in place_list:
    get_extra_info(place)



