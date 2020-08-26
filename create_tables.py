# -*- coding: utf-8 -*-
'''
    File name: create_tables.py
    Author: Henry Letton
    Date created: 2020-08-25
    Python Version: 3.7.3
    Desciption: Create empty tables to store google places data
'''

#%% Required modules

import mysql.connector

#%% Connect to database
conn = mysql.connector.connect(user='u235764393_HL', 
                              password='',
                              host='sql134.main-hosting.eu',
                              database='u235764393_HLDB')

cursor = conn.cursor()

#%% Create table to store places
try:
    cursor.execute("CREATE TABLE GMP_Places ("
            "place_id TEXT,"
            "name TEXT,"
            "business_status TEXT,"
            "vicinity TEXT,"
            "longitude FLOAT,"
            "latitude FLOAT,"
            "rating FLOAT,"
            "user_ratings_total INT,"
            "timestamp_p TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY ( place_id(50) ) )")
except:
    print("Places table already exists")

#%% Create table to store extra places data
try:
    cursor.execute("CREATE TABLE GMP_Places_Extra ("
            "place_id TEXT,"
            "formatted_address TEXT,"
            "postcode TEXT,"
            "formatted_phone_number TEXT,"
            "timestamp_pe TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY ( place_id(50) ) )")
except:
    print("Places extra table already exists")

#%% Create table to store place types
try:
    cursor.execute("CREATE TABLE GMP_Types ("
            "place_id TEXT,"
            "type TEXT,"
            "timestamp_t TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY ( place_id(50),type(50) ) )")
except:
    print("Types table already exists")

#%% Create table to store place reviews
try:
    cursor.execute("CREATE TABLE GMP_Reviews ("
            "place_id TEXT,"
            "author_name TEXT,"
            "description TEXT,"
            "rating INT,"
            "created_at TIMESTAMP,"
            "timestamp_s TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY ( place_id(50),created_at ) )")
except:
    print("Reviews table already exists")


#%% Close connection
conn.close()