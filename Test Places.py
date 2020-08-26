# -*- coding: utf-8 -*-
"""
Created on Thu Jul 30 13:38:48 2020

@name: Test Places

@author: Henry
"""


#%% initial stuff
import requests
import pandas as pd

api_key = ''


#%% search for place

parameters = {"key": api_key, 
              "input": 'painshill park', 
              "inputtype": 'textquery',
              "fields" : 'business_status,formatted_address,geometry,icon,name,place_id,plus_code,types,price_level,rating,user_ratings_total'}

response = requests.post("https://maps.googleapis.com/maps/api/place/findplacefromtext/json", params=parameters)

print(response.text)

#place = pd.read_json(response.text)
place2 = response.json()
place3 = pd.DataFrame(place2.get('candidates'))

#%% get place details

parameters2 = {"key": api_key, 
              "place_id": place3['place_id'][0]}

response2 = requests.post("https://maps.googleapis.com/maps/api/place/details/json", params=parameters2)

print(response2.text)

further_details = response2.json().get('result')
review_details = pd.DataFrame(further_details.get('reviews'))

#%% get places

parameters3 = {"key": api_key, 
               "location" : '51.25,-0.01',
               "radius" : '50000',
               "type" : 'library'}

response3 = requests.post("https://maps.googleapis.com/maps/api/place/nearbysearch/json", params=parameters3)

print(response3.text)

place4 = response3.json()
place5 = pd.DataFrame(place4.get('results'))

#%% get next 20

parameters4 = {"key": api_key,
               "pagetoken" : place4['next_page_token']}

response4 = requests.post("https://maps.googleapis.com/maps/api/place/nearbysearch/json", params=parameters4)

print(response4.text)




