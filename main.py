import json
import re
from xml.etree.ElementInclude import include
from requests.auth import HTTPBasicAuth
import requests
import pandas as pd
from pandas import json_normalize

capacity = 10#input("Enter your capacity: ")
latitude = 47.104496# input("Enter your latitude: ")
longitude = 15.841415#input("Enter your logitude: ")
countryID = 'AT'
OperatorID = '000'

#include auth.txt



keyfile=open('ladestellen-apikey.txt')
API_KEY = keyfile.readline().rstrip()
#url = 'https://api.e-control.at/charge/1.0/search?capacity={cap}&latitude={lat}&longitude={long}'.format(cap=capacity,lat=latitude,long=longitude)
url = 'https://api.e-control.at/charge/1.0/countries/AT/operators'

print(url)
headers = {'Accept': 'application/json',}
auth = HTTPBasicAuth('stefanpirker', 'xWQ24m2z3AqKP4HcarZz',)
response = requests.get(url, headers=headers, auth=auth)
#print(response.headers)
#print(response.content)
#data = json.loads(response.decode('utf-8'))
data = requests.get(url, headers=headers, auth=auth).json()
data_set = data#[0]
#data_location = data_set['stationId']
#print(data_location)
test = json_normalize(data_set)
#print(test)
print(test)