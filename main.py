import json
import pathlib
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

##### Print part of Table first and then just read first and third column
print(test.iloc[:,0:4])
single_op=test.iloc[0,:].to_frame()
header_info = (test.columns)
header_info_list = header_info[[0,2,9]]

#### Now apply just the two columns from above over the whole table to get just the 2 needed columns for the sql
#header_info_list = header_info[0,3]
table_sub = test[header_info_list]
from pathlib import Path
filepath = Path('out.csv')
table_sub.to_csv(filepath)
#print(table_sub)

#print(header_info)
#print(header_info[0:3])
#print(header_info[11])

#### Now make the API url just for the operatorID
get_operatorId = list(test[header_info[0]])

boolx=False
counter = 0

if boolx == False:
    for i in get_operatorId:
        urlop = 'https://api.e-control.at/charge/1.0/countries/AT/operators/{operator_id}'.format(operator_id=str(i))
        counter = counter +1
        print(urlop)
        data_op = requests.get(urlop, headers=headers, auth=auth).json()
        test_op = json_normalize(data_op)
        #print(urlop)
        #print(data_op)
        #print(i)
        print(test_op.shape)
        if (counter == 20):
            print(test_op)



#print(get_operatorId)
print("---------------------------------------------------------------")
print(single_op)