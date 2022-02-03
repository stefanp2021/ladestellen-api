#### Import all Packages
import json
import pathlib
import re
from xml.etree.ElementInclude import include
from requests.auth import HTTPBasicAuth
import requests
import pandas as pd
from pandas import json_normalize
from pathlib import Path

#capacity = 10#input("Enter your capacity: ")
#latitude = 47.104496# input("Enter your latitude: ")
#longitude = 15.841415#input("Enter your logitude: ")

#OperatorID = '000'

#include auth.txt


countryID_AT = 'AT'
countryID_GER = 'DE'

keyfile=open('ladestellen-apikey.txt')
API_KEY = keyfile.readline().rstrip()
#url = 'https://api.e-control.at/charge/1.0/search?capacity={cap}&latitude={lat}&longitude={long}'.format(cap=capacity,lat=latitude,long=longitude)
url_AT = 'https://api.e-control.at/charge/1.0/countries/{cAT}/operators'.format(cAT=countryID_AT)
url_GER = 'https://api.e-control.at/charge/1.0/countries/{cGER}/operators'.format(cGER=countryID_GER)



print(url_AT)
headers = {'Accept': 'application/json',}
auth = HTTPBasicAuth('stefanpirker', 'xWQ24m2z3AqKP4HcarZz',)

### AT Url
data_set_AT = requests.get(url_AT, headers=headers, auth=auth).json()
df_Operator_AT = json_normalize(data_set_AT)
header_info_AT = (df_Operator_AT.columns)

get_operatorId_AT = list(df_Operator_AT[header_info_AT[0]])

#print(df_Operator_AT)
print(get_operatorId_AT)

df_Operator_AUT_CSV = df_Operator_AT.iloc[0:2,:]

print(df_Operator_AUT_CSV)
##### Save csv test
filepath = Path('Operator_AUT.csv')
df_Operator_AUT_CSV.to_csv(filepath, header=True, index=True)

boolx=True
counter = 0

if boolx == True:
    for i in get_operatorId_AT:
        #urlop = 'https://api.e-control.at/charge/1.0/countries/{cAT}/operators/{operator_id}'.format(cAT = countryID_AT,operator_id=str(i))
        urlstation = 'https://api.e-control.at/charge/1.0/countries/{cAT}/operators/{operator_id}/stations'.format(cAT = countryID_AT,operator_id=str(i))
        counter = counter + 1
        #print(urlop)
        data_op_LS = requests.get(urlstation, headers=headers, auth=auth).json()
        df_LS_AUT = json_normalize(data_op_LS)

        if (counter == 25):
            print(df_LS_AUT.shape)
            print(df_LS_AUT)
            ##### Save csv test
            filepath_LS = Path('Station_AUT.csv')
            df_LS_AUT.to_csv(filepath_LS, header=True, index=True)
print(counter)





print("---------------------------------------------------------------")
### GER Url
data_set_GER = requests.get(url_GER, headers=headers, auth=auth).json()
df_Operator_GER = json_normalize(data_set_GER)
header_info_GER = (df_Operator_GER.columns)
get_operatorId_GER = list(df_Operator_GER[header_info_GER[0]])
print(get_operatorId_GER)




#response = requests.get(url_AT, headers=headers, auth=auth)
#print(response.headers)
#print(response.content)
#data = json.loads(response.decode('utf-8'))

#data_set = data#[0]
#data_location = data_set['stationId']
#print(data_location)

#print(test)

##### Print part of Table first and then just read first and third column
    #print(test.iloc[:,0:4])
    #single_op=test.iloc[0,:].to_frame()

    #header_info_list = header_info[[0,2,4,5,9]]

#### Now apply just the two columns from above over the whole table to get just the 2 needed columns for the sql
#header_info_list = header_info[0,3]
    #table_sub = test[header_info_list]
##### Save csv test
    #filepath = Path('out.csv')
    #table_sub.to_csv(filepath)
#print(table_sub)

#print(header_info)
#print(header_info[0:3])
#print(header_info[11])

#### Now make the API url just for the operatorID




#print(get_operatorId)
print("---------------------------------------------------------------")
#print(single_op)
