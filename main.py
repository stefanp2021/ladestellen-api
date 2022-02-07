import os
#os.system('Object.py')

#from curses import meta
from importlib.metadata import metadata
import json
from requests.auth import HTTPBasicAuth
import requests
import pandas as pd
from pandas import json_normalize
from pathlib import Path

from dataclasses import dataclass
import mysql.connector

from Object import Operator, Station, Street


countryID_AT = 'AT'
bezeichner_op="Operater_id"

keyfile=open('ladestellen-apikey.txt')
API_KEY = keyfile.readline().rstrip()
#url = 'https://api.e-control.at/charge/1.0/search?capacity={cap}&latitude={lat}&longitude={long}'.format(cap=capacity,lat=latitude,long=longitude)
url_AT = 'https://api.e-control.at/charge/1.0/countries/{cAT}/operators'.format(cAT=countryID_AT)
#url_GER = 'https://api.e-control.at/charge/1.0/countries/{cGER}/operators'.format(cGER=countryID_GER)

#print(url_AT)
headers = {'Accept': 'application/json',}
auth = HTTPBasicAuth('stefanpirker', 'xWQ24m2z3AqKP4HcarZz',)


### AT Url
data_set_AT = requests.get(url_AT, headers=headers, auth=auth).json()
df_Operator_AT = json_normalize(data_set_AT)
header_info_AT = (df_Operator_AT.columns)

get_operatorId_AT = list(df_Operator_AT[header_info_AT[0]])


###### Create new Table for adresses with the same name as from API
print("#####---------------#################################--------------------------")
location = header_info_AT[9:13]
print(location)
#print(header_info_AT)
country = location[0]
PLZ = location[1]
Ort = location[2]
Straße = location[3]
df_table_adress = df_Operator_AT[[country,PLZ,Ort,Straße]]

print("#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-##-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-##---------------------------")

### To be able to turn it off
waste_op_id = []

#### Hier werden jetzt die Stationen ausgelesen, pro Land und Operator

for i in get_operatorId_AT:
    #counter = counter + 1
    #if counter == 20:
        #print(i)
        #print(counter)
        ### Just work with 1 to see if it works in general
        #if counter == 20:
    urlstation = 'https://api.e-control.at/charge/1.0/countries/{cAT}/operators/{operator_id}/stations'.format(cAT = countryID_AT,operator_id=str(i))

    #print(urlop)
    data_op_LS = requests.get(urlstation, headers=headers, auth=auth).json()
    df_LS_AUT = json_normalize(data_op_LS)

    #if counter == 20:
    #    filepath_Adresse = Path('output/Adresse_AUT_Komplett.csv')
    #    df_LS_AUT.to_csv(filepath_Adresse, header=True, index=False)

    #print(df_LS_AUT)

    df_shape_column_AUT = df_LS_AUT.shape[0]
    list_op_Station = [i for x in range(int(df_shape_column_AUT))]

    #print(df_LS_AUT)
    df_LS_AUT[bezeichner_op] = list_op_Station
    df_LS_AUT["country"] = "AT"
    #print(df_LS_AUT)
    df_column_names = list(df_LS_AUT.columns)
    #print(df_column_names)


    ##### Now fill the whole df_table_adress with the adresses from the organizer first
    ##### But some Operators are empty
    if(df_LS_AUT.empty == True):
        waste_op_id.append(i)
    else:
        df_table_station_Adress = df_LS_AUT[[country,PLZ,Ort,Straße]]

        frames = [df_table_adress,df_table_station_Adress]
        df_table_adress = pd.concat(frames)

###
