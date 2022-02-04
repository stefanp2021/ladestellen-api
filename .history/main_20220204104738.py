import json
import pathlib
import re
from xml.etree.ElementInclude import include
from requests.auth import HTTPBasicAuth
import requests
import pandas as pd
from pandas import json_normalize
from pathlib import Path
#import mysql.connector
import pymysql

#capacity = 10#input("Enter your capacity: ")
#latitude = 47.104496# input("Enter your latitude: ")
#longitude = 15.841415#input("Enter your logitude: ")

#OperatorID = '000'

#include auth.txt


countryID_AT = 'AT'
countryID_GER = 'DE'
bezeichner_op="Operater_id"

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


###### Create new Table for adresses with the same name as from API
print("#####---------------#################################--------------------------")
location = header_info_AT[9:13]
print(location)
print(header_info_AT)
country = location[0]
PLZ = location[1]
Ort = location[2]
Straße = location[3]
#bezeichner_adress = [PLZ,Ort,Straße]
#df_table_adress = pd.DataFrame(columns=bezeichner_adress)
df_table_adress = df_Operator_AT[[country,PLZ,Ort,Straße]]

#print(df_Operator_AT)
#print(get_operatorId_AT)
#print(df_table_adress)
df_Operator_AUT_CSV = df_Operator_AT.iloc[0:2,:]

#print(df_Operator_AUT_CSV)


##### Save csv test
    #filepath = Path('output/Operator_AUT.csv')
    #df_Operator_AUT_CSV.to_csv(filepath, header=True, index=True)


print("#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-##-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-##---------------------------")

### To be able to turn it off
boolx=False
counter = 0
waste_op_id = []

#### Hier werden jetzt die Stationen ausgelesen, pro Land und Operator
if boolx == True:
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

        #print(df_LS_AUT)

        df_shape_column_AUT = df_LS_AUT.shape[0]
        list_op_Station = [i for x in range(int(df_shape_column_AUT))]

        #print(df_LS_AUT)
        df_LS_AUT[bezeichner_op] = list_op_Station
        #print(df_LS_AUT)
        df_column_names = list(df_LS_AUT.columns)
        #print(df_column_names)

    
        ##### Now fill the whole df_table_adress with the adresses from the organizer first
        ##### But some Operators are empty
        if(df_LS_AUT.empty == True):
            waste_op_id.append(i)
        else:
            df_table_station_Adress = df_LS_AUT[[PLZ,Ort,Straße]]

            frames = [df_table_adress,df_table_station_Adress]
            df_table_adress = pd.concat(frames)

        #print(df_shape_column_AUT)
        #if (counter == 25):
        #    print(df_LS_AUT.shape)
        #    print(df_LS_AUT)
        #    ##### Save csv test
            #  filepath_LS = Path('output/Station_AUT.csv')
            #df_LS_AUT.to_csv(filepath_LS, header=True, index=True)
    #print(counter)


##### Now drop duplicates
df_final_adresses = df_table_adress.drop_duplicates()
df_final_adresses.reset_index(inplace = True, drop=True)
print(df_final_adresses)

###### Now we have to insert all IDs from the table_PLZ to match the ID in Adress
df_prepare_PLZ_City = df_final_adresses[[country,PLZ,Ort]]
df_final_PLZ_City = df_prepare_PLZ_City.drop_duplicates()
df_final_PLZ_City.reset_index(inplace=True,drop=True)
print(df_final_PLZ_City)


#filepath_Adresse = Path('output/Adresse_AUT.csv')
#df_final_adresses.to_csv(filepath_Adresse, header=True, index=False)
#filepath_PLZ = Path('output/PLZ_AUT.csv')
#df_final_PLZ_City.to_csv(filepath_PLZ, header=True, index=False)

print("---------------------------------------------------------------")
print(waste_op_id)
### GER Url
#data_set_GER = requests.get(url_GER, headers=headers, auth=auth).json()
#df_Operator_GER = json_normalize(data_set_GER)
#header_info_GER = (df_Operator_GER.columns)
#get_operatorId_GER = list(df_Operator_GER[header_info_GER[0]])
#print(get_operatorId_GER)


#### Provider


#urlprovider ='https://api.e-control.at/charge/1.0/countries/{cAT}/providers'.format(cAT=countryID_AT)
#print(urlprovider)
#data_op_Pro = requests.get(urlprovider, headers=headers, auth=auth).json()
#print(data_op_Pro)
#df_Pro_AUT = json_normalize(data_op_Pro)
#print(df_Pro_AUT)
print("--*-*-***-**-*-+-+--+-++-*-+++++-*****---**+---**-------------------")


#print(df_Operator_AUT_CSV.iloc[:,9:17])


''' mydb = mysql.connector.connect(
  host="192.168.10.21:3306", #dev.muenzer.at #192.168.10.21, board 3306
  #host="192.168.10.21",
  user="root",
  password="db-Root2021!"
) '''


conn = pymysql.connect(db='ladestellen',user='ladestellen',passwd='ybV1NfB0sCrzWS22hzOiMZ7YwkmtIwMT',host='192.168.10.21')
print(conn)
