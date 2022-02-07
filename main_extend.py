import os
from tkinter.tix import Tree
#os.system('Object.py')

#from curses import meta
from importlib.metadata import metadata
import json
from multiprocessing.spawn import import_main_path
from operator import and_
import pathlib
import re
from turtle import clear
from xml.etree.ElementInclude import include
import mysqlx
from requests.auth import HTTPBasicAuth
import requests
import pandas as pd
from pandas import json_normalize
from pathlib import Path

from dataclasses import dataclass
import mysql.connector


#import pymysql
#import sqlconnection

import sqlalchemy
from sqlalchemy import create_engine, select, MetaData, Table, and_

from Object import Operator, Station, Street

countryID_AT = 'AT'
#countryID_GER = 'DE'
bezeichner_op="Operater_id"

keyfile=open('ladestellen-apikey.txt')
API_KEY = keyfile.readline().rstrip()
#url = 'https://api.e-control.at/charge/1.0/search?capacity={cap}&latitude={lat}&longitude={long}'.format(cap=capacity,lat=latitude,long=longitude)
url_AT = 'https://api.e-control.at/charge/1.0/countries/{cAT}/operators'.format(cAT=countryID_AT)
#url_GER = 'https://api.e-control.at/charge/1.0/countries/{cGER}/operators'.format(cGER=countryID_GER)

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

print(country,PLZ,Ort)

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
boolx=True
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
print("----        TESTING -----------------------")

#print(df_table_station_Adress)
#print(df_table_adress)

##### Now drop duplicates
df_final_adresses = df_table_adress.drop_duplicates()
df_final_adresses.reset_index(inplace = True, drop=True)
#print(df_final_adresses)

###### Now we have to insert all IDs from the table_PLZ to match the ID in Adress
df_prepare_PLZ_City = df_final_adresses[[country,PLZ,Ort]]
df_final_PLZ_City = df_prepare_PLZ_City.drop_duplicates()
df_final_PLZ_City.reset_index(inplace=True,drop=True)
print(df_final_PLZ_City.iloc[980:1000,:])

#filepath_Adresse = Path('output/Adresse_AUT.csv')
#df_final_adresses.to_csv(filepath_Adresse, header=True, index=False)
#filepath_PLZ = Path('output/PLZ_AUT.csv')
#df_final_PLZ_City.to_csv(filepath_PLZ, header=True, index=False)


sql_up = False


if sql_up == True:
    mydb = mysql.connector.connect(
        host="dev.muenzer.at",
        user="ladestellen",
        password ="ybV1NfB0sCrzWS22hzOiMZ7YwkmtIwMT",
        database="ladestellen"
    )

    mycursor = mydb.cursor()
    #sql = "INSERT INTO tbl_plz (city, postCode, country) VALUES (%s, %s, %s)"
    #sql = "INSERT INTO tbl_plz (city, postCode, country) VALUES (%s, %s, %s) WHERE NOT EXISTS (Select * from tbl_plz WHERE city=%s AND postCode=%s) LIMIT 1"
    #sql = "INSERT IGNORE INTO tbl_plz (city, postCode, country) VALUES (%s, %s, %s)"
    #sql = "INSERT IGNORE INTO tbl_plz SET (`PLZ = %s'), transcript_chrom_start` = 12345, transcript_chrom_end` = 12678"
    sql = "SELECT * FROM tbl_plz WHERE city=%s and postCode=%s"
    

    for i in range(df_final_PLZ_City.shape[0]):
        data_row = df_final_PLZ_City.iloc[i,:]


        val = (data_row[2], data_row[1])
        mycursor.execute(sql,val)
        myresult = mycursor.fetchall()
        print(myresult)
        #myresult_list = list(myresult)
        mydb.commit()

        print(i)
        print(len(myresult))
        print(myresult)
        print(data_row)
        print('------------------------------------')
        if(len(list(myresult)) <= 0 and data_row[2] is not None and data_row[1] is not None):


            mydb1 = mysql.connector.connect(
                host="dev.muenzer.at",
                user="ladestellen",
                password ="ybV1NfB0sCrzWS22hzOiMZ7YwkmtIwMT",
                database="ladestellen"
            )

            mycursor_1 = mydb1.cursor()
            sql_1 = "INSERT INTO tbl_plz (city, postCode, country) VALUES (%s, %s, %s)"
            val_1 = (data_row[2], data_row[1], data_row[0])
            mycursor_1.execute(sql_1,val_1)
            mydb1.commit()
            

    #print(mycursor.rowcount, "record inserted.")


########################################################################################
"""mydb = mysql.connector.connect(
    host="dev.muenzer.at",
    user="ladestellen",
    password ="ybV1NfB0sCrzWS22hzOiMZ7YwkmtIwMT",
    database="ladestellen"
)
mycursor = mydb.cursor()
sql = "SELECT * FROM tbl_plz where city=%s and postCode=%s"
val = ('Null','Null')
mycursor.execute(sql,val)
myresult = mycursor.fetchall()
mydb.commit()
print(len(myresult))"""
########################################################################################

mydb = mysql.connector.connect(
        host="dev.muenzer.at",
        user="ladestellen",
        password ="ybV1NfB0sCrzWS22hzOiMZ7YwkmtIwMT",
        database="ladestellen"
    )

mycursor = mydb.cursor()
sql = "SELECT * FROM tbl_plz"
mycursor.execute(sql)
myresult = mycursor.fetchall()
#print(type(myresult))

mydb.commit()


df_SelectPLZSQL = pd.DataFrame (myresult, columns = ['ID','city','postCode','country'])
#print(df_SelectPLZSQL)

booltest = False

if booltest == True:
        
    ########  Select PLZ for df_Street from MySQL

    # ACHTUNG eventuell +1 bei length

    for i in range(df_final_adresses.shape[0]):
        df_Street_Single = df_final_adresses.iloc[i,:]
        obj_Street = Street(country=df_Street_Single["country"], postCode=df_Street_Single["postCode"],
        location=df_Street_Single["location"], street=df_Street_Single["street"])
        df_subset = df_SelectPLZSQL[df_SelectPLZSQL["country"] == obj_Street.country and df_SelectPLZSQL["postCode"] == obj_Street.postCode and df_SelectPLZSQL["location"] == obj_Street.location]
        if(df_subset.empty == False):
            get_PLZID = df_subset["ID"]
            obj_Street.PLZID = get_PLZID
        
        # SQL Upload function
        obj_Street.UpdateSQLStreet()
        #Deconstruct
        del obj_Street
            

    ##### Now for Operator

    mycursor = mydb.cursor()
    sql_street = "SELECT * FROM tbl_street"
    df_SelectStreetSQL = mycursor.execute(sql_street)

    mydb.commit()

    for i in range(df_Operator_AT.shape[0]):
        df_Operator_Single = df_Operator_AT.iloc[i,:]
        obj_Operator = Operator(operatorId = df_Operator_Single["operatorId"] ,type= df_Operator_Single["type"] ,
        organization= df_Operator_Single["organization"] ,
        commercialRegisterNumber= df_Operator_Single["commercialRegisterNumber"] ,sex= df_Operator_Single["sex"] ,
        titlePrefix= df_Operator_Single["titlePrefix"] ,firstName= df_Operator_Single["firstName"] ,lastName= df_Operator_Single["lastName"] ,
        titleSuffix= df_Operator_Single["titleSuffix"] ,street= df_Operator_Single["street"] , website= df_Operator_Single["website"] ,
        deviatingOperatorIds= df_Operator_Single["deviatingOperatorIds"] ,status= df_Operator_Single["status"] )

        df_subset = df_SelectStreetSQL[df_SelectStreetSQL["street"] == obj_Operator.street]  #Abfrage zugriff auf PLZ, Loc von anderer Tabelle
        if(df_subset.empty == False):
            get_StreetID = df_subset["ID"]
            obj_Operator.StreetIDr = get_StreetID

        # SQL Upload function
        obj_Operator.UpdateSQLStreet()
        #Deconstruct
        del obj_Operator

    ##### Now for Station  #################################

    mycursor = mydb.cursor()
    sql_street = "SELECT * FROM tbl_street"
    df_SelectStreetSQL = mycursor.execute(sql_street)

    mydb.commit()

    #### Abfrage nach Operator_DataFrame
    mycursor_Op = mydb.cursor()
    sql_operator = "SELECT * FROM tbl_operator"
    df_SelectOperatorSQL = mycursor_Op.execute(sql_operator)

    mydb.commit()

    #### Now for every operator, the list needs a new SQL

    waste_op_id_2=0

    for i in get_operatorId_AT:

            urlstation = 'https://api.e-control.at/charge/1.0/countries/{cAT}/operators/{operator_id}/stations'.format(cAT = countryID_AT,operator_id=str(i))

            #print(urlop)
            data_op_LS = requests.get(urlstation, headers=headers, auth=auth).json()
            df_LS_AUT = json_normalize(data_op_LS)

            if(df_LS_AUT.empty == True):
                waste_op_id_2.append(i)
            else:

                for j in range(df_LS_AUT.shape[0]):

                    df_Station_single = df_LS_AUT.iloc[j,:]

                    obj_Station = Station(stationId = df_Station_single["stationId"] ,stationStatus= df_Station_single["stationStatus"] ,
                    label= df_Station_single["label"] ,
                    description= df_Station_single["description"] ,postCode= df_Station_single["postCode"] ,
                    city= df_Station_single["city"] ,street= df_Station_single["street"] ,latitude= df_Station_single["latitude"] ,
                    longitude= df_Station_single["longitude"] ,contactName= df_Station_single["contactName"] , telephone= df_Station_single["telephone"] ,
                    email= df_Station_single["email"] ,website= df_Station_single["website"],
                    directions= df_Station_single["directions"] ,greenEnergy= df_Station_single["greenEnergy"],
                    freeParking= df_Station_single["freeParking"] ,priceUrl= df_Station_single["priceUrl"],
                    public= df_Station_single["public"] ,openingHours= df_Station_single["openingHours"],
                    openingHoursdetails= df_Station_single["openingHoursdetails"] ,Operater_id= i)


                    df_subset = df_SelectStreetSQL[df_SelectStreetSQL["street"] == obj_Station.street]  #Abfrage zugriff auf PLZ, Loc von anderer Tabelle
                    if(df_subset.empty == False):
                        get_StreetID = df_subset["ID"]
                        obj_Station.StreetID = get_StreetID

                    df_subset_op = df_SelectOperatorSQL[df_SelectOperatorSQL["operatorId"] == obj_Station.Operater_id]  #Abfrage zugriff auf PLZ, Loc von anderer Tabelle
                    if(df_subset_op.empty == False):
                        get_OperatorID = df_subset_op["ID"]
                        obj_Station.Operator_ID = get_OperatorID

                    # SQL Upload function
                    obj_Station.UpdateSQLStreet()
                    #Deconstruct
                    del obj_Station
