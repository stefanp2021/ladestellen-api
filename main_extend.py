

"""  Das hier ist nicht nötig  """

"""
from lib2to3.pytree import convert
from logging import PlaceHolder
from importlib.metadata import metadata
import json
import string
from traceback import print_tb
from pymysql import NULL
from requests.auth import HTTPBasicAuth
import requests
import pandas as pd
from pandas import json_normalize
from pathlib import Path
from dataclasses import dataclass
import mysql.connector
import copy

from sqlalchemy import null

#import Object
from Object import Operator, Station, OCountry


##############################################  BLOCK Basic Values  ########################################################

countryID_AT = 'AT'
bezeichner_op="Operater_id"
bezeichner_name="Operator_Name"

keyfile=open('ladestellen-apikey.txt')
API_KEY = keyfile.readline().rstrip()
#url = 'https://api.e-control.at/charge/1.0/search?capacity={cap}&latitude={lat}&longitude={long}'.format(cap=capacity,lat=latitude,long=longitude)
url_AT = 'https://api.e-control.at/charge/1.0/countries/{cAT}/operators'.format(cAT=countryID_AT)
#url_GER = 'https://api.e-control.at/charge/1.0/countries/{cGER}/operators'.format(cGER=countryID_GER)

headers = {'Accept': 'application/json',}
auth = HTTPBasicAuth('stefanpirker', 'xWQ24m2z3AqKP4HcarZz',)


mydb = mysql.connector.connect(
        host="dev.muenzer.at",
        user="ladestellen",
        password ="ybV1NfB0sCrzWS22hzOiMZ7YwkmtIwMT",
        database="ladestellen"
    )


##################################################################################################################################################################
# START with the Operator Filling in the SQL-DataBase 
##################################################################################################################################################################

#region FILL other SQL-DataBases

df_API_Operator_AUT = requests.get(url_AT, headers=headers, auth=auth).json() #only AUT
df_API_Operator_AUT_DataFrame = json_normalize(df_API_Operator_AUT)

header_info_df_Operator = (df_API_Operator_AUT_DataFrame.columns)
get_operatorId_AT = list(df_API_Operator_AUT_DataFrame[header_info_df_Operator[0]])

df_shape_API_Operator_AUT = df_API_Operator_AUT_DataFrame.shape[0]

def func_dump(obj):
    for attr in dir(obj):
        print("obj.%s = %r" % (attr, getattr(obj, attr)))



for i in range(df_shape_API_Operator_AUT):

    df_API_Operator_Single = df_API_Operator_AUT_DataFrame.iloc[i,:]

    #print('-----')

    #print(df_API_Operator_Single)
    OperatorId = df_API_Operator_Single[header_info_df_Operator[0]]
    Type = df_API_Operator_Single[header_info_df_Operator[1]]
    Organization = df_API_Operator_Single[header_info_df_Operator[2]]
    CommercialRegisterNumber = df_API_Operator_Single[header_info_df_Operator[3]]
    Sex = df_API_Operator_Single[header_info_df_Operator[4]]
    TitlePrefix = df_API_Operator_Single[header_info_df_Operator[5]]
    FirstName = df_API_Operator_Single[header_info_df_Operator[6]]
    LastName = df_API_Operator_Single[header_info_df_Operator[7]]
    TitleSuffix = df_API_Operator_Single[header_info_df_Operator[8]]
    Country = df_API_Operator_Single[header_info_df_Operator[9]]
    PostCode = df_API_Operator_Single[header_info_df_Operator[10]]
    City = df_API_Operator_Single[header_info_df_Operator[11]]
    StreetData = df_API_Operator_Single[header_info_df_Operator[12]]
    Website= df_API_Operator_Single[header_info_df_Operator[13]]
    DeviatingOperatorIds= df_API_Operator_Single[header_info_df_Operator[14]]
    Status= df_API_Operator_Single[header_info_df_Operator[15]]

    obj_Operator = Operator(operatorId=OperatorId,type=Type,organization=Organization,commercialRegisterNumber=CommercialRegisterNumber,
    sex=Sex,titlePrefix=TitlePrefix,firstName=FirstName,lastName=LastName,titleSuffix=TitleSuffix,country=Country,postCode=PostCode,city=City,street=StreetData,website=Website,
    deviatingOperatorIds=DeviatingOperatorIds,status=Status)

    ###Insert into tbl_Operator in SQL if not exist else update
    Street_Op_Id = obj_Operator.ReturnStreetID(mydb)
    #print(Street_Op_Id)

    obj_Operator.StreetID = int(Street_Op_Id[0][0])
    #print(obj_Operator.StreetID)
    #func_dump(obj_Operator)
    #print('-----------')

    count_obj_Operator = obj_Operator.AskCountOperator(mydb)

    if(count_obj_Operator[0][0] <1):
        obj_Operator.InsertSQLOperator(mydb)
    else:
        obj_Operator.UpdateSQLOperator(mydb)

    #print("----------------------------------------------------------")
    del obj_Operator


mydb.close()



##################################################################################################################################################################
 # START with the Station Filling in the SQL-DataBase
##################################################################################################################################################################


print("Finished with Operators, now start with Stations")

mydb = mysql.connector.connect(
        host="dev.muenzer.at",
        user="ladestellen",
        password ="ybV1NfB0sCrzWS22hzOiMZ7YwkmtIwMT",
        database="ladestellen"
    )


for i in range(len(get_operatorId_AT)):

    operator_short =  get_operatorId_AT[i]
    urlstation = 'https://api.e-control.at/charge/1.0/countries/{cAT}/operators/{operator_id}/stations'.format(cAT = countryID_AT,operator_id=str(operator_short))

    data_op_LS = requests.get(urlstation, headers=headers, auth=auth).json()
    df_API_Station_AUT_DataFrame = json_normalize(data_op_LS)
    header_Station_AUT_DataFrame = list(df_API_Station_AUT_DataFrame.columns)
    #print(header_Station_AUT_DataFrame)
    for j in range(len(df_API_Station_AUT_DataFrame)):
            
        df_API_Station_Single = df_API_Station_AUT_DataFrame.iloc[j,:]

        #print(df_API_Station_Single)

        Stationid = df_API_Station_Single[header_Station_AUT_DataFrame[0]]
        StationStatus = df_API_Station_Single[header_Station_AUT_DataFrame[1]]
        Label = df_API_Station_Single[header_Station_AUT_DataFrame[2]]
        Description = df_API_Station_Single[header_Station_AUT_DataFrame[3]]
        PostCode = df_API_Station_Single[header_Station_AUT_DataFrame[4]]
        City = df_API_Station_Single[header_Station_AUT_DataFrame[5]]
        Streetdata = df_API_Station_Single[header_Station_AUT_DataFrame[6]]
        Latitude = df_API_Station_Single[header_Station_AUT_DataFrame[7]]
        Longitude = df_API_Station_Single[header_Station_AUT_DataFrame[8]]
        ContactName = df_API_Station_Single[header_Station_AUT_DataFrame[9]]
        Telephone = df_API_Station_Single[header_Station_AUT_DataFrame[10]]
        Email = df_API_Station_Single[header_Station_AUT_DataFrame[11]]
        Website = df_API_Station_Single[header_Station_AUT_DataFrame[12]]
        Directions= df_API_Station_Single[header_Station_AUT_DataFrame[13]]
        GreenEnergy= df_API_Station_Single[header_Station_AUT_DataFrame[14]]
        FreeParking= df_API_Station_Single[header_Station_AUT_DataFrame[15]]
        PriceUrl= df_API_Station_Single[header_Station_AUT_DataFrame[16]]
        Public= df_API_Station_Single[header_Station_AUT_DataFrame[17]]
        OpeningHours_text= df_API_Station_Single[header_Station_AUT_DataFrame[18]]
        OpeningHours_details= df_API_Station_Single[header_Station_AUT_DataFrame[19]]

        obj_Station = Station(stationId=Stationid, stationStatus=StationStatus,label=Label,description=Description,postCode=PostCode,city=City,
        street=Streetdata,latitude=Latitude,longitude=Longitude,contactName=ContactName,telephone=Telephone,email=Email,website=Website,directions=Directions,
        greenEnergy=GreenEnergy,freeParking=FreeParking,priceUrl=PriceUrl,public=Public,openingHours=OpeningHours_text,openingHoursdetails=OpeningHours_details,operater_id=str(operator_short))

        Street_Op_Id = obj_Station.ReturnStreetID(mydb)
        #print(Street_Op_Id)
        obj_Station.StreetID = Street_Op_Id[0][0]

        Operator_Station_Id = obj_Station.ReturnOperatorID(mydb)
        obj_Station.Operator_ID = Operator_Station_Id[0][0]


        count_obj_Stations = obj_Station.AskCountOperator(mydb)

        #func_dump(obj_Station)

        #func_dump(obj_Station)
        #print('---------------------------------------')
        if(count_obj_Stations[0][0] < 1):
            #func_dump(obj_Station)
            obj_Station.InsertSQLStation(mydb)
        else:
            obj_Station.UpdateSQLStation(mydb)


        del obj_Station

print("Finished with Stations")
#endregion
"""