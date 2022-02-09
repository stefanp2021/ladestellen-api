

from lib2to3.pytree import convert
from logging import PlaceHolder
from importlib.metadata import metadata
import json
import string
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
from Object import Operator, Station, Street


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
""" START with the Operator Filling in the SQL-DataBase """
##################################################################################################################################################################

#region FILL other SQL-DataBases

df_API_Operator_AUT = requests.get(url_AT, headers=headers, auth=auth).json() #only AUT
df_API_Operator_AUT_DataFrame = json_normalize(df_API_Operator_AUT)

header_info_df_Operator = (df_API_Operator_AUT_DataFrame.columns)

df_shape_API_Operator_AUT_DataFrame = df_API_Operator_AUT_DataFrame.shape[0]

def func_dump(obj):
    for attr in dir(obj):
        print("obj.%s = %r" % (attr, getattr(obj, attr)))

for i in range(df_shape_API_Operator_AUT_DataFrame):

    print(i)
    print('-----')

    df_API_Operator_Single = df_API_Operator_AUT_DataFrame.iloc[i,:]

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
    obj_Operator.StreetID = Street_Op_Id[0][0]
    #print(obj_Operator.StreetID)

    #func_dump(obj_Operator)
    #print('-----------')

    count_obj_Operator = obj_Operator.AskCountOperator(mydb)
    if(count_obj_Operator[0][0] <1):
        obj_Operator.InsertSQLOperator(mydb)
    else:
        obj_Operator.UpdateSQLOperator(mydb)


    del obj_Operator

#endregion

mydb.close()