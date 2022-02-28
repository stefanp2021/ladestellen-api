import os
import numpy as np
import pandas as pd
import sys
import tqdm
import sqlalchemy
import pymysql
import hashlib
import copy
import requests
import json

#https://stackoverflow.com/questions/40083753/sqlalchemy-creating-view-with-orm
# installation: pip install sqlalchemy-utils
import sqlalchemy_utils

from tqdm import tqdm 
from importlib.metadata import metadata
from pandas import cut, json_normalize
from pathlib import Path
from dataclasses import dataclass
from pymysql import NULL
from itertools import count
from venv import create

from sqlalchemy.orm import declarative_base, sessionmaker, relationship, backref
from sqlalchemy import VARCHAR, Column,String,DateTime,Integer,create_engine, ForeignKey, PrimaryKeyConstraint,Sequence,Float,Boolean, UniqueConstraint, MetaData,select, func
from datetime import datetime
from sqlalchemy.sql import *
from requests.auth import HTTPBasicAuth
from sqlalchemy_utils import create_materialized_view, create_view,refresh_materialized_view, database_exists, view
from requests.auth import HTTPBasicAuth
from pandas import json_normalize

from SQLAlch_Tables import engine, Session, PLZ,Country,Address,Operators,Stations, OrgType
from function_own import func_GetRidofNone, func_iterateLists, func_not_so_magic_search


local_session = Session(bind=engine)


#countryID = ["AT","DE"]
bezeichner_op="Operater_id"
bezeichner_name="Operator_Name"

#Get countries from DataBase
myresult_country =  local_session.query(Country).all()
for i in range(len(myresult_country)):
    api_country_tuple = myresult_country[i].Country
    print(api_country_tuple)


countryID = ["AT","DE"]

keyfile=open('ladestellen-apikey.txt')
API_KEY = keyfile.readline().rstrip()
url = 'https://api.e-control.at/charge/1.0/countries/{cAT}/operators'.format(cAT=countryID[0])

headers = {'Accept': 'application/json',}
auth = HTTPBasicAuth('stefanpirker', 'xWQ24m2z3AqKP4HcarZz',)


##################################################################################################################################################################
#     HERE we are using the query's to get Information for the API 
##################################################################################################################################################################

#region GET_API_Information

data_set = requests.get(url, headers=headers, auth=auth).json()
df_Operator = json_normalize(data_set)
header_info = (df_Operator.columns)
# Get all ID's from the Operators

columns_we_want = ['operatorId', 'type', 'organization', 'commercialRegisterNumber', 'sex',
       'titlePrefix', 'firstName', 'lastName', 'titleSuffix', 'country',
       'postCode', 'city', 'street', 'website', 'deviatingOperatorIds',
       'status']

try:
    deviating_Operator_Id_unable = list(df_Operator[columns_we_want[-2]])
    deviating_Operator_Id = func_iterateLists(deviating_Operator_Id_unable)
    df_Operator_deviating = func_not_so_magic_search(deviating_Operator_Id,columns_we_want[-2],df_Operator)
    df_Operator_deviating_Finishing = copy.copy(df_Operator_deviating)
    df_Operator_deviating_Finishing[columns_we_want[0]] = df_Operator_deviating_Finishing[columns_we_want[-2]]
    df_Operator_deviating_Finishing[columns_we_want[0]] = df_Operator_deviating_Finishing[columns_we_want[0]].str.get(0)

except:
    print("{} isn't possible to get, maybe columnname changed or there is just no deviating Operator ID".format(columns_we_want[-2]))


# To check if the API have changed, we make sure there is a column "operatorId"
if any(columns_we_want[0] in string for string in header_info):

    if(df_Operator_deviating_Finishing.empty == False):
        try:  
            
            deviating_OP_number = df_Operator_deviating_Finishing[columns_we_want[0]].values
            already_existing = deviating_OP_number[0] in list(df_Operator[columns_we_want[0]])

            if(already_existing == False):
                frames_OP = [df_Operator, df_Operator_deviating_Finishing]
                df_Operator = pd.concat(frames_OP)

        except:
            print("Can't concat the Operator DataFrame with the Special Operator Frame")

#endregion

    #Here we get rid of all Non-approved Operators and the deviatingOperatorIds-columns because we added them already
    df_Operator_approved = copy.copy(df_Operator.loc[df_Operator[columns_we_want[-1]] != "UNAPPROVED"])
    df_Operator_approved.drop(columns=[columns_we_want[-2]], inplace=True)
    df_Operator_approved.reset_index(inplace=True,drop=True)
    #Now we get all ID's from the Operators
    get_operatorId = list(df_Operator_approved[columns_we_want[0]])
else:
    print("Step 1: API description of the columns changed, {} is not in the API anymore, please check ".format(columns_we_want[0]))
    replay = input("Press AnyKey")
    if replay == True:
        sys.exit()


try:
    # Just to make sure we get the right columns for the PLZ table
    if any(columns_we_want[0] in string for string in header_info):
        op_ID = columns_we_want[0]
    if any(columns_we_want[2] in string for string in header_info):
        op_Name = columns_we_want[2]
    if any(columns_we_want[9] in string for string in header_info):
        country = columns_we_want[9]
    if any(columns_we_want[10] in string for string in header_info):
        PLZ = columns_we_want[10]
    if any(columns_we_want[11] in string for string in header_info):
        Ort = columns_we_want[11]
    if any(columns_we_want[12] in string for string in header_info):
        Straße = columns_we_want[12]

    #Create and Empty DataFrame for later use
    df_table_adress_Operator = df_Operator_approved[[op_ID, op_Name, country, PLZ, Ort, Straße]]
    df_table_adress_Station = pd.DataFrame(columns=df_table_adress_Operator.columns)

except:
    print("STEP 2: API column names changed")
    replay = input("Press AnyKey")
    if replay == True:
        sys.exit()


#print(df_Operator_approved)
print(" Finished Reading API-Operator: Now we go through each one to start filling the sql")




