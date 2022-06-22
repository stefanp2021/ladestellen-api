##############################################  BLOCK Package  ########################################################


from cmath import isnan
from genericpath import exists
from lib2to3.pytree import convert
from logging import PlaceHolder
import os
from importlib.metadata import metadata
import json
from re import A
import string
from urllib.request import OpenerDirector
import numpy
from pymysql import NULL
from requests.auth import HTTPBasicAuth
import requests
import pandas as pd
from pandas import cut, json_normalize
from pathlib import Path
from dataclasses import dataclass
import mysql.connector
import copy
import sys
#import security.py
#from keyring import get_keyring
import tqdm
from tqdm import tqdm 
#from sqlalchemy import column, null
from sqlobject import Operator, Station, Street, OCountry, PLZ_Location, OType

import datetime
from datetime import datetime
from datetime import timedelta


##############################################  BLOCK Basic Values  ########################################################

def func_dump(obj):
    """To get all items from an Object"""
    for attr in dir(obj):
        print("obj.%s = %r" % (attr, getattr(obj, attr)))

def func_iterateLists(takelist):
    return_list = []
    """ iterate over lists in a list because some values are saved in that way"""
    for list in takelist:
        for number in list:
           # print(type(number))
            if number:
                return_list.append(number)
    return(return_list)


def func_fillObjectStation(StStationId, StStationStatus, StLabel, StDescription, StPostCode, StCity,StStreetdata,StLatitiude,StLongitude,StContactName,StTelephone,
StEmail,StWebsite,StDirections,StGreenEnergy, StFreeParking, StPriceUrl, StPublic,StDateTime,StOpeningHours_text,StOpeningHours_details,StoperatorId):
    #To fill the Station-Object
    obj_Station = Station(stationId=StStationId, stationStatus=StStationStatus,label=StLabel,description=StDescription,postCode=StPostCode,city=StCity,
    street=StStreetdata,latitude=StLatitiude,longitude=StLongitude,contactName=StContactName,telephone=StTelephone,email=StEmail,website=StWebsite,directions=StDirections,
    greenEnergy=StGreenEnergy,freeParking=StFreeParking,priceUrl=StPriceUrl,public=StPublic,DateTime=StDateTime,openingHours=StOpeningHours_text,openingHoursdetails=StOpeningHours_details,operater_id=StoperatorId)

    PLZ_Station_Id = obj_Station.ReturnPLZID(mydb)
    obj_Station.PLZID = PLZ_Station_Id[0][0]

    Street_Op_Id = obj_Station.ReturnStreetID(mydb)
    #print(Street_Op_Id)
    if(len(Street_Op_Id) > 1):
        #print("Here ## ##### ### ## ### ## ")
        #print(Street_Op_Id)
        #print(Street_Op_Id[0][0])
        obj_Station.StreetID = Street_Op_Id[0][0]
        #print('-#-#-#######-#-##-')
    else:
        #print("Normal")
        #print(Street_Op_Id[0][0])
        obj_Station.StreetID = Street_Op_Id[0][0]
        #print('-#-#-#-#-##-')

    Operator_Station_Id = obj_Station.ReturnOperatorID(mydb)
    obj_Station.Operator_ID = Operator_Station_Id[0][0]

    count_obj_Stations = obj_Station.AskCountStation(mydb)

    #func_dump(obj_Station)

    #func_dump(obj_Station)
    #print('---------------------------------------')
    if(count_obj_Stations[0][0] < 1):
        #func_dump(obj_Station)
        obj_Station.InsertSQLStation(mydb)
    else:
        obj_Station.UpdateSQLStation(mydb)


    del obj_Station



def not_so_magic_search(srchs, col, df):
    """ search whole DataFrame where the DeviatingOperator matches"""
    bools = pd.concat([df[col].apply(lambda x: srch in x) for srch in srchs],axis=1)
    return df[bools.sum(axis=1) > 0]



def func_fillObjectPLZ(Pcountry, Pplz, Port):
    """To fill the Location-Object"""
    obj_PLZ = PLZ_Location(country=Pcountry,
    postcode=Pplz,
    location=Port)
    obj_PLZ.CountryID = obj_PLZ.ReturnCountryID(mydb)[0][0]
    count_plz = obj_PLZ.AskCountPLZ(mydb)

    #print(count_plz)
    if(count_plz[0][0] < 1):
        obj_PLZ.InsertSQLPLZ(mydb)
    else:
        pass
    del obj_PLZ


def func_fillObjectStreet(Splz, Slocation, Sstreet):
    """To fill the Street-Object"""
    obj_Street = Street(postCode=Splz,
    location=Slocation,
    street=Sstreet)
    obj_Street.PLZID = obj_Street.ReturnPLZID(mydb)[0][0]
    count_street = obj_Street.AskCountStreet(mydb)

    #func_dump(obj_Street)
    #print(count_street)
    if(count_street[0][0] < 1):
        #print("Problem Insert")
        obj_Street.InsertSQLStreet(mydb)
    else:
        pass
    del obj_Street



# Connection to Database

mydb = mysql.connector.connect(
        host="192.168.10.21",
        user="ladestellen",
        password ="ybV1NfB0sCrzWS22hzOiMZ7YwkmtIwMT",
        database="ladestellen"
    )



######################################################################
countryID = ["AT","DE"]
bezeichner_op="Operater_id"
bezeichner_name="Operator_Name"


# Get all countries which are in the the MySQL-DB right now

mycursor = mydb.cursor()
sql_country = "SELECT CountryName FROM tbl_country"
mycursor.execute(sql_country)
myresult_country = mycursor.fetchall()
mydb.commit()
mycursor.close()

#print(myresult_country[0])
#print(len(myresult_country))
for i in range(len(myresult_country)):
    api_country_tuple = myresult_country[i]
    api_country = api_country_tuple[0]
    print(api_country)

with open(os.path.join(sys.path[0], "ladestellen-apikey.txt"), "r") as apikey:
    apidata=apikey.read
#API_KEY = keyfile.readline().rstrip()
url = 'https://api.e-control.at/charge/1.0/countries/{cAT}/operators'.format(cAT=countryID[0])


headers = {'Accept': 'application/json',}
pw='xWQ24m2z3AqKP4HcarZz'
#print(pw)
auth = HTTPBasicAuth('stefanpirker', pw,)

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

#region deviating Operator
################################# Reserviert falls mal ein deviating Operator wirksam wird
try:
    deviating_Operator_Id_unable = list(df_Operator[columns_we_want[-2]])
    deviating_Operator_Id = func_iterateLists(deviating_Operator_Id_unable)
    df_Operator_deviating = not_so_magic_search(deviating_Operator_Id,columns_we_want[-2],df_Operator)
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


actual_dateTime = datetime.now().isoformat(sep=' ', timespec='milliseconds')

##########################################################################################
#######                 NOW we can start with filling the SQL_table --> tbl_Plz, tbl_Street, tbl_country, tbl_Operator
#
for i in range(df_Operator_approved.shape[0]):
    get_Base_for_Objects = df_Operator_approved.loc[i]
    #print(i)
    ## Now get the Country
    obj_Country = OCountry(country=get_Base_for_Objects[country]) #country is from before where we said that country = header_value[x]
    count_country = obj_Country.AskCountCountry(mydb)
    if(count_country[0][0] < 1):
        obj_Country.InsertSQLCountry(mydb)
    else:
        #Es existiert bereits
        pass
    del obj_Country

    # Now get the PLZ
    func_fillObjectPLZ(Pcountry=get_Base_for_Objects[country],
    Pplz=get_Base_for_Objects[PLZ],
    Port=get_Base_for_Objects[Ort])

    #Now get the Street
    #print(get_Base_for_Objects[PLZ],get_Base_for_Objects[Ort],get_Base_for_Objects[Straße])
    #print('------')
    func_fillObjectStreet(Splz=get_Base_for_Objects[PLZ],
    Slocation=get_Base_for_Objects[Ort],
    Sstreet=get_Base_for_Objects[Straße])
    #print('-----------------------------------')
    ## Now get the OType
    obj_OType = OType(type=get_Base_for_Objects[columns_we_want[1]])
    count_OType = obj_OType.AskCountOType(mydb)
    if(count_OType[0][0] < 1):
        obj_OType.InsertSQLOType(mydb)
    else:
        #Es existiert bereits
        pass
    del obj_OType


    #Now get the Operator
    OperatorId = get_Base_for_Objects[op_ID]
    Type = get_Base_for_Objects[columns_we_want[1]]
    Organization = get_Base_for_Objects[columns_we_want[2]]
    CommercialRegisterNumber = get_Base_for_Objects[columns_we_want[3]]
    Sex = get_Base_for_Objects[columns_we_want[4]]
    TitlePrefix = get_Base_for_Objects[columns_we_want[5]]
    FirstName = get_Base_for_Objects[columns_we_want[6]]
    LastName = get_Base_for_Objects[columns_we_want[7]]
    TitleSuffix = get_Base_for_Objects[columns_we_want[8]]
    Country = get_Base_for_Objects[country]
    PostCode = get_Base_for_Objects[PLZ]
    City = get_Base_for_Objects[Ort]
    StreetData = get_Base_for_Objects[Straße]
    Website= get_Base_for_Objects[columns_we_want[13]]
    Status= get_Base_for_Objects[columns_we_want[15]]

    obj_Operator = Operator(operatorId=OperatorId,type=Type,organization=Organization,commercialRegisterNumber=CommercialRegisterNumber,
    sex=Sex,titlePrefix=TitlePrefix,firstName=FirstName,lastName=LastName,titleSuffix=TitleSuffix,country=Country,postCode=PostCode,city=City,
    street=StreetData,website=Website, status=Status)

    ###Insert into tbl_Operator in SQL if not exist else update
    obj_Operator.TypeID = obj_Operator.ReturnTypeID(mydb)[0][0]
    obj_Operator.PLZID = obj_Operator.ReturnPLZID(mydb)[0][0]
    obj_Operator.StreetID = obj_Operator.ReturnStreetID(mydb)[0][0]

    count_obj_Operator = obj_Operator.AskCountOperator(mydb)

    #print()

    if(count_obj_Operator[0][0] <1):
        obj_Operator.InsertSQLOperator(mydb)
    else:
        obj_Operator.UpdateSQLOperator(mydb)

    #print("----------------------------------------------------------")
    del obj_Operator


#print(df_Operator_approved)

print(" Finished Operator: The Operators are now in the SQL")


print('#################################################################################################################################')


#print(df_Operator_approved)






### NOW we can read the Stations

# But first we need a base-Construct which we can fill with the requested API now

waste_op_id = []

country_fill = countryID[0]

#get_operatorId_test = get_operatorId[:5]
#get_operatorId=get_operatorId_test
#print(get_operatorId)

print("Starting to go through all Operators and search/request API for all associated Stations")


for k in tqdm(range(len(get_operatorId))):
    get_OID = get_operatorId[k]
    #print(get_OID)

    urlstation = 'https://api.e-control.at/charge/1.0/countries/{cAT}/operators/{operator_id}/stations'.format(cAT = countryID[0],operator_id=str(get_OID)) ## Hier muss man DE noch berücksichtigen in der Schleife
    data_Station_request = requests.get(urlstation, headers=headers, auth=auth).json()
    df_Station_from_Operator = json_normalize(data_Station_request)

    header_Station_country_DataFrame = list(df_Station_from_Operator.columns)


    if(df_Station_from_Operator.empty == True):
        waste_op_id.append(k)
    else:
        #Now add the Operator-Id/name from before and the country because a station don't have that BASIC-INFOS!!!

        for m in range(df_Station_from_Operator.shape[0]):
            df_get_iter_item_dfStation = copy.copy(df_Station_from_Operator.loc[m])

            df_get_iter_item_dfStation[op_ID] = get_OID

            Stationid = df_get_iter_item_dfStation[header_Station_country_DataFrame[0]]
            StationStatus = df_get_iter_item_dfStation[header_Station_country_DataFrame[1]]
            Label = df_get_iter_item_dfStation[header_Station_country_DataFrame[2]]
            Description = df_get_iter_item_dfStation[header_Station_country_DataFrame[3]]
            Streetdata = df_get_iter_item_dfStation[Straße]
            Latitude = df_get_iter_item_dfStation[header_Station_country_DataFrame[7]]
            Longitude = df_get_iter_item_dfStation[header_Station_country_DataFrame[8]]
            ContactName = df_get_iter_item_dfStation[header_Station_country_DataFrame[9]]
            Telephone = df_get_iter_item_dfStation[header_Station_country_DataFrame[10]]
            Email = df_get_iter_item_dfStation[header_Station_country_DataFrame[11]]
            Website = df_get_iter_item_dfStation[header_Station_country_DataFrame[12]]
            Directions= df_get_iter_item_dfStation[header_Station_country_DataFrame[13]]
            GreenEnergy= df_get_iter_item_dfStation[header_Station_country_DataFrame[14]]
            FreeParking= df_get_iter_item_dfStation[header_Station_country_DataFrame[15]]
            PriceUrl= df_get_iter_item_dfStation[header_Station_country_DataFrame[16]]
            Public= df_get_iter_item_dfStation[header_Station_country_DataFrame[17]]
            OpeningHours_text= df_get_iter_item_dfStation[header_Station_country_DataFrame[18]]
            OpeningHours_details= df_get_iter_item_dfStation[header_Station_country_DataFrame[19]]

            #print(k)
            #print(m)
            #print(Streetdata)
            #print("--------")

            if(df_get_iter_item_dfStation[PLZ].isdigit() and not df_get_iter_item_dfStation[Ort].isdigit()):

                ################### Now here we know, that the PLZ and Location are correct
                #print("Correct Values")
                # Now get the PLZ
                func_fillObjectPLZ(Pcountry=country_fill,
                Pplz=df_get_iter_item_dfStation[PLZ],
                Port=df_get_iter_item_dfStation[Ort])

                #Now get the Street
                func_fillObjectStreet(Splz=df_get_iter_item_dfStation[PLZ],
                Slocation=df_get_iter_item_dfStation[Ort],
                Sstreet=df_get_iter_item_dfStation[Straße])

             

                ### Fill the Station now
                PostCode = df_get_iter_item_dfStation[PLZ]
                City = df_get_iter_item_dfStation[Ort]
                
                #print("Start Station fill")

                func_fillObjectStation(StStationId=Stationid, StStationStatus=StationStatus, StLabel=Label, StDescription=Description, StPostCode=PostCode,StCity=City,
                StStreetdata=Streetdata,StLatitiude=Latitude,StLongitude=Longitude,StContactName=ContactName,StTelephone=Telephone,StEmail=Email,StWebsite=Website,StDirections=Directions,
                StGreenEnergy=GreenEnergy,StFreeParking=FreeParking,StPriceUrl=PriceUrl,StPublic=Public, StDateTime = actual_dateTime ,StOpeningHours_text=OpeningHours_text,
                StOpeningHours_details=OpeningHours_details,StoperatorId=str(get_OID))


                
            elif(df_get_iter_item_dfStation[PLZ][1:4].isdigit() and not df_get_iter_item_dfStation[Ort].isdigit()):

                #### This is, if we know, that PLZ and Location are in the same column --> cut the first half of PLZ until " "=space occurs
                #print(df_get_iter_item_dfStation[PLZ])
                #if they right PLZ and Location in the same column
                cut_PLZ = df_get_iter_item_dfStation[PLZ].split(" ")[0]
                #print("Cut the PLZ into half and keep this half {plz}".format(plz=cut_PLZ))

                # Now get the PLZ
                func_fillObjectPLZ(Pcountry=country_fill,
                Pplz=cut_PLZ,
                Port=df_get_iter_item_dfStation[Ort])

                #Now get the Street
                func_fillObjectStreet(Splz=cut_PLZ,
                Slocation=df_get_iter_item_dfStation[Ort],
                Sstreet=df_get_iter_item_dfStation[Straße])


                
                ### Fill the Station now
                PostCode = cut_PLZ
                City = df_get_iter_item_dfStation[Ort]
                

                func_fillObjectStation(StStationId=Stationid, StStationStatus=StationStatus, StLabel=Label, StDescription=Description, StPostCode=PostCode,StCity=City,
                StStreetdata=Streetdata,StLatitiude=Latitude,StLongitude=Longitude,StContactName=ContactName,StTelephone=Telephone,StEmail=Email,StWebsite=Website,StDirections=Directions,
                StGreenEnergy=GreenEnergy,StFreeParking=FreeParking,StPriceUrl=PriceUrl,StPublic=Public, StDateTime = actual_dateTime,StOpeningHours_text=OpeningHours_text,
                StOpeningHours_details=OpeningHours_details,StoperatorId=str(get_OID))



            elif (df_get_iter_item_dfStation[Ort].isdigit() and not df_get_iter_item_dfStation[PLZ].isdigit()):
                ###### Here we know that location and plz are switched
                #print("Switched Values")

                # Now get the PLZ
                func_fillObjectPLZ(Pcountry=country_fill,
                Pplz=df_get_iter_item_dfStation[Ort],
                Port=df_get_iter_item_dfStation[PLZ])

                #Now get the Street
                func_fillObjectStreet(Splz=df_get_iter_item_dfStation[Ort],
                Slocation=df_get_iter_item_dfStation[PLZ],
                Sstreet=df_get_iter_item_dfStation[Straße])


                ### Fill the Station now
                PostCode = df_get_iter_item_dfStation[Ort]
                City = df_get_iter_item_dfStation[PLZ]
                
                func_fillObjectStation(StStationId=Stationid, StStationStatus=StationStatus, StLabel=Label, StDescription=Description, StPostCode=PostCode,StCity=City,
                StStreetdata=Streetdata,StLatitiude=Latitude,StLongitude=Longitude,StContactName=ContactName,StTelephone=Telephone,StEmail=Email,StWebsite=Website,StDirections=Directions,
                StGreenEnergy=GreenEnergy,StFreeParking=FreeParking,StPriceUrl=PriceUrl,StPublic=Public,StDateTime=actual_dateTime,StOpeningHours_text=OpeningHours_text,
                StOpeningHours_details=OpeningHours_details,StoperatorId=str(get_OID))


            elif(df_get_iter_item_dfStation[PLZ].isdigit() and df_get_iter_item_dfStation[Ort].isdigit()):
                # Here is now the Location and the postCode a digit

                # Now get the PLZ
                func_fillObjectPLZ(Pcountry=country_fill,
                Pplz=df_get_iter_item_dfStation[PLZ],
                Port=df_get_iter_item_dfStation[Ort])

                #Now get the Street
                func_fillObjectStreet(Splz=df_get_iter_item_dfStation[PLZ],
                Slocation=df_get_iter_item_dfStation[Ort],
                Sstreet=df_get_iter_item_dfStation[Straße])


                ### Fill the Station now
                PostCode = df_get_iter_item_dfStation[PLZ]
                City = df_get_iter_item_dfStation[Ort]
                
                func_fillObjectStation(StStationId=Stationid, StStationStatus=StationStatus, StLabel=Label, StDescription=Description, StPostCode=PostCode,StCity=City,
                StStreetdata=Streetdata,StLatitiude=Latitude,StLongitude=Longitude,StContactName=ContactName,StTelephone=Telephone,StEmail=Email,StWebsite=Website,StDirections=Directions,
                StGreenEnergy=GreenEnergy,StFreeParking=FreeParking,StPriceUrl=PriceUrl,StPublic=Public,StDateTime = actual_dateTime,StOpeningHours_text=OpeningHours_text,
                StOpeningHours_details=OpeningHours_details,StoperatorId=str(get_OID))


            else:
                print("something different occurs")
                print(df_get_iter_item_dfStation)
                print("-----------")
            #print("----------------------------------------------------------------------------------------")


print(" Finished Station API request: The Station request from the API is finished")
