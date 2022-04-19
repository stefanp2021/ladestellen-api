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

import tqdm
from tqdm import tqdm 

#from sqlalchemy import column, null
from sqlobject import Operator, Station, Street, OCountry, PLZ_Location, OType


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
StEmail,StWebsite,StDirections,StGreenEnergy, StFreeParking, StPriceUrl, StPublic,StOpeningHours_text,StOpeningHours_details,StoperatorId):
    #To fill the Station-Object
    obj_Station = Station(stationId=StStationId, stationStatus=StStationStatus,label=StLabel,description=StDescription,postCode=StPostCode,city=StCity,
    street=StStreetdata,latitude=StLatitiude,longitude=StLongitude,contactName=StContactName,telephone=StTelephone,email=StEmail,website=StWebsite,directions=StDirections,
    greenEnergy=StGreenEnergy,freeParking=StFreeParking,priceUrl=StPriceUrl,public=StPublic,openingHours=StOpeningHours_text,openingHoursdetails=StOpeningHours_details,operater_id=StoperatorId)

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
        host="dev.muenzer.at",
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
                StGreenEnergy=GreenEnergy,StFreeParking=FreeParking,StPriceUrl=PriceUrl,StPublic=Public,StOpeningHours_text=OpeningHours_text,
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
                StGreenEnergy=GreenEnergy,StFreeParking=FreeParking,StPriceUrl=PriceUrl,StPublic=Public,StOpeningHours_text=OpeningHours_text,
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
                StGreenEnergy=GreenEnergy,StFreeParking=FreeParking,StPriceUrl=PriceUrl,StPublic=Public,StOpeningHours_text=OpeningHours_text,
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
                StGreenEnergy=GreenEnergy,StFreeParking=FreeParking,StPriceUrl=PriceUrl,StPublic=Public,StOpeningHours_text=OpeningHours_text,
                StOpeningHours_details=OpeningHours_details,StoperatorId=str(get_OID))


            else:
                print("something different occurs")
                print(df_get_iter_item_dfStation)
                print("-----------")
            #print("----------------------------------------------------------------------------------------")


print(" Finished Station API request: The Station request from the API is finished")





























































#print(df_Ladestationen_Station_Whole)


##########################################################################################
#######                 NOW we can start with filling the SQL_table again --> tbl_Plz, tbl_Street, tbl_country, tbl_Station
#
"""

for i in range(df_Ladestationen_Station_Whole.shape[0]):
    get_Base_for_Station = pd.DataFrame(df_Ladestationen_Station_Whole.loc[i])
    #print(i)
    #print(get_Base_for_Station)
    
   
    if(get_Base_for_Station.empty == True):
        pass
    else:
        #print(get_Base_for_Station)
        read_PLZ_Station = get_Base_for_Station[columns_we_want[PLZ]].values
        #print(read_PLZ_Station)

        if(len(read_PLZ_Station[0] < 5)):
            land_Station = "AT"
        else:
            land_Station = "DE"
        ## Now get the Country

        # Now get the PLZ
        obj_PLZ = PLZ_Location(country=land_Station,
        postcode=get_Base_for_Station[PLZ],
        location=get_Base_for_Station[Ort])
        obj_PLZ.CountryID = obj_PLZ.ReturnCountryID(mydb)[0][0]
        count_plz = obj_PLZ.AskCountPLZ(mydb)
        if(count_plz[0][0] < 1):
            obj_PLZ.InsertSQLPLZ(mydb)
        else:
            pass
        del obj_PLZ

    #Now get the Street
  
    obj_Street = Street(postCode=get_Base_for_Station[PLZ],
    location=get_Base_for_Station[Ort],
    street=get_Base_for_Station[Straße])
    obj_Street.PLZID = obj_Street.ReturnPLZID(mydb)[0][0]
    count_street = obj_Street.AskCountStreet(mydb)
    if(count_street[0][0] < 1):
        obj_Street.InsertSQLStreet(mydb)
    else:
        pass
    del obj_Street
    """













############### SicherheitsSave
##########################################################################################
#######                 NOW we can start with filling the SQL_table --> tbl_Plz, tbl_Street, tbl_country, tbl_Operator
#
"""
for i in range(df_Operator_approved.shape[0]):
    get_Base_for_Objects = df_Operator_approved.loc[i]

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
    obj_PLZ = PLZ_Location(country=get_Base_for_Objects[country],
    postcode=get_Base_for_Objects[PLZ],
    location=get_Base_for_Objects[Ort])
    obj_PLZ.CountryID = obj_PLZ.ReturnCountryID(mydb)[0][0]
    count_plz = obj_PLZ.AskCountPLZ(mydb)
    if(count_plz[0][0] < 1):
        obj_PLZ.InsertSQLPLZ(mydb)
    else:
        pass
    del obj_PLZ

    #Now get the Street
    obj_Street = Street(postCode=get_Base_for_Objects[PLZ],
    location=get_Base_for_Objects[Ort],
    street=get_Base_for_Objects[Straße])
    obj_Street.PLZID = obj_Street.ReturnPLZID(mydb)[0][0]
    count_street = obj_Street.AskCountStreet(mydb)
    if(count_street[0][0] < 1):
        obj_Street.InsertSQLStreet(mydb)
    else:
        pass
    del obj_Street

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


"""

























































































































"""
for j in range(len(get_operatorId)):
    get_OID = get_operatorId[j]

    urlstation = 'https://api.e-control.at/charge/1.0/countries/{cAT}/operators/{operator_id}/stations'.format(cAT = countryID[0],operator_id=str(get_OID)) #derzeit 1 weil dort ein wert drinsteht
    data_op_LS_Base = requests.get(urlstation, headers=headers, auth=auth).json()
    df_LS_AUT_Base = json_normalize(data_op_LS_Base)

    if(df_LS_AUT_Base.empty == True):
        pass
    else:
        #Now we found a non empty URL-Request, so we can break out of the for-loop at the end
        df_shape_column_AUT_Base = df_LS_AUT_Base.shape[0]

        #Now add the Operator-Id/name from before and the country because a station don't have that BASIC-INFOS!!!
        df_LS_AUT_Base[op_ID] = ""
        df_LS_AUT_Base[op_Name] = ""
        df_LS_AUT_Base[country] = ""

        df_Ladestationen_Station_Whole = pd.DataFrame(columns=df_LS_AUT_Base.columns)

        break







#endregion

##################################################################################################################################################################
#   HERE we start filling the PLZ Table in MySQL which is the Base for other implementations 
##################################################################################################################################################################

#region Fill_PLZ_Table

### Now we can add all other stations


#df_Operator contains ALL Operator, now we select specific ones

for i in get_operatorId:
    
    urlstation = 'https://api.e-control.at/charge/1.0/countries/{cAT}/operators/{operator_id}/stations'.format(cAT = countryID[0],operator_id=str(i))

    data_op_LS = requests.get(urlstation, headers=headers, auth=auth).json()
    df_LS_AUT = json_normalize(data_op_LS)

    #print()
    Get_Op_name_for_station = df_Operator[df_Operator[op_ID] == i]
    #print(Get_Op_name_for_station)
    Real_Operator_Name_for_Station = Get_Op_name_for_station[op_Name].values[0]
    
    #print()


    # Now here we need the List with the exact length to Add the OperatorId to the individual Stations
    df_shape_column_AUT = df_LS_AUT.shape[0]
    list_op_Station = [i for x in range(int(df_shape_column_AUT))]
    list_op_Station_Name = [Real_Operator_Name_for_Station for x in range(int(df_shape_column_AUT))]

    #Now add the Operator-Id from before and the country
    df_LS_AUT[op_ID] = list_op_Station
    df_LS_AUT[op_Name] = list_op_Station_Name
    df_LS_AUT["country"] = "AT"

    df_column_names = list(df_LS_AUT.columns)


    #print(df_LS_AUT.loc[df_LS_AUT["city"] == "ALKOVEN"])
    #if (df_shape_column_AUT > 0 ):
    #    print(df_LS_AUT[["city","postCode","operatorId"]])
    #    print(df_LS_AUT.iloc[:,-3])
        #print(df_LS_AUT)

    #    print(df_LS_AUT.loc[df_LS_AUT["city"]=="ALKOVEN"])
    ##### Now fill the whole df_table_adress with the adresses from the organizer first
    ##### But some Operators are empty
    if(df_LS_AUT.empty == True):
        waste_op_id.append(id)
    else:

        frames_Station = [df_Ladestationen_Station_Whole,df_LS_AUT]
        df_Ladestationen_Station_Whole = pd.concat(frames_Station)


        df_table_single_Operator_Station = df_LS_AUT[[country,PLZ,Ort,Straße]]

        frames = [df_table_adress_Station,df_table_single_Operator_Station]
        df_table_adress_Station = pd.concat(frames)

###


print('------------------------')

#print(df_table_adress_Station)

#print(df_Ladestationen_Station_Whole)

df_Ladestationen_Station_Whole.reset_index(inplace=True,drop=True)

#print(df_table_adress_Station)
#print(df_table_adress_Operator)
#filepath_Adresse = Path('output/Station_Whole.csv')
#df_Ladestationen_Station_Whole.to_csv(filepath_Adresse, header=True, index=False)

    # And now if the SQL-Table is not empty, add at least 1 Null line 


##### Now we add the Adresses to tbl_PLZ


## But first we need a Null entry, else it isn't possible to fill it up with that
Null_Dataset = df_table_adress_Operator[df_table_adress_Operator[PLZ].isnull() | df_table_adress_Operator[Ort].isnull()]
if(Null_Dataset.empty == False):
    select_Null = Null_Dataset.iloc[0,:]
    pC_Null = select_Null["postCode"]
    City_Null = select_Null["city"]
    land_Null = select_Null["country"]
    mycursor_Null = mydb.cursor()
    #### IF SQL-Table is empty
    sql_Null_Count = "SELECT COUNT(*) FROM tbl_plz"
    mycursor_Null.execute(sql_Null_Count)
    count_result = mycursor_Null.fetchall()
    count_result_tuple = count_result[0]
    count_Null_select = count_result_tuple[0]
    print(count_Null_select)
    if count_Null_select < 1:
        sql_Null = "INSERT INTO tbl_plz (city, postCode, country) VALUES (%s, %s, %s)"
        val_Null = (City_Null, pC_Null, land_Null)
        mycursor_Null.execute(sql_Null,val_Null)

## Now we have 1 Null element we add all the others
print('-#-#-#-#-#-#-#-#-##-#-#-#-#--#-#-#-#-##-#-#-#-#-#-#-#-#-#-#-##-#-#-#-#-#-#--#-##-#-#-#--##-#-#-#-#--##-')
mycursor = mydb.cursor()
sql = "SELECT * FROM tbl_plz WHERE city=%s and postCode=%s and country=%s"

for i in range(df_table_adress_Operator.shape[0]):
        data_row = df_table_adress_Operator.iloc[i,:]

        pC = data_row["postCode"]
        City = data_row["city"]
        land = data_row["country"]

        val = (City, pC,land)
        mycursor.execute(sql,val)
        myresult = mycursor.fetchall()


        #print(type(myresult))
        if(len(myresult)) != 0:
            #print("{pc} | {cit} already existing".format(pc = pC, cit=City))
            pass
        else:
            if (pC is None or City is None):
                print("NULL is already existing")
                print(myresult)
            else:
                mycursor_1 = mydb.cursor()
                sql_1 = "INSERT INTO tbl_plz (city, postCode, country) VALUES (%s, %s, %s)"
                val_1 = (City, pC, land)
                mycursor_1.execute(sql_1,val_1)       

        mydb.commit()


### Now we add the Stations

mydb = mysql.connector.connect(
        host="dev.muenzer.at",
        user="ladestellen",
        password ="ybV1NfB0sCrzWS22hzOiMZ7YwkmtIwMT",
        database="ladestellen"
    )

print('-#-#-#-#-#-#-#-#-##-#-#-#-#--#-#-#-#-##-#-#-#-#-#-#-#-#-#-#-##-#-#-#-#-#-#--#-##-#-#-#--##-#-#-#-#--##-')
mycursor = mydb.cursor()
sql = "SELECT * FROM tbl_plz WHERE city=%s and postCode=%s and country=%s"

for i in range(df_table_adress_Station.shape[0]):
        data_row = df_table_adress_Station.iloc[i,:]

        pC = data_row["postCode"]
        City = data_row["city"]
        land = data_row["country"]

        val = (City, pC,land)
        mycursor.execute(sql,val)
        myresult_Stat = mycursor.fetchall()
        #print(myresult_Stat)
        #print(len(myresult_Stat))
        #print("----")
        if(len(myresult_Stat)) != 0:
            #print("{pc} | {cit} already existing".format(pc = pC, cit=City))
            pass
        else:
            if (pC is None or City is None):
                print("NULL is already existing")
                #print(myresult)
            else:
                #print(type(pC))
                #print(type(City))
                ### Now we have to look, if there is PLZ and Location switched

                boolDigit = pC.startswith('-') and pC[1:].isdigit()
                boolTextDigit = City.isdigit()
                if(boolDigit == True or boolTextDigit==False): #and isinstance(City,string)
                    
                    #print("Right path Station")

                    mycursor_1 = mydb.cursor()
                    sql_1 = "INSERT INTO tbl_plz (city, postCode, country) VALUES (%s, %s, %s)"
                    val_1 = (City, pC, land)
                    mycursor_1.execute(sql_1,val_1)
                else:
                    
                    #print("Right path Station but with SWITCH")

                    pC = data_row["city"]
                    City = data_row["postCode"]
                    # Now we need to see, if the values are now correct

                    #Now look if the switched version are in the sql-table
                    mycursor_switch = mydb.cursor()
                    sql_switch = "SELECT * FROM tbl_plz WHERE city=%s and postCode=%s and country=%s"
                    val_switch = (City,pC,land)
                    mycursor_switch.execute(sql_switch,val_switch)
                    myresult_switch = mycursor_switch.fetchall()
                    if(len(myresult_switch)) != 0:
                        #print("{pc} | {cit} already existing".format(pc = pC, cit=City))
                        pass
                    else:
                        #print("Post: "+pC)
                        #print("Ort: "+City)
                        #print('------')

                        boolDigit = pC.startswith('-') and pC[1:].isdigit()
                        boolTextDigit = City.isdigit()

                        if(boolDigit == True or boolTextDigit==False):#and isinstance(City,string)
                            mycursor_1 = mydb.cursor()
                            sql_1 = "INSERT INTO tbl_plz (city, postCode, country) VALUES (%s, %s, %s)"
                            val_1 = (City, pC, land)
                            mycursor_1.execute(sql_1,val_1)
                        else:
                            print("Something went wrong with {a}, {b} before".format(a=pC, b=City))
                            #Now for the rest witch doesn't fit anywhere
                            #Abfrage ob es bereits existiert
                            
                            mycursor_Rest = mydb.cursor()
                            sql_Rest = "SELECT * FROM tbl_plz WHERE city=%s and postCode=%s and country=%s"
                            val_Rest = (City,pC,land)
                            mycursor_Rest.execute(sql_Rest,val_Rest)
                            myresult_Rest = mycursor_Rest.fetchall()
                            if(len(myresult_Rest)) != 0:
                                print("However it is in Database and functionally")
                                pass
                            else:
                                print("But it is fixed now")
                                mycursor_RestInsert = mydb.cursor()
                                sql_RestInsert = "INSERT INTO tbl_plz (city, postCode, country) VALUES (%s, %s, %s)"
                                val_RestInsert = (str(City), pC, land)
                                mycursor_RestInsert.execute(sql_RestInsert,val_RestInsert)

        mydb.commit()


#endregion

### Now here we fill in all other Information with the PLZ-values from the SQL Database


##################################################################################################################################################################
#        START with the Adresses Filling in the SQL-DataBase 
##################################################################################################################################################################

#region FILL other SQL-DataBases

## Fill table_Street first
### First with Operator
print("##############################--------------------------###############################-----------------------------########################")
mydb = mysql.connector.connect(
        host="dev.muenzer.at",
        user="ladestellen",
        password ="ybV1NfB0sCrzWS22hzOiMZ7YwkmtIwMT",
        database="ladestellen"
    )

def func_Insert_In_tblStreet(df_data, db_connection):
    mycursor = db_connection.cursor()

    counter=0

    for i in range(df_data.shape[0]):
        data_row = df_data.iloc[i,:]

        counter = counter + 1

        pC = data_row["postCode"]
        City = data_row["city"]
        land = data_row["country"]
        data_street = data_row["street"]

        #print(myresult_Street)
        ## Here we check if we get a NULL from the API or if PLZ/City are switched
        if(City is None or pC is None):
            obj_street = Street(country=land,postCode=None,location=None,street=data_street)

            ##Abfrage ob es das schon gibt
            count_items_table = obj_street.AskCountStreet(connector=db_connection,booleanValue=True)
            count_items_table_single = count_items_table[0]
            ##Wenn existierend
            if(count_items_table_single[0] < 1):
                sql = "SELECT PLZ_ID FROM tbl_plz WHERE city IS NULL AND postCode IS NULL"
                mycursor.execute(sql)
                myresult_Street = mycursor.fetchall()
                print("PLZ bei None")
                get_PLZ_ID = myresult_Street[0][0]
                obj_street.PLZID = get_PLZ_ID
                
                obj_street.UpdateSQLStreet(db_connection)
                del(obj_street)
            else:
                #Existiert bereits
                pass

        else:
            
            try:
                testpC = int(pC)
            except ValueError:
                testpC=str(pC)

            if(isinstance(testpC,int)):

                obj_street = Street(country=land,postCode=pC,location=City,street=data_street)
                #print(obj_street.street)

                ##Abfrage ob es das schon gibt
                count_items_table = obj_street.AskCountStreet(connector=db_connection,booleanValue=False)
                count_items_table_single = count_items_table[0]
                #print(count_items_table_single[0])
                #print("--------------------------------------------")

                if(count_items_table_single[0] < 1):

                    sql = "SELECT PLZ_ID FROM tbl_plz WHERE city=%s AND postCode=%s AND country=%s"
                    val = (obj_street.location, obj_street.postCode, obj_street.country)
                    mycursor.execute(sql,val)
                    myresult_Street = mycursor.fetchall()
                    #print("PLZ bei normal")
                    get_PLZ_ID = myresult_Street[0][0]
                    obj_street.PLZID = get_PLZ_ID

                    obj_street.UpdateSQLStreet(db_connection)

                    del(obj_street)

            else:
                #Now we have to look if the Postcode is just switched with the City or they are both in the PLZ-entry
                #print(data_row)
                #print("City = {}".format(City))
                #print("postCode = {}".format(pC))


                pC_pruef = pC[0:4]

                try:
                    testpC1 = int(pC_pruef)
                except ValueError:
                    testpC1 =str(pC_pruef)

                if(isinstance(testpC1,int)):
                    # in some cases the PLZ and City are in the same field
                    obj_street = Street(country=land,postCode=pC,location=City,street=data_street)
                    #print("Should be in same field: " + obj_street.postCode)

                else:
                    #So here we have PLZ and City switched, now we reswitch it
                    obj_street = Street(country=land,postCode=City,location=pC,street=data_street)
                    #print("should be switched: postCode = {0}     city = {1}".format(obj_street.postCode, obj_street.location))

                print("Umgedreht")
                

                count_items_table = obj_street.AskCountStreet(connector=db_connection,booleanValue=False)
                count_items_table_single = count_items_table[0]
                #print(type(count_items_table_single))
                #print(count_items_table_single[0][0])

                if(count_items_table_single[0] < 1):
                
                    sql = "SELECT PLZ_ID FROM tbl_plz WHERE city=%s AND postCode=%s AND country=%s"
                    val = (obj_street.location, obj_street.postCode, obj_street.country)
                    #print("SELECT PLZ_ID FROM tbl_plz WHERE city={a} AND postCode={b} AND country={c}".format(a=obj_street.location, b = obj_street.postCode, c = obj_street.country))
                    mycursor.execute(sql,val)
                    myresult_Street = mycursor.fetchall()
                    print("PLZ bei umgedreht")
                    get_PLZ_ID = myresult_Street[0][0]
                    obj_street.PLZID = get_PLZ_ID

                    obj_street.UpdateSQLStreet(db_connection)

                    del(obj_street)

        #print(myresult_Street[0][0])
        db_connection.commit()
    mycursor.close()


df_adr_tail = df_table_adress_Station.tail()
print(df_adr_tail[["city","postCode","country"]])

func_Insert_In_tblStreet(df_table_adress_Operator, mydb)
func_Insert_In_tblStreet(df_table_adress_Station, mydb)


#print(df_table_adress_Station[df_table_adress_Station["city"] == "ALKOVEN"])


print('-#-#-#-#-')

#print(df_table_adress_Station)


#endregion

"""
