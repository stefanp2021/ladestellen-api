##############################################  BLOCK Package  ########################################################


from logging import PlaceHolder
import os
#os.system('Object.py')
#from curses import meta
from importlib.metadata import metadata
import json
import string
from requests.auth import HTTPBasicAuth
import requests
import pandas as pd
from pandas import json_normalize
from pathlib import Path
from dataclasses import dataclass
import mysql.connector
import copy

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



##############################################  BLOCK Abfrage  ########################################################

data_set_AT = requests.get(url_AT, headers=headers, auth=auth).json() #only AUT
df_Operator_AT = json_normalize(data_set_AT)
header_info_AT = (df_Operator_AT.columns)

# Get all ID's from the Operators
get_operatorId_AT = list(df_Operator_AT[header_info_AT[0]])
#get_operatorName = list(df_Operator_AT[header_info_AT[2]])


print('-------')
###### Create new Table for adresses with the same name as from API

### Get all adress-information from Operators

location = header_info_AT[9:13]

country = location[0]
PLZ = location[1]
Ort = location[2]
Straße = location[3]


op_ID = header_info_AT[0]
op_Name = header_info_AT[2]

df_table_adress_Operator = df_Operator_AT[[country,PLZ,Ort,Straße]]
df_table_adress_Station = pd.DataFrame(columns=df_table_adress_Operator.columns)

#### Hier werden jetzt die Stationen ausgelesen, pro Land und Operator --> jetzt nur AUT

### Need one for beginn ######## --> TO START THE WHOLE DATAFRAME

urlstation = 'https://api.e-control.at/charge/1.0/countries/{cAT}/operators/{operator_id}/stations'.format(cAT = countryID_AT,operator_id=get_operatorId_AT[1])
data_op_LS_Base = requests.get(urlstation, headers=headers, auth=auth).json()
df_LS_AUT_Base = json_normalize(data_op_LS_Base)

df_shape_column_AUT_Base = df_LS_AUT_Base.shape[0]

#Now add the Operator-Id from before and the country
df_LS_AUT_Base[op_ID] = ""
df_LS_AUT_Base[op_Name] = ""
df_LS_AUT_Base["country"] = "AT"

#df_column_names = list(df_LS_AUT_Base.columns)
#print(op_ID,op_Name)

df_Ladestationen_Station_Whole = pd.DataFrame(columns=df_LS_AUT_Base.columns)

waste_op_id = []

#a = df_Operator_AT[df_Operator_AT[op_ID] == get_operatorId_AT[1] ]
#print(a[op_Name].values[0])

print(df_Ladestationen_Station_Whole)


### Now we can add all other stations
for i in get_operatorId_AT:

    urlstation = 'https://api.e-control.at/charge/1.0/countries/{cAT}/operators/{operator_id}/stations'.format(cAT = countryID_AT,operator_id=str(i))

    data_op_LS = requests.get(urlstation, headers=headers, auth=auth).json()
    df_LS_AUT = json_normalize(data_op_LS)

    Get_Op_name_for_station = df_Operator_AT[df_Operator_AT[op_ID] == i]
    Real_Operator_Name_for_Station = Get_Op_name_for_station[op_Name].values[0]
    
    # Now here we need the List with the exact length to Add the OperatorId to the individual Stations
    df_shape_column_AUT = df_LS_AUT.shape[0]
    list_op_Station = [i for x in range(int(df_shape_column_AUT))]
    list_op_Station_Name = [Real_Operator_Name_for_Station for x in range(int(df_shape_column_AUT))]

    #Now add the Operator-Id from before and the country
    df_LS_AUT[op_ID] = list_op_Station
    df_LS_AUT[op_Name] = list_op_Station_Name
    df_LS_AUT["country"] = "AT"

    df_column_names = list(df_LS_AUT.columns)

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



mydb = mysql.connector.connect(
        host="dev.muenzer.at",
        user="ladestellen",
        password ="ybV1NfB0sCrzWS22hzOiMZ7YwkmtIwMT",
        database="ladestellen"
    )


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
                        print("Post: "+pC)
                        print("Ort: "+City)
                        print('------')

                        boolDigit = pC.startswith('-') and pC[1:].isdigit()
                        boolTextDigit = City.isdigit()

                        if(boolDigit == True or boolTextDigit==False):#and isinstance(City,string)
                            mycursor_1 = mydb.cursor()
                            sql_1 = "INSERT INTO tbl_plz (city, postCode, country) VALUES (%s, %s, %s)"
                            val_1 = (City, pC, land)
                            mycursor_1.execute(sql_1,val_1)
                        else:
                            print("Something went wrong with {a}, {b}".format(a=pC, b=City))

        mydb.commit()
