##############################################  BLOCK Package  ########################################################


from lib2to3.pytree import convert
from logging import PlaceHolder
import os
#os.system('Object.py')
#from curses import meta
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



##################################################################################################################################################################
""" HERE we are using the query's to get Information for the API """
##################################################################################################################################################################

#region GET_API_Information

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

#print(df_Operator_AT[df_Operator_AT["city"] == "6556"])
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

#print(df_Ladestationen_Station_Whole)

#endregion

##################################################################################################################################################################
""" HERE we start filling the PLZ Table in MySQL which is the Base for other implementations """
##################################################################################################################################################################




#region Fill_PLZ_Table
#counter = 0
### Now we can add all other stations
for i in get_operatorId_AT:
    #counter = counter + 1
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
""" START with the Adresses Filling in the SQL-DataBase """
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




##################################################################################################################################################################
""" START with the Operator Filling in the SQL-DataBase """
##################################################################################################################################################################

#region FILL other SQL-DataBases

df_API_Operator_AUT = requests.get(url_AT, headers=headers, auth=auth).json() #only AUT
df_API_Operator_AUT_DataFrame = json_normalize(df_API_Operator_AUT)

header_info_df_Operator = (df_API_Operator_AUT_DataFrame.columns)

#endregion



a=1


