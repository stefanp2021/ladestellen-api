### Class -object



from itertools import count
from turtle import st

#from sqlalchemy import true

def func_GetRidofNone(wert):
    if((wert is None) or (wert == "Null") or (wert=="NULL") or (wert == "None") or (wert == "NONE")):
        wert=""
        return(wert)
    else:
        return(wert)



class OCountry:
    species = "Country Name"

    def __init__(self, country):
        self.country = country

    def AskCountCountry(self, connector):
        mycursor = connector.cursor()
        
        sql_country = "SELECT COUNT(*) FROM tbl_country WHERE CountryName =%s" # "SELECT * FROM tbl_addr WHERE street IS NULL" 
        val_country = (self.countryname,)
        mycursor.execute(sql_country,val_country)
        myresult_country = mycursor.fetchall()
        connector.commit()
        mycursor.close()
        return(myresult_country)

    def InsertSQLCountry(self,connector):
        mycursor = connector.cursor()
        sql = "INSERT INTO tbl_country (countryname) VALUES (%s)"
        val = (self.country,)
        mycursor.execute(sql,val)
        connector.commit()
        mycursor.close()

    def __del__(self):
        pass


class PLZ_Location:
    species = "Location Name"

    def __init__(self, country, postcode, location):
        self.country = country
        self.postCode = postcode
        self.location = location
        self.CountryID = ''


    def __call__(self):
        print(self)
 

    def ReturnCountryID(self, connector):
        mycursor = connector.cursor()

        sql_countryid = "SELECT country_ID FROM tbl_country WHERE countryname=%s"
        val_countryid = (self.country,)
        mycursor.execute(sql_countryid,val_countryid)
        myresult_CountryID = mycursor.fetchall()
        connector.commit()
        mycursor.close()
        return(myresult_CountryID)
    
    def AskCountPLZ(self, connector):
        mycursor = connector.cursor()

        #ask for 3 different ways: 1)all good, 2)all null,3) location is null 
        # Number 2) all null
        if (self.postCode is None):
            sql = "SELECT COUNT(*) FROM tbl_plz WHERE postCode IS NULL"
            mycursor.execute(sql,)
            myresult_count = mycursor.fetchall()
            connector.commit()
        #Number 3) location null  
        elif(self.location is None):
            sql = "SELECT COUNT(*) FROM tbl_plz WHERE Location IS NULL AND postCode = %s"
            val = (self.postCode,)
            mycursor.execute(sql,val)
            myresult_count = mycursor.fetchall()
            connector.commit()
        #Number 1)
        else:
            sql = "SELECT COUNT(*) FROM tbl_plz WHERE postCode = %s AND city =%s"
            val = (self.postCode,self.location)
            mycursor.execute(sql,val)
            myresult_count = mycursor.fetchall()
            connector.commit()
        
        mycursor.close()
        return(myresult_count)

    def InsertSQLPLZ(self,connector):
        mycursor = connector.cursor()
        sql = "INSERT INTO tbl_plz (city,postCode,country_Id) VALUES (%s, %s,%s)"
        val = (self.location, self.postCode, self.CountryID )
        mycursor.execute(sql,val)
        connector.commit()
        mycursor.close()

    def __del__(self):
        pass




class Street:
    species = "Street Name"
    def __init__(self, postCode, location, street):
        self.street = street
        self.postCode = postCode
        self.location = location
        self.PLZID = ""


    def ReturnPLZID(self, connector):
        mycursor = connector.cursor()

        sql_countryid = "SELECT PLZ_ID FROM tbl_plz WHERE (city=%s OR city IS NULL) AND (postCode=%s OR postCode is NULL)"
        val_countryid = (self.location,self.postCode)
        mycursor.execute(sql_countryid,val_countryid)
        myresult_PLZID = mycursor.fetchall()
        connector.commit()
        mycursor.close()
        return(myresult_PLZID)

    def InsertSQLStreet(self,connector):
        mycursor = connector.cursor()
        sql = "INSERT INTO tbl_addr (street, Plz_Id) VALUES (%s, %s)"
        val = (self.street,self.PLZID)
        mycursor.execute(sql,val)
        connector.commit()

    def AskCountStreet(self, connector):

        if(self.street is None):
            mycursor = connector.cursor()
            sql = "SELECT COUNT(*) FROM tbl_addr WHERE street IS NULL AND Plz_Id=%s"
            val = (self.PLZID,)
            mycursor.execute(sql,val)
            myresult_count = mycursor.fetchall()
            connector.commit()
            mycursor.close()
            return(myresult_count)
        else:
            mycursor = connector.cursor()
            sql = "SELECT COUNT(*) FROM tbl_addr WHERE street=%s AND Plz_Id=%s"
            val = (self.street,self.PLZID)
            mycursor.execute(sql,val)
            myresult_count = mycursor.fetchall()
            connector.commit()
            mycursor.close()
            return(myresult_count)

    def __del__(self):
        pass
        #Deconstrutor


class OType:
    species = "Type Name"

    def __init__(self, type):
        self.type = type

    def AskCountOType(self, connector):
        mycursor = connector.cursor()
        
        sql = "SELECT COUNT(*) FROM tbl_OrgType WHERE OrgType =%s"
        val = (self.type,)
        mycursor.execute(sql,val)
        myresult_type = mycursor.fetchall()
        connector.commit()
        mycursor.close()
        return(myresult_type)

    def InsertSQLOType(self,connector):
        mycursor = connector.cursor()
        sql = "INSERT INTO tbl_OrgType (OrgType) VALUES (%s)"
        val = (self.type,)
        mycursor.execute(sql,val)
        connector.commit()
        mycursor.close()

    def __del__(self):
        pass


class Operator:
    species = "Operator Values"
    def __init__(self,operatorId,type,organization,commercialRegisterNumber,sex,
    titlePrefix,firstName,lastName,titleSuffix,country,postCode,city,street,
    website,status):
        self.operatorcode = operatorId
        self.type = type
        self.organization = organization
        self.commercialRegisterNumber = commercialRegisterNumber
        self.sex = sex
        self.titlePrefix = titlePrefix
        self.firstName=firstName
        self.lastName=lastName
        self.titleSuffix = titleSuffix
        self.country = country
        self.postCode = postCode
        self.city = city
        self.street=street
        self.website = website
        self.status = status
        self.StreetID = ""
        self.PLZID=""
        self.TypeID=""
        self.OLabel = "{o} {f} {l}".format(o = func_GetRidofNone(self.organization), f=func_GetRidofNone(self.firstName), l=func_GetRidofNone(self.lastName))
        #if(self.operatorcode == "MOO"):
        #    print(self.postCode, self.city,self.street)
    
    def AskCountOperator(self, connector):
        mycursor = connector.cursor()
        
        sql = "SELECT COUNT(*) FROM tbl_operators WHERE operator_code = %s AND address_Id =%s"
        val = (self.operatorcode,self.StreetID)
        mycursor.execute(sql,val)
        myresult_count = mycursor.fetchall()
        connector.commit()
        mycursor.close()
        return(myresult_count)

    def ReturnTypeID(self,connector):
        mycursor = connector.cursor()

        sql_type = "SELECT Org_ID FROM tbl_OrgType WHERE OrgType=%s"
        val_type = (self.type,)
        mycursor.execute(sql_type,val_type)
        myresult_OrgID = mycursor.fetchall()
        connector.commit()
        mycursor.close()
        return(myresult_OrgID)

    def ReturnPLZID(self,connector):
        mycursor = connector.cursor()

        sql_countryid = "SELECT PLZ_ID FROM tbl_plz WHERE (city=%s OR city IS NULL) AND (postCode=%s OR postCode is NULL)"
        val_countryid = (self.city,self.postCode)
        mycursor.execute(sql_countryid,val_countryid)
        myresult_PLZID = mycursor.fetchall()
        connector.commit()
        mycursor.close()
        return(myresult_PLZID)


    def ReturnStreetID(self, connector):
        mycursor = connector.cursor()
        
        sql_streetid = "SELECT Adress_ID FROM tbl_addr WHERE (street=%s or street IS NULL)"
        val_streetid = (self.street,)
        mycursor.execute(sql_streetid,val_streetid)
        myresult_StreetID = mycursor.fetchall()
        connector.commit()
        mycursor.close()
        return(myresult_StreetID)


    def InsertSQLOperator(self,connector):

        mycursor = connector.cursor()
        sql = "INSERT INTO tbl_operators (operator_code,address_Id,type_Id,organization,commercialRegisterNumber,sex,titlePrefix,firstName,lastName,titleSuffix,website,status,OrgLabel) VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s,%s,%s)"
        val = (self.operatorcode, self.StreetID, self.TypeID, self.organization, self.commercialRegisterNumber, self.sex, self.titlePrefix, self.firstName, 
        self.lastName, self.titleSuffix, self.website, self.status,self.OLabel)
        mycursor.execute(sql,val)
        connector.commit()
        mycursor.close()
 

    def UpdateSQLOperator(self,connector):

        mycursor = connector.cursor()
        #sql = "INSERT INTO tbl_operators (operatorId,address_Id,type,organization,commercialRegisterNumber,sex,titlePrefix,firstName,lastName,titleSuffix,website,deviatingOperatorIds,status) VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s)"
        sql = "UPDATE tbl_operators SET type_Id=%s,organization=%s,commercialRegisterNumber=%s,sex=%s,titlePrefix=%s,firstName=%s,lastName=%s,titleSuffix=%s,website=%s,status=%s,OrgLabel=%s WHERE operator_code=%s AND address_Id=%s"
        val = (self.TypeID, self.organization,self.commercialRegisterNumber,self.sex,self.titlePrefix,self.firstName, 
        self.lastName,self.titleSuffix,self.website,self.status,self.OLabel,self.operatorcode,self.StreetID)
        mycursor.execute(sql,val)
        connector.commit()
        mycursor.close()

    def __del__(self):
        pass
        #Deconstrutor

class Station:
    species = "Station Values"
    def __init__(self,stationId,stationStatus,label,description,postCode,city,street,latitude,longitude,
    contactName,telephone,email,website,directions,greenEnergy,freeParking,priceUrl,public,openingHours,openingHoursdetails,
    operater_id):
        self.stationId = stationId
        self.stationStatus = stationStatus
        self.label = label
        self.description = description
        self.postCode = postCode
        self.city = city
        self.street=street
        self.latitude= str(latitude)
        self.longitude = str(longitude)
        self.contactName = contactName
        self.telephone = telephone
        self.email = email
        self.directions=directions
        self.website = website
        self.greenEnergy = str(greenEnergy)
        self.freeParking = str(freeParking)
        self.priceUrl = priceUrl
        self.public = str(public)
        self.openingHours = openingHours
        self.openingHoursdetails = ""#openingHoursdetails
        self.operater_id = operater_id
        self.Operator_ID = ""
        self.StreetID = ""
        self.PLZID=""


    def UpdateSQLStation(self,connector):
        mycursor = connector.cursor()
        #print("UPDATE STATION {}".format(self.StreetID))
        sql = "UPDATE tbl_stations SET stationStatus=%s,label=%s,description=%s,latitude=%s,longitude=%s,contactName=%s,telephone=%s,email=%s,website=%s,directions=%s,greenEnergy=%s, freeParking=%s, priceUrl=%s, public=%s  WHERE stationId=%s AND address_Id=%s AND operator_Id=%s"
        val = (self.stationStatus, self.label, self.description , self.latitude, self.longitude,self.contactName, self.telephone, self.email,
        self.website, self.directions, self.greenEnergy, self.freeParking, self.priceUrl, self.public, self.stationId, self.StreetID, self.Operator_ID)
        mycursor.execute(sql,val)
        connector.commit()

    def InsertSQLStation(self,connector):
        mycursor = connector.cursor()
        #print("INSERT STATION {} should be Id".format(self.StreetID))
        sql = "INSERT INTO tbl_stations (stationId, operator_Id, stationStatus,label,description,address_Id,latitude,longitude,contactName,telephone,email,website,directions,greenEnergy,freeParking,priceUrl,public) VALUES (%s, %s,%s, %s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s,%s, %s,%s)"
        #print(sql)
        val = (self.stationId, self.Operator_ID, self.stationStatus, self.label, self.description, self.StreetID,self.latitude,self.longitude, 
        self.contactName,self.telephone,self.email, self.website,self.directions,self.greenEnergy,self.freeParking,self.priceUrl,
        self.public)
        mycursor.execute(sql,val)
        connector.commit()

    def AskCountStation(self, connector):
        mycursor = connector.cursor()

        sql = "SELECT COUNT(*) FROM tbl_stations WHERE stationId = %s AND address_Id=%s AND operator_Id = %s"
        val = (self.stationId,self.StreetID, self.Operator_ID)
        mycursor.execute(sql,val)
        myresult_count = mycursor.fetchall()
        connector.commit()
        return(myresult_count)

    def ReturnPLZID(self,connector):
        mycursor = connector.cursor()

        sql_countryid = "SELECT PLZ_ID FROM tbl_plz WHERE (city=%s OR city IS NULL) AND (postCode=%s OR postCode is NULL)"
        val_countryid = (self.city,self.postCode)
        mycursor.execute(sql_countryid,val_countryid)
        myresult_PLZID = mycursor.fetchall()
        connector.commit()
        mycursor.close()
        return(myresult_PLZID)

    def ReturnStreetID(self, connector):
        mycursor = connector.cursor()
        if(self.street is None):
            sql_streetid = "SELECT Adress_ID FROM tbl_addr WHERE street IS NULL AND Plz_Id=%s"
            val_street = (self.PLZID,)
            mycursor.execute(sql_streetid,val_street)
            myresult_StreetID = mycursor.fetchall()
            connector.commit()
        else:
            sql_streetid = "SELECT Adress_ID FROM tbl_addr WHERE street=%s AND Plz_Id=%s"
            val_streetid = (self.street,self.PLZID)
            mycursor.execute(sql_streetid,val_streetid)
            myresult_StreetID = mycursor.fetchall()
            #print(myresult_StreetID)
            connector.commit()

        return(myresult_StreetID)

    def ReturnOperatorID(self, connector):
        mycursor = connector.cursor()
        sql_operatorid = "SELECT Operators_ID FROM tbl_operators WHERE operator_code=%s"
        val_operatorid = (self.operater_id,)
        mycursor.execute(sql_operatorid,val_operatorid)
        myresult_OperatorID = mycursor.fetchall()
        connector.commit()
                
        return(myresult_OperatorID)

    def __del__(self):
        pass
        #Deconstrutor