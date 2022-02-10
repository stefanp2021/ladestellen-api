### Class -object



from itertools import count
from turtle import st

from sqlalchemy import true




class Street:
    species = "Street Name"
    def __init__(self, country, postCode, location, street):
        self.street = street
        self.country = country
        self.postCode = postCode
        self.location = location
        self.PLZID = ""

    def UpdateSQLStreet(self,connector):
        mycursor = connector.cursor()
        sql = "INSERT INTO tbl_addr (street, Plz_Id) VALUES (%s, %s)"
        val = (self.street,self.PLZID)
        mycursor.execute(sql,val)
        connector.commit()

    def AskCountStreet(self, connector, booleanValue):
        mycursor = connector.cursor()
        if(booleanValue==False):
            sql_pruefe = "SELECT COUNT(*) FROM tbl_addr WHERE street=%s" #"SELECT * FROM tbl_addr WHERE street=%s"
            val_pruefe = (self.street,)
            mycursor.execute(sql_pruefe,val_pruefe)
            myresult_Street = mycursor.fetchall()
            connector.commit()
        else:
            sql_pruefe = "SELECT COUNT(*) FROM tbl_addr WHERE street IS NULL" # "SELECT * FROM tbl_addr WHERE street IS NULL" 
            mycursor.execute(sql_pruefe)
            myresult_Street = mycursor.fetchall()
            connector.commit()

        return(myresult_Street)


    def __del__(self):
        pass
        #Deconstrutor


class Operator:
    species = "Operator Values"
    def __init__(self,operatorId,type,organization,commercialRegisterNumber,sex,
    titlePrefix,firstName,lastName,titleSuffix,country,postCode,city,street,
    website,deviatingOperatorIds,status):
        self.operatorID = operatorId
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
        self.deviatingOperatorIds = ""#deviatingOperatorIds
        self.status = status
        self.StreetID = ""

    
    def AskCountOperator(self, connector):
        mycursor = connector.cursor()

        sql = "SELECT COUNT(*) FROM tbl_operators WHERE operatorId = %s"
        val = (self.operatorID,)
        mycursor.execute(sql,val)
        myresult_count = mycursor.fetchall()
        connector.commit()
        return(myresult_count)

    def ReturnStreetID(self, connector):
        mycursor = connector.cursor()
        if(self.street is None):
            sql_streetid = "SELECT Adress_ID FROM tbl_addr WHERE street IS NULL"
            mycursor.execute(sql_streetid)
            myresult_StreetID = mycursor.fetchall()
            connector.commit()
        else:
            sql_streetid = "SELECT Adress_ID FROM tbl_addr WHERE street=%s"
            val_streetid = (self.street,)
            mycursor.execute(sql_streetid,val_streetid)
            myresult_StreetID = mycursor.fetchall()
            connector.commit()
            
        return(myresult_StreetID)


    def InsertSQLOperator(self,connector):
        mycursor = connector.cursor()
        sql = "INSERT INTO tbl_operators (operatorId,address_Id,type,organization,commercialRegisterNumber,sex,titlePrefix,firstName,lastName,titleSuffix,website,deviatingOperatorIds,status) VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s)"
        val = (self.operatorID, self.StreetID, self.type, self.organization, self.commercialRegisterNumber, self.sex, self.titlePrefix, self.firstName, 
        self.lastName, self.titleSuffix, self.website, self.deviatingOperatorIds, self.status)
        mycursor.execute(sql,val)
        connector.commit()
 

    def UpdateSQLOperator(self,connector):
        mycursor = connector.cursor()
        #sql = "INSERT INTO tbl_operators (operatorId,address_Id,type,organization,commercialRegisterNumber,sex,titlePrefix,firstName,lastName,titleSuffix,website,deviatingOperatorIds,status) VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s)"
        sql = "UPDATE tbl_operators SET address_Id=%s,type=%s,organization=%s,commercialRegisterNumber=%s,sex=%s,titlePrefix=%s,firstName=%s,lastName=%s,titleSuffix=%s,website=%s,deviatingOperatorIds=%s,status=%s WHERE operatorId=%s"
        val = (self.StreetID, self.type, self.organization,self.commercialRegisterNumber,self.sex,self.titlePrefix,self.firstName, 
        self.lastName,self.titleSuffix,self.website,self.deviatingOperatorIds,self.status,self.operatorID)
        mycursor.execute(sql,val)
        connector.commit()

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


    def UpdateSQLStation(self,connector):
        mycursor = connector.cursor()
        sql = "UPDATE tbl_stations SET operator_Id=%s,stationStatus=%s,label=%s,description=%s, address_Id=%s,latitude=%s,longitude=%s,contactName=%s,telephone=%s,email=%s,website=%s,directions=%s,greenEnergy=%s, freeParking=%s, priceUrl=%s, public=%s  WHERE stationId=%s"
        val = (self.Operator_ID, self.stationStatus, self.label, self.description ,self.StreetID, self.latitude, self.longitude,self.contactName, self.telephone, self.email,
        self.website, self.directions, self.greenEnergy, self.freeParking, self.priceUrl, self.public, self.stationId)
        mycursor.execute(sql,val)
        connector.commit()

    def InsertSQLStation(self,connector):
        mycursor = connector.cursor()
        sql = "INSERT INTO tbl_stations (stationId, operator_Id, stationStatus,label,description,address_Id,latitude,longitude,contactName,telephone,email,website,directions,greenEnergy,freeParking,priceUrl,public) VALUES (%s, %s,%s, %s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s,%s, %s,%s)"
        #print(sql)
        val = (self.stationId, self.Operator_ID, self.stationStatus, self.label, self.description, self.StreetID,self.latitude,self.longitude, 
        self.contactName,self.telephone,self.email, self.website,self.directions,self.greenEnergy,self.freeParking,self.priceUrl,
        self.public)
        mycursor.execute(sql,val)
        connector.commit()

    def AskCountOperator(self, connector):
            mycursor = connector.cursor()

            sql = "SELECT COUNT(*) FROM tbl_stations WHERE stationId = %s"
            val = (self.stationId,)
            mycursor.execute(sql,val)
            myresult_count = mycursor.fetchall()
            connector.commit()
            return(myresult_count)

    def ReturnStreetID(self, connector):
        mycursor = connector.cursor()
        if(self.street is None):
            sql_streetid = "SELECT Adress_ID FROM tbl_addr WHERE street IS NULL"
            mycursor.execute(sql_streetid)
            myresult_StreetID = mycursor.fetchall()
            connector.commit()
        else:
            sql_streetid = "SELECT Adress_ID FROM tbl_addr WHERE street=%s"
            val_streetid = (self.street,)
            mycursor.execute(sql_streetid,val_streetid)
            myresult_StreetID = mycursor.fetchall()
            #print(myresult_StreetID)
            connector.commit()

        return(myresult_StreetID)

    def ReturnOperatorID(self, connector):
        mycursor = connector.cursor()
        sql_operatorid = "SELECT Operator_ID FROM tbl_operators WHERE operatorId=%s"
        val_operatorid = (self.operater_id,)
        mycursor.execute(sql_operatorid,val_operatorid)
        myresult_OperatorID = mycursor.fetchall()
        connector.commit()
                
        return(myresult_OperatorID)

    def __del__(self):
        pass
        #Deconstrutor