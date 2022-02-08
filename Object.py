### Class -object



from itertools import count
from turtle import st




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
        self.organization = organization,
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
        self.deviatingOperatorIds = deviatingOperatorIds
        self.status = status
        self.StreetID = ""

    def UpdateSQLOperator(self,connector,TabelleInto):
        mycursor = connector.cursor()
        sql = "INSERT INTO {tab} (operatorId,type,organization,commercialRegisterNumber,sex,titlePrefix,firstName,lastName,titleSuffix,street,website,deviatingOperatorIds,status) VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s)".format(tab = TabelleInto)

        val = (self.operatorID, self.type, self.organization,self.commercialRegisterNumber,self.sex,self.titlePrefix,self.firstName, 
        self.lastName,self.titleSuffix,self.StreetID, self.StreetID,self.website,self.deviatingOperatorIds,self.status)
        mycursor.execute(sql,val)
        connector.commit()
    def __del__(self):
        pass
        #Deconstrutor

class Station:
    species = "Station Values"
    def __init__(self,stationId,stationStatus,label,description,postCode,city,street,latitude,longitude,
    contactName,telephone,email,website,directions,greenEnergy,freeParking,priceUrl,public,openingHours,openingHoursdetails,
    Operater_id):
        self.stationId = stationId
        self.stationStatus = stationStatus
        self.label = label,
        self.description = description
        self.postCode = postCode
        self.city = city
        self.street=street
        self.latitude=latitude
        self.longitude = longitude
        self.contactName = contactName
        self.telephone = telephone
        self.email = email
        self.directions=directions
        self.website = website
        self.greenEnergy = greenEnergy
        self.freeParking = freeParking
        self.priceUrl = priceUrl
        self.public = public
        self.openingHours = openingHours
        self.openingHoursdetails = openingHoursdetails
        self.Operater_id = Operater_id
        self.Operator_ID = ""
        self.StreetID = ""

    def UpdateSQLStation(self,connector, TabelleInto):
        mycursor = connector.cursor()
        sql = "INSERT INTO {tab} (stationId,stationStatus,label,description,street,latitude,longitude,contactName,telephone,email,website,directions,greenEnergy,freeParking,priceUrl,public,openingHours,openingHoursdetails,Operater_id) VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s,%s, %s,%s,%s)".format(tab = TabelleInto)

        val = (self.stationId, self.stationStatus, self.label,self.description,self.StreetID,self.latitude,self.longitude, 
        self.contactName,self.telephone,self.email, self.website,self.directions,self.greenEnergy,self.freeParking,self.priceUrl,
        self.public,self.openingHours,self.openingHoursdetails,self.Operater_id)
        mycursor.execute(sql,val)
        connector.commit()
    def __del__(self):
        pass
        #Deconstrutor