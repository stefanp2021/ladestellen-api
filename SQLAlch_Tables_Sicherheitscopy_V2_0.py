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


"""
##from curses import meta
from ast import Add, keyword
from itertools import count
import os
from random import seed
from venv import create
from flask import session
import numpy as np
import pandas as pd
#import mysql.connector
import copy
import sys
import tqdm
import sqlalchemy
import pymysql
import function_own
import hashlib



##################################
from tqdm import tqdm 

import copy
from importlib.metadata import metadata
from pandas import cut, json_normalize
from pathlib import Path
from dataclasses import dataclass
from pymysql import NULL


from sqlalchemy.orm import declarative_base, sessionmaker, relationship, backref
from sqlalchemy import VARCHAR, Column,String,DateTime,Integer,create_engine, ForeignKey, PrimaryKeyConstraint,Sequence,Float,Boolean, UniqueConstraint, MetaData,select, func
from datetime import datetime
from sqlalchemy.sql import *
from requests.auth import HTTPBasicAuth

#from function_own import func_GetRidofNone

from function_own import func_GetRidofNone

#https://stackoverflow.com/questions/40083753/sqlalchemy-creating-view-with-orm
# installation: pip install sqlalchemy-utils
#import sqlalchemy_utils
#from sqlalchemy_utils import create_view, create_materialized_view

from sqlalchemy_utils import create_materialized_view, create_view,refresh_materialized_view, database_exists, view


"""

# Engine
engine = create_engine("mysql+pymysql://ladestellen:ybV1NfB0sCrzWS22hzOiMZ7YwkmtIwMT@192.168.10.21/ladestellen")
#Base
Base = declarative_base()
#Base.metadata.create_all(engine)

#session class
Session = sessionmaker()
#date_create = Column(DateTime(),default=datetime.now())

# Now we create the Database

class Country(Base):
    __tablename__="tbl_country"
    country_ID = Column(Integer,Sequence('country_id_seq'), primary_key=True,autoincrement=True)#
    Country = Column(VARCHAR(4),nullable=False) # Darf nicht Null sein
    

    def __init__(self,country):
        self.Country = country
        #self.country_ID = ""

    #country_ID = ""

    #plzs = relationship('PLZ', backref='PLZ.country_Id',primaryjoin='Country.country_ID==PLZ.country_Id', lazy='dynamic')


class PLZ(Base):
    __tablename__="tbl_plz"
    __table_args__ = (UniqueConstraint('city', 'postCode', name='location_full'),)
    PLZ_ID = Column(Integer(), Sequence('plz_id_seq'),primary_key=True,autoincrement=True)
    city = Column(VARCHAR(250),nullable=True) # Darf Null sein
    postCode = Column(Integer(), nullable=True) # Darf Null sein
    country_Id = Column(Integer(), ForeignKey(Country.country_ID))
    

    def __init__(self, city, postCode, country):
        self.city = city
        self.postCode = postCode
        #self.country_Id = ""
        #self.PLZ_ID = ""
        self.country = country

    
    countries = relationship("Country",backref=backref("tbl_plz"))
    #friends = relationship('PLZ', backref='PLZ.country_Id',primaryjoin='Country.country_ID==PLZ.country_Id', lazy='dynamic')

    #country = relationship('Country', foreign_keys='PLZ.country_Id') #,overlaps="PLZ.country_Id,plzs"
    
    #addresses = relationship('PLZ', backref='PLZ.country_Id',primaryjoin='Country.country_ID==PLZ.country_Id', lazy='dynamic')



class Address(Base):
    __tablename__="tbl_addr"
    Address_ID = Column(Integer(), primary_key=True,autoincrement=True)
    street = Column(VARCHAR(400),nullable=True) # Darf Null sein
    Plz_Id = Column(Integer(), ForeignKey(PLZ.PLZ_ID))
    

    def __init__(self,street, plz):
        self.street = street
        #self.Address_ID = ""
        #self.Plz_Id = ""
        self.plz = plz

    plzs = relationship("PLZ",backref=backref("tbl_addr"))
    #plz = relationship('PLZ', foreign_keys='Address.Plz_Id')

    #operators = relationship('Operators', backref='Address.Address_ID',primaryjoin='Address.Address_ID==Operators.address_Id', lazy='dynamic')
    #stations = relationship('Stations', backref='Address.Address_ID',primaryjoin='Address.Address_ID==Stations.address_Id', lazy='dynamic')


class OrgType(Base):
    __tablename__="tbl_OrgType"
    Org_ID = Column(Integer(), primary_key=True,autoincrement=True)
    OrgType = Column(VARCHAR(50),nullable=False) # Darf nicht Null sein
    

    def __init__(self,type):
        self.OrgType = type
        self.Org_ID = ""



    #operators = relationship('Operators', backref='OrgType.Org_ID',primaryjoin='OrgType.Org_ID==Operators.type_Id', lazy='dynamic')



class Operators(Base):
    __tablename__="tbl_operators"
    Operators_ID = Column(Integer(), primary_key=True,autoincrement=True)
    operator_code = Column(VARCHAR(100), nullable=True)
    address_Id =  Column(Integer(), ForeignKey(Address.Address_ID))
    type_Id =  Column(Integer(), ForeignKey(OrgType.Org_ID))
    organization = Column(VARCHAR(100),nullable=True)
    commercialRegisterNumber = Column(VARCHAR(100),nullable=True)
    sex = Column(VARCHAR(100),nullable=True)
    titlePrefix = Column(VARCHAR(100),nullable=True)
    firstName = Column(VARCHAR(100),nullable=True)
    lastName = Column(VARCHAR(100),nullable=True)
    titleSuffix = Column(VARCHAR(100),nullable=True)
    website = Column(VARCHAR(100),nullable=True)
    deviatingOperatorIds = Column(VARCHAR(100),nullable=True)
    status = Column(VARCHAR(100),nullable=True)
    OrgLabel = Column(VARCHAR(300),nullable=True)

    

    def __init__(self,operatorcode,type,organization,commercialRegisterNumber,sex,
    titlePrefix,firstName,lastName,titleSuffix,country,postCode,city,street,
    website,status):
        self.operatorcode = operatorcode
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
        #self.StreetID = ""
        #self.PLZID=""
        #self.TypeID=""
        #self.OLabel = "{o} {f} {l}".format(o = func_GetRidofNone(self.organization), f=func_GetRidofNone(self.firstName), l=func_GetRidofNone(self.lastName))

    opertype = relationship("OrgType",backref=backref("tbl_operators"))
    address = relationship("Address",backref=backref("tbl_operators"))

    #stations = relationship('Stations', backref='Operators.Operators_ID',primaryjoin='Operators.Operators_ID==Stations.operator_Id', lazy='dynamic')

    #addresse = relationship('Address', foreign_keys='Operators.address_Id')
    #orgtype = relationship('OrgType', foreign_keys='Operators.type_Id')


class Stations(Base):
    __tablename__="tbl_stations"
    Station_ID = Column(Integer(), primary_key=True,autoincrement=True)
    stationId = Column(VARCHAR(45), nullable=True)
    operator_Id =  Column(Integer(), ForeignKey(Operators.Operators_ID))
    stationStatus = Column(VARCHAR(45), nullable=True)
    label = Column(VARCHAR(2000), nullable=True)
    description = Column(VARCHAR(2000), nullable=True)
    address_Id =  Column(Integer(), ForeignKey(Address.Address_ID))
    latitude = Column(VARCHAR(45),nullable=True)
    longitude = Column(VARCHAR(45),nullable=True)
    contactName = Column(VARCHAR(2000),nullable=True)
    telephone = Column(VARCHAR(45),nullable=True)
    email = Column(VARCHAR(200),nullable=True)
    website = Column(VARCHAR(200),nullable=True)
    directions = Column(VARCHAR(2000),nullable=True)
    greenEnergy = Column(VARCHAR(45),nullable=True)
    freeParking = Column(VARCHAR(45),nullable=True)
    priceUrl = Column(VARCHAR(500),nullable=True)
    public = Column(VARCHAR(500),nullable=True)

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
            #self.Operator_ID = ""
            #self.StreetID = ""
            #self.PLZID=""

    #addresse = relationship('Address', foreign_keys='Stations.address_Id')
    #operators = relationship('Operators', foreign_keys='Stations.operator_Id')
    operator = relationship("Operators",backref=backref("tbl_stations"))
    address = relationship("Address",backref=backref("tbl_stations"))

Base.metadata.create_all(engine)


local_session = Session(bind=engine)
list_countries_start = ["AT","DE"]

for i in range(len(list_countries_start)):
    countrycount =  local_session.query(Country).filter(Country.Country == list_countries_start[i]).first()
    if(countrycount is None):
        c1 = Country(country= list_countries_start[i])   
        local_session.add(c1)
    else:
        pass

local_session.commit()

###################### View


"""
class OperatorView(Base):
    __table__ = create_view(
        name='vw_ladestellen_per_operator',
        selectable=sqlalchemy.select(
            [
                Operators.Operators_ID.label("man_Operator_Id"),
                Operators.operator_code.label("rec_operatorid"),
                Operators.OrgLabel.label("OLabel"),
                Stations.Station_ID.label("stationId"),
                Stations.label.label("stations_label"),
                Stations.description.label("stations_description"),
                Address.street.label("operator_street"),
                PLZ.postCode.label("operator_postCode"),
                PLZ.city.label("operator_city"),
                Operators.website,
                OrgType.OrgType.label("Organ")
            ]
        ),
        metadata=Base.metadata,
        cascade_on_drop=True,
        attr
    )



"""



"""
class OperatorView(Base):
    __table__ = create_view(
        name='vw_ladestellen_per_operator',
        selectable=sqlalchemy.select(
            [
                Operators.Operators_ID.label("man_Operator_Id"),
                Operators.operator_code.label("rec_operatorid"),
                Operators.OrgLabel.label("OLabel"),
                Stations.Station_ID.label("stationId"),
                Stations.label.label("stations_label"),
                Stations.description.label("stations_description"),
                Address.street.label("operator_street"),
                PLZ.postCode.label("operator_postCode"),
                PLZ.city.label("operator_city"),
                Operators.website,
                OrgType.OrgType.label("Organ")
            ]
        ),
        metadata=Base.metadata,
        cascade_on_drop=True,
    )

Base.metadata.create_all(engine)

"""





















































































# Funktionier
"""


class OperatorView(Base):
    __table__ = create_view(
        name='vw_ladestellen_per_operator',
        selectable=sqlalchemy.select(
            [
                Operators.Operators_ID.label("man_Operator_Id"),
                Operators.operator_code.label("rec_operatorid"),
                Operators.OrgLabel.label("OLabel"),
                Stations.Station_ID.label("stationId"),
                Stations.label.label("stations_label"),
                Stations.description.label("stations_description"),
                Address.street.label("operator_street"),
                PLZ.postCode.label("operator_postCode"),
                PLZ.city.label("operator_city"),
                Operators.website,
                OrgType.OrgType.label("Organ")

            ]
        ),
        metadata=Base.metadata,
    )
views = [OperatorView]



Base.metadata.create_all(engine)






"""












"""
class OperatorView(Base):
    __table__ = create_view(
        name='op_view',
        selectable=sqlalchemy.select(
            [
                Operators.Operators_ID.label("man_Operator_Id"),
                Operators.operator_code.label("rec_operatorid"),
                Operators.OrgLabel.label("OLabel"),
                Stations.Station_ID.label("stationId"),
                Stations.label.label("stations_label"),
                Stations.description.label("stations_description"),
                Address.street.label("operator_street"),
                PLZ.postCode.label("operator_postCode"),
                PLZ.city.label("operator_city"),
                Operators.website,
                OrgType.OrgType.label("Organ")

            ],
            from_obj=(
                #Operators.__table__.join(Stations, Operators.Operators_ID == Stations.operator_Id),
                #Operators.__table__.join(Address, Operators.address_Id == Address.Address_ID),
                #Address.__table__.join(PLZ, Address.Plz_Id == PLZ.PLZ_ID),
                #Operators.__table__.join(OrgType, Operators.type_Id == OrgType.Org_ID)

                Operators.__table__.join(Stations, Operators.Operators_ID == Stations.operator_Id),
                Address.__table__.join(Operators, Operators.address_Id == Address.Address_ID),
                PLZ.__table__.join(Address, Address.Plz_Id == PLZ.PLZ_ID),
                OrgType.__table__.join(Operators, Operators.type_Id == OrgType.Org_ID)

            )
        ),
        metadata=Base.metadata
    )"""

#https://stackoverflow.com/questions/9766940/how-to-create-an-sql-view-with-sqlalchemy
"""
class OperatorView(Base):
    __table__ = create_view(
        name='vw_ladestellen_per_operator',
        selectable=sqlalchemy.select(
            [
                Operators.Operators_ID.label("man_Operator_Id"),
                Operators.operator_code.label("rec_operatorid"),
                Operators.OrgLabel.label("OLabel"),
                Stations.Station_ID.label("stationId"),
                Stations.label.label("stations_label"),
                Stations.description.label("stations_description"),
                Address.street.label("operator_street"),
                PLZ.postCode.label("operator_postCode"),
                PLZ.city.label("operator_city"),
                Operators.website,
                OrgType.OrgType.label("Organ")

            ]
        ),
        metadata=Base.metadata,
    )
views = [OperatorView]
#OperatorView.__table__.drop(engine)
#for view in views:
#    create(view)
"""
