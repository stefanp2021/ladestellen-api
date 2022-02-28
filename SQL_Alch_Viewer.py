import sqlalchemy_utils
from sqlalchemy_utils import create_view
from SQLAlch_Tables import Session,engine,Base,Country, PLZ, Address,OrgType,Operators,Stations



#stackoverflow.com/questions/6044309/sqlalchemy-how-to-join-several-tables-by-one-query
#tutorials.com/sqlalchemy/sqlalchemy_orm_working_with_joins.htm


local_session = Session(bind=engine)

class ViewerLadestellenOp(Base):
    #_query=select(["tbl_operators","tbl_stations"])
    _query=local_session([Operators,OrgType])



    _query = local_session.query()
    __tablename__="new_view"
    __table__ = create_view(__tablename__,_query,Base.metadata)

    #id = column_property(user_table.c.id, address_table.c.user_id)
    #address_id = address_table.c.id
try:
    Base.metadata.create_all(engine)
except:
    print("already exists")
    pass


