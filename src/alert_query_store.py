from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, DateTime, Text
import sqlalchemy

Base = declarative_base()

class AlertQueryStore(Base):
    __tablename__ = 'alert_query_store'

    # Column defs
    edc_alert_query_id = Column(Integer, primary_key=True)
    search_terms = Column(String(512))
    url = Column(String(255))
    raw_query_results = Text(String)
    date_created = Column(DateTime)

    def get_db_connection(db_host, db_port, db_name, db_user, db_pass):
        engine = sqlalchemy.create_engine("postgresql://{}:{}@{}:{}/{}".format(db_user, db_pass, db_host, db_port, db_name))
        engine.dialect.description_encoding = None
        Session = sessionmaker(bind=engine)
        return Session()