from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, DateTime, Text
import sqlalchemy

Base = declarative_base()

class AlertStreamPayloads(Base):
    __tablename__ = 'alert_stream_payloads'

    # Column defs
    edc_alert_stream_id = Column(Integer, primary_key=True)
    topic = Column(String(255))
    url = Column(String(500))
    raw_payload = Text(String)
    date_received = Column(DateTime)
    science_stamp_url = Column(String(255))
    difference_stamp_url = Column(String(255))
    template_stamp_url = Column(String(255))

    def get_db_connection(db_host, db_port, db_name, db_user, db_pass):
        engine = sqlalchemy.create_engine("postgresql://{}:{}@{}:{}/{}".format(db_user, db_pass, db_host, db_port, db_name))
        engine.dialect.description_encoding = None
        Session = sessionmaker(bind=engine)
        return Session()