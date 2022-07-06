from antares_client import StreamingClient
from antares_client.search import get_by_id
from statsmodels.stats.weightstats import DescrStatsW
import os, json, time
import numpy as np
from google.cloud import logging
from google.cloud import storage
import sqlalchemy
import pickle

CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']

# Instantiates the logging client
logging_client = logging.Client()
log_name = "alert-streaming-client"
logger = logging_client.logger(log_name)

DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASS"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_NAME = os.environ["DB_NAME"]
TOPICS = [os.environ["TOPIC"]]
CONFIG = {
    "api_key": os.environ["API_KEY"],
    "api_secret": os.environ["API_SECRET"]
}

# For test purposes, prevents massive amounts of alerts from being stored
test_limit = 10
test_count = 0
# End of test vars

gcs = storage.Client()
bucket = gcs.bucket(CLOUD_STORAGE_BUCKET)

def process_alert(topic, locus):
    logger.log_text("got an alert!")
    logger.log_text(json.dumps(topic))
    logger.log_text(json.dumps(locus.__dict__))

    destination_filename = locus.locus_id  +  "-" + str(round(time.time() * 1000)) + ".json"

    if test_count == 0:
        query_by_id(locus.locus_id)

    alert = json.dumps(locus.__dict__)
    alert_url = upload_alert(destination_filename, alert)
    save_relational_data(alert_url, alert)

def query_by_id(locus_id):
    logger.log_text("locus_id : " + locus_id)

    query_results = get_by_id(locus_id)
    f = open("/tmp/query_result.pkl", 'wb')
    pickle.dump(query_results, f)
    f.close()
    results_file = open('/tmp/query_result.pkl', 'rb') 
    

    buf =  pickle.load(results_file)
    logger.log_text("about to log results file properties:")
    logger.log_text("q.alerts : " + str(buf.alerts))
    logger.log_text("q.catalog_objects : " + str(buf.catalog_objects))
    logger.log_text("q.catalogs : " + str(buf.catalogs))
    logger.log_text("q.coordinates : " + str(buf.coordinates))
    logger.log_text("q.dec : " + str(buf.dec))
    logger.log_text("q.ra : " + str(buf.ra))
    logger.log_text("q.properties : " + str(buf.properties))
    logger.log_text("q.tags : " + str(buf.tags))

    logger.log_text("q.lightcurve : " + str(buf.lightcurve))
    logger.log(buf.lightcurve)

    results_file.close()
    logger.log_text("about to log query results")
    
def save_relational_data(alert_url, alert):
    db = init_connection_engine()
    stmt = sqlalchemy.text(
        "INSERT INTO alert_stream_payloads (topic, url, raw_payload)"
        "VALUES (:topic, :url, :raw_payload)"
    )

    try:
        with db.connect() as conn:
            row = conn.execute(stmt, topic=TOPICS[0], url=alert_url, raw_payload=alert)
            conn.close()
    except Exception as e:
        logger.log(e)

def upload_alert(filename, content):
    blob = bucket.blob(filename)
    blob.upload_from_string(content)
    
    logger.log_text("logging blob.self_link:")
    logger.log_text(blob.self_link)
    logger.log_text("done logging")
    return blob.self_link


def main():
    global test_count, test_limit
    with StreamingClient(TOPICS, **CONFIG) as client:
        for topic, locus in client.iter():
            if test_count < test_limit: # testing limits
                process_alert(topic, locus)
                test_count += 1 # testing limits

def init_connection_engine():
    db_config = {
        # [START cloud_sql_postgres_sqlalchemy_limit]
        # Pool size is the maximum number of permanent connections to keep.
        "pool_size": 5,
        # Temporarily exceeds the set pool_size if no connections are available.
        "max_overflow": 2,
        # The total number of concurrent connections for your application will be
        # a total of pool_size and max_overflow.
        # [END cloud_sql_postgres_sqlalchemy_limit]

        # [START cloud_sql_postgres_sqlalchemy_backoff]
        # SQLAlchemy automatically uses delays between failed connection attempts,
        # but provides no arguments for configuration.
        # [END cloud_sql_postgres_sqlalchemy_backoff]

        # [START cloud_sql_postgres_sqlalchemy_timeout]
        # 'pool_timeout' is the maximum number of seconds to wait when retrieving a
        # new connection from the pool. After the specified amount of time, an
        # exception will be thrown.
        "pool_timeout": 30,  # 30 seconds
        # [END cloud_sql_postgres_sqlalchemy_timeout]

        # [START cloud_sql_postgres_sqlalchemy_lifetime]
        # 'pool_recycle' is the maximum number of seconds a connection can persist.
        # Connections that live longer than the specified amount of time will be
        # reestablished2
        "pool_recycle": 1800,  # 30 minutes
        # [END cloud_sql_postgres_sqlalchemy_lifetime]
    }


    return init_tcp_connection_engine(db_config)

def init_tcp_connection_engine(db_config):
    pool = sqlalchemy.create_engine("postgresql://{}:{}@{}:{}/{}".format(DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME))
    pool.dialect.description_encoding = None
    return pool

if __name__ == "__main__":
    main()
