from antares_client import StreamingClient
from antares_client.search import get_by_id, search
import os, json, time, datetime
from google.cloud import logging
from google.cloud import storage
from elasticsearch_dsl import Search
from flask import Flask, request, Response
import sqlalchemy
import pickle
import julian

app = Flask(__name__)

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
TOPIC = os.environ["TOPIC"]
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

@app.route("/tasks/query-results")
def get_query_results():
    from_d = request.args.get("from")
    to_d = request.args.get("to")
    logger.log_text("/tasks/query-results/ triggered!!!")
    payload = {}
    logger.log_text("about to query from endpoint")

    if from_d == None or to_d == None:
        query_results = query_by_date_range(get_date_in_mjd(), get_date_in_mjd() - 5)
    else:
        query_results = query_by_date_range(validate_and_convert_input_date(to_d), validate_and_convert_input_date(from_d))
    
    logger.log_text("done querying")

    if query_results != None:
        logger.log_text("query results are NOT empty!!!")
        payload["result_count"] = len(query_results)
        for result in query_results:
            qr_json = jsonify_query_results(result)
            dest_fn = create_filename_from_locus_id(result.locus_id)
            qr_url = upload_file(dest_fn, qr_json)
            save_query_data(qr_url, qr_json, result.locus_id)
    else:
        logger.log_text("query results ARE empty!!!")
        payload["result_count"] = 0
    # res = json.dumps(payload)
    return payload, 200

# Format of input must be in the following format:
#
# MMDDYYYY
#
# Example: 07112022
def validate_and_convert_input_date(pre_date):
    if isinstance(pre_date, int):
        pre_date = str(pre_date)

    month = pre_date[0:2]
    day = pre_date[2:4]
    year = pre_date[4:]
    post_date = year + "-" + month + "-" + day
    d = datetime.fromisoformat(post_date)

    return round(julian.to_jd(d, fmt="mjd"))

def create_filename_from_locus_id(locus_id):
    return locus_id  +  "-" + str(round(time.time() * 1000)) + ".json"

def process_alert(topic, locus):
    logger.log_text("got an alert!")
    logger.log_text(json.dumps(topic))
    logger.log_text(json.dumps(locus.__dict__))

    destination_filename = create_filename_from_locus_id(locus.locus_id)

    if test_count == 0:
        query_by_id(locus.locus_id)

    alert = json.dumps(locus.__dict__)
    alert_url = upload_file(destination_filename, alert)
    save_alert_data(alert_url, alert)
    query_by_date_range(get_date_in_mjd(), get_date_in_mjd() - 5)

# mjd: modified julian date
def get_date_in_mjd():
    return round(julian.to_jd(datetime.datetime.today(), fmt="mjd"))

def query_by_date_range(to_d, from_d):
    global test_count, test_limit
    logger.log_text("about to perform elasticsearch query")
    logger.log_text("from: " + str(from_d) + " , to: " + str(to_d))
    query = (
        Search()
        .filter("range", **{"properties.newest_alert_observation_time": {"gte": from_d, "lte": to_d}})
        .filter("term", tags=TOPIC)
        .to_dict()
    )
    print("about to perform elasticsearch query")
    try:
        results = search(query)
        # print(type(results))
        test_count = 0 # reset test counter
        res_set = []
        for result in results:
            if test_count == test_limit:
                break
            res_set.append(result)
            print("logging result:")
            # print(type(result))
            print(result)
            test_count += 1
        # print("logging first_result:")
        # print(first_result)
        return res_set
    except Exception as e:
        print("an error occurred!!!")
        print(e.with_traceback)
        print(dir(e))
    return None

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
    logger.log_text("done logging query results")

    logger.log_text("about to log json object")
    qr_json = jsonify_query_results(buf)
    logger.log_text(qr_json)

    destination_filename = create_filename_from_locus_id(locus_id)
    qr_url = upload_file(destination_filename, qr_json)

    logger.log_text("about to save DB data")
    save_query_data(qr_url, qr_json, locus_id)
    logger.log_text("done saving DB data")

def jsonify_query_results(qr):
    json_qr = {}
    json_qr["ra"] = qr.ra
    json_qr["dec"] = qr.dec

    # Loop through arrays property and convert each Alert object to a dict
    alerts_arr = []
    for alert in qr.alerts:
        alerts_arr.append(alert.alert_id)
    json_qr["alerts"] = alerts_arr

    # Convert Panda DataFrame to dict
    # json_qr["lightcurve"] = qr.lightcurve.to_dict()
    
    json_qr["catalog_objects"] = qr.catalog_objects
    json_qr["catalogs"] = qr.catalogs
    json_qr["locus_id"] = qr.locus_id
    json_qr["properties"] = qr.properties
    json_qr["tags"] = qr.tags
    # json_qr["timeseries"] = qr.timeseries.as_array()
    json_qr["watch_list_ids"] = qr.watch_list_ids
    json_qr["watch_object_ids"] = qr.watch_object_ids

    return json.dumps(json_qr)
    
def save_query_data(qr_url, results, locus_id):
    db = init_connection_engine()
    stmt = sqlalchemy.text(
        "INSERT INTO alert_query_store (search_terms, url, raw_query_results)"
        "VALUES (:search_terms, :url, :raw_query_results)"
    )

    try:
        search_term = "search_by_id(" + locus_id + ")"
        with db.connect() as conn:
            row = conn.execute(stmt, search_terms=search_term, url=qr_url, raw_query_results=results)
            conn.close()
    except Exception as e:
        logger.log("an exception occurred!!!")
        logger.log_text(e)

def save_alert_data(alert_url, alert):
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

def upload_file(filename, content):
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
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    # main()
