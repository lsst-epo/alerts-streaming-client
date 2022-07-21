from antares_client import StreamingClient
from antares_client.search import get_by_id, search
import os, json, time, urllib.request
from google.cloud import logging
from google.cloud import storage
from elasticsearch_dsl import Search
from flask import Flask, request, Response
import pickle
from mjd_date_util import ModifiedJulianDateUtil as mjd
from alert_query_store import AlertQueryStore
from alert_stream_payloads import AlertStreamPayloads

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
    payload = {}

    if from_d == None or to_d == None:
        query_results, query = query_by_date_range(mjd.get_date_in_mjd(), mjd.get_date_in_mjd() - 5)
    else:
        query_results, query = query_by_date_range(mjd.validate_and_convert_input_date(to_d), mjd.validate_and_convert_input_date(from_d))
    
    if query_results != None:
        payload["result_count"] = len(query_results)
        for result in query_results:
            try:
                qr_json = jsonify_query_results(result)
                dest_fn = create_filename_from_locus_id(result.locus_id)
                qr_url = upload_file(dest_fn, qr_json)
                save_query_data(qr_url, qr_json, result.locus_id, query)
            except Exception as e:
                logger.log(e)
                logger.log_text(e)
                continue # keep processing 
    else:
        payload["result_count"] = 0
    return payload, 200

def create_filename_from_locus_id(locus_id):
    return locus_id  +  "-" + str(round(time.time() * 1000)) + ".json"

def process_alert(topic, locus):
    destination_filename = create_filename_from_locus_id(locus.locus_id)

    if test_count == 0:
        query_by_id(locus.locus_id)

    alert = json.dumps(locus.__dict__)
    alert_url = upload_file(destination_filename, alert)
    save_alert_data(alert_url, alert)
    query_by_date_range(mjd.get_date_in_mjd(), mjd.get_date_in_mjd() - 5)

def query_by_date_range(to_d, from_d):
    global test_count, test_limit
    query = (
        Search()
        .filter("range", **{"properties.newest_alert_observation_time": {"gte": from_d, "lte": to_d}})
        .filter("term", tags=TOPIC)
        .to_dict()
    )
    try:
        results = search(query)
        test_count = 0 # reset test counter
        res_set = []
        for result in results:
            if test_count == test_limit:
                break
            res_set.append(result)
            test_count += 1
        return res_set, query.__str__()
    except Exception as e:
        print("an error occurred!!!")
        print(e.with_traceback)
    return None

def query_by_id(locus_id):
    query_results = get_by_id(locus_id)
    f = open("/tmp/query_result.pkl", 'wb')
    pickle.dump(query_results, f)
    f.close()
    results_file = open('/tmp/query_result.pkl', 'rb') 

    buf =  pickle.load(results_file)

    results_file.close()
    qr_json = jsonify_query_results(buf)

    destination_filename = create_filename_from_locus_id(locus_id)
    qr_url = upload_file(destination_filename, qr_json)

    save_query_data(qr_url, qr_json, locus_id)

def jsonify_query_results(qr):
    json_qr = {}
    json_qr["ra"] = qr.ra
    json_qr["dec"] = qr.dec

    # Loop through arrays property and convert each Alert object to a dict
    alerts_arr = []
    alerts_limit = 1
    alerts_count = 0
    for alert in qr.alerts:
        alert_data = {}
        alert_data["alert_id"] = alert.alert_id
        alert_data["ztf_pid"] = alert.properties["ztf_pid"]
        alerts_arr.append(alert_data)

        if alerts_count < alerts_limit:
            if alert.alert_id[0:15] != "ztf_upper_limit":
                candidate_id = alert.alert_id[alert.alert_id.find(':')+1:]
                sci_stamp_url = None
                diff_stamp_url = None
                templ_stamp_url = None

                # Science stamp
                try:
                    sci_stamp = f"https://storage.googleapis.com/antares-production-ztf-stamps/candid{candidate_id}_pid{candidate_id[0:12]}_targ_sci.fits.png"
                    sci_res = urllib.request.urlopen(sci_stamp)
                    sci_bytes = sci_res.read()
                    sci_stamp_url = upload_file(f"candid{candidate_id}_pid{candidate_id[0:12]}_targ_sci.fits.png", sci_bytes)
                except Exception as e:
                    logger.log_text("Could not fetch the science stamp for " + alert.alert_id)
                    logger.log_text(e)

                # Diff stamp
                try:
                    diff_stamp = f"https://storage.googleapis.com/antares-production-ztf-stamps/candid{candidate_id}_pid{candidate_id[0:12]}_targ_diff.fits.png"
                    diff_res = urllib.request.urlopen(diff_stamp)
                    diff_bytes = diff_res.read()
                    diff_stamp_url = upload_file(f"candid{candidate_id}_pid{candidate_id[0:12]}_targ_diff.fits.png", diff_bytes)
                except Exception as e:
                    logger.log_text("Could not fetch the difference stamp for " + alert.alert_id)
                    logger.log_text(e)

                # Template stamp
                try:
                    templ_stamp = f"https://storage.googleapis.com/antares-production-ztf-stamps/candid{candidate_id}_ref.fits.png"
                    templ_res = urllib.request.urlopen(templ_stamp)
                    templ_bytes = templ_res.read()
                    templ_stamp_url = upload_file(f"candid{candidate_id}_pid{candidate_id[0:12]}_targ_diff.fits.png", templ_bytes)
                except Exception as e:
                    logger.log_text("Could not fetch the template stamp for " + alert.alert_id)
                    logger.log_text(e)

                # Upload alert
                destination_filename = create_filename_from_locus_id(alert.alert_id)
                alert_json = json.dumps(alert.__dict__)
                alert_json_url = upload_file(destination_filename, alert_json)

                # Persist date
                save_alert_data(alert_json_url, alert_json, sci_stamp_url, diff_stamp_url, templ_stamp_url)
                alerts_count += 1
       
    json_qr["alerts"] = alerts_arr
    json_qr["catalog_objects"] = qr.catalog_objects
    json_qr["catalogs"] = qr.catalogs
    json_qr["locus_id"] = qr.locus_id
    json_qr["properties"] = qr.properties
    json_qr["tags"] = qr.tags
    json_qr["watch_list_ids"] = qr.watch_list_ids
    json_qr["watch_object_ids"] = qr.watch_object_ids

    return json.dumps(json_qr)
    
def save_query_data(qr_url, results, locus_id, query):
    try:
        db = AlertQueryStore.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
        alert_query_record = AlertQueryStore(search_terms = query, url=qr_url, raw_query_results=results)
        db.add(alert_query_record)
    except Exception as e:
        logger.log_text("An exception occurred while attempting to insert new records in the ALERT_QUERY_STORE database:")
        logger.log_text(e)   

def save_alert_data(alert_url, alert, sci_stamp = None, diff_stamp = None, templ_stamp = None):
    try:
        db = AlertStreamPayloads.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
        alert_stream_record = AlertStreamPayloads(topic=TOPICS[0], url=alert_url, raw_payload=alert, science_stamp_url=sci_stamp, difference_stamp_url=diff_stamp, template_stamp_url=templ_stamp)
        db.add(alert_stream_record)
    except Exception as e:
        logger.log_text("An exception occurred while attempting to insert new records in the ALERT_STREAM_PAYLOADS database:")
        logger.log_text(e)  

def upload_file(filename, content):
    blob = bucket.blob(filename)
    blob.upload_from_string(content)

    return blob.self_link

def main():
    global test_count, test_limit
    with StreamingClient(TOPICS, **CONFIG) as client:
        for topic, locus in client.iter():
            if test_count < test_limit: # testing limits
                process_alert(topic, locus)
                test_count += 1 # testing limits

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
