from antares_client import StreamingClient
from statsmodels.stats.weightstats import DescrStatsW
import os, json, time
import numpy as np
from google.cloud import logging
from google.cloud import storage

CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']

# Instantiates the logging client
logging_client = logging.Client()
log_name = "alert-streaming-client"
logger = logging_client.logger(log_name)

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
    blob = bucket.blob(destination_filename)

    alert = json.dumps(locus.__dict__)
    
    blob.upload_from_string(alert)

def main():
    global test_count, test_limit
    with StreamingClient(TOPICS, **CONFIG) as client:
        for topic, locus in client.iter():
            if test_count < test_limit: # testing limits
                process_alert(topic, locus)
                test_count += 1 # testing limits

            


if __name__ == "__main__":
    main()
