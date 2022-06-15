from antares_client import StreamingClient
from statsmodels.stats.weightstats import DescrStatsW
import os, json
import numpy as np
from google.cloud import logging

# Instantiates the logging client
logging_client = logging.Client()
log_name = "alert-streaming-client"
logger = logging_client.logger(log_name)

TOPICS = ["in_m31_staging"]
CONFIG = {
    "api_key": os.environ["API_KEY"],
    "api_secret": os.environ["API_SECRET"],
}

def process_alert(topic, locus):
    logger.log_text("got an alert!")
    logger.log_text(json.dumps(topic))
    logger.log_text(json.dumps(locus.__dict__))

    """Put your code here!"""


def main():
    with StreamingClient(TOPICS, **CONFIG) as client:
        for topic, locus in client.iter():
            process_alert(topic, locus)


if __name__ == "__main__":
    main()
