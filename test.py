from antares_client import StreamingClient
from antares_client.search import get_by_id, search
import os, json, time
from elasticsearch_dsl import Search
import pickle

TOPIC = "high_amplitude_variable_star_candidate_staging"
API_KEY = "rubinepo"
API_SECRET = "lpwRtuQxCH9eKjaFAjZ21CklAVarj23zsHVIih8EnHvKQQrGqKE6g5wRcpRSWuJm"

CONFIG = {
    "api_key": API_KEY,
    "api_secret": API_SECRET
}

# For test purposes, prevents massive amounts of alerts from being stored
test_limit = 10
test_count = 0
# End of test vars

def query_by_date_range():
    query = (
        Search()
        .filter("range", **{"properties.newest_alert_observation_time": {"gte": 59767, "lte": 59772}})
        .filter("term", tags="high_amplitude_variable_star_candidate")
        .filter("limit", value=1)
        .to_dict()
    )
    print("about to perform elasticsearch query")
    try:
        results = search(query)
        # print(type(results))

        for result in results:
            print("logging result:")
            # print(type(result))
            print(result)
        # print("logging first_result:")
        # print(first_result)
    except Exception as e:
        print("an error occurred!!!")
        print(e.__str__())
        # print(dir(e))
    return

def main():
    query_by_date_range()


if __name__ == "__main__":
    main()
