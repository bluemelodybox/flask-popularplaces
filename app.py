# app.py
from flask import Flask, request, jsonify
import populartimes
import os, json
from time import time
from datetime import timedelta
from glob import glob
from urllib.parse import urlparse
import redis

app = Flask(__name__)
url = urlparse(os.environ.get("REDISCLOUD_URL"))
r = redis.Redis(host=url.hostname, port=url.port, password=url.password)
API_KEY = os.environ["GOOGLE_API_KEY"]

# Display data collected
@app.route("/")
def index():

    keys = [k for k in r.scan_iter()]
    # More processing here
    return jsonify(keys)


# Get data from google api
@app.route("/api")
def api():

    places = [
        "ChIJjyjjwCAX2jERxYHvTxAw4X0",  # Bishan park
        "ChIJk_idN3oU2jEReqhHxnv3lgI",  # Chongpang market
        "ChIJMcwh6o0Z2jERNxsLqnSIvlw",  # Ion Orchard mall
        "ChIJP7z00McZ2jERJztQqXkRIC4",  # Mustafa centre
        "ChIJeRqAraYX2jERQpyIAXSU1SU",  # Nex shopping mall
    ]
    creation_time = int(time())  # Get time of crawl without milliseconds
    res = [populartimes.get_id(API_KEY, place) for place in places]
    res.append(creation_time)

    # if not r.exists("data_list"):
    #     data_list = []
    #     r.set(name="data_list", value=json.dumps(data_list))

    if not r.exists("last_created_time"):
        r.set(name="last_created_time", value=creation_time)
        r.setex(name=creation_time, time=timedelta(hours=2), value=json.dumps(res))
        # data_list = json.loads(r.get("data_list"))
        # data_list.append(creation_time)
    else:
        last_created_time = r.get("last_created_time")
        if creation_time - last_created_time < 900:
            return jsonify("Failed, time too recent")
        else:
            r.set(name="last_created_time", value=creation_time)
            r.setex(name=creation_time, time=timedelta(hours=2), value=json.dumps(res))

    return jsonify("Success")


if __name__ == "__main__":
    # Threaded option to enable multiple instances for multiple user access support
    app.run(threaded=True, port=5000)
