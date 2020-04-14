# app.py
from flask import Flask, request, jsonify
import populartimes
import os, json
from time import time
from datetime import timedelta, datetime
from urllib.parse import urlparse
import redis

app = Flask(__name__)
url = urlparse(os.environ.get("REDISCLOUD_URL"))
r = redis.Redis(host=url.hostname, port=url.port, password=url.password)
API_KEY = os.environ["GOOGLE_API_KEY"]


@app.route("/")
def index():
    return "<h1>Popularplaces API<h1>"


# Display data collected
@app.route("/data/")
def display_data():

    keys = [
        int(k.decode("utf-8"))
        for k in r.scan_iter()
        if k.decode("utf-8") != "last_created_time"
    ]
    keys.sort()  # Earliest date first
    print(keys)
    data = [json.loads(r.get(str(k)).decode("utf-8")) for k in keys]
    # More processing here
    return jsonify(
        {"mapData": data, "lastUpdatedTime": datetime.fromtimestamp(data[-1][-1])}
    )


# Get data from google api
@app.route("/fetch/")
def fetch_data():

    places = [
        "ChIJjyjjwCAX2jERxYHvTxAw4X0",  # Bishan park
        "ChIJk_idN3oU2jEReqhHxnv3lgI",  # Chongpang market
        "ChIJMcwh6o0Z2jERNxsLqnSIvlw",  # Ion Orchard mall
        "ChIJP7z00McZ2jERJztQqXkRIC4",  # Mustafa centre
        "ChIJeRqAraYX2jERQpyIAXSU1SU",  # Nex shopping mall
    ]
    creation_time = int(time())  # Get time of crawl without milliseconds
    if not r.exists("last_created_time"):
        res = [populartimes.get_id(API_KEY, place) for place in places]
        res.append(creation_time)
        r.set(name="last_created_time", value=creation_time)
        r.setex(name=creation_time, time=timedelta(hours=2), value=json.dumps(res))
    else:
        last_created_time = int(r.get("last_created_time").decode("utf-8"))
        if creation_time - last_created_time < 720:
            return jsonify("Failed, time too recent")
        else:
            res = [populartimes.get_id(API_KEY, place) for place in places]
            res.append(creation_time)
            r.set(name="last_created_time", value=creation_time)
            r.setex(name=creation_time, time=timedelta(hours=2), value=json.dumps(res))

    return jsonify("Success")


if __name__ == "__main__":
    # Threaded option to enable multiple instances for multiple user access support
    app.run(threaded=True, port=5000)
