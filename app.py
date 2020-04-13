# app.py
from flask import Flask, request, jsonify
import populartimes
import os, json
from time import time
from datetime import timedelta
from glob import glob
import redis

app = Flask(__name__)
r = redis.from_url(os.environ.get("REDIS_URL"))
API_KEY = os.environ["GOOGLE_API_KEY"]

# Display data collected
@app.route("/")
def index():

    keys = [k for k in  in r.scan_iter()]

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
    creation_time = int(time())  # Get time of crawl
    res = [populartimes.get_id(API_KEY, place) for place in places]
    res.append(creation_time)
    r.setex(str(creation_time), timedelta(hours=2), value=res)

    return jsonify("Success")


if __name__ == "__main__":
    # Threaded option to enable multiple instances for multiple user access support
    app.run(threaded=True, port=5000)
