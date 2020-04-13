# app.py
from flask import Flask, request, jsonify
import populartimes
import os, json
from time import time
from datetime import datetime
from glob import glob

app = Flask(__name__)

# Display data collected
@app.route("/")
def index():
    files = glob("./data/*.json")
    files.sort(key=os.path.getmtime)
    data = []
    for fi in files:
        with open(fi) as f:
            d = json.load(f)
        data.append(d)
    # More processing here
    return jsonify(data)


# Get data from google api
@app.route("/api")
def api():
    API_KEY = os.environ["GOOGLE_API_KEY"]
    places = [
        "ChIJjyjjwCAX2jERxYHvTxAw4X0",  # Bishan park
        "ChIJk_idN3oU2jEReqhHxnv3lgI",  # Chongpang market
        "ChIJMcwh6o0Z2jERNxsLqnSIvlw",  # Ion Orchard mall
        "ChIJP7z00McZ2jERJztQqXkRIC4",  # Mustafa centre
        "ChIJeRqAraYX2jERQpyIAXSU1SU",  # Nex shopping mall
    ]
    creation_time = int(time())  # Get time of crawl
    res = [populartimes.get_id(API_KEY, place) for place in places]
    res.append(datetime.fromtimestamp(creation_time))

    files = glob("./data/*.json")
    files.sort(key=os.path.getmtime)  # Earliest creation date first
    if len(files) > 8:
        os.remove(files[0])  # Remove earliest data
        del files[0]
    with open(f"./data/{creation_time}.json") as f:
        json.dump(res, f)
    return jsonify("Success")


if __name__ == "__main__":
    # Threaded option to enable multiple instances for multiple user access support
    app.run(threaded=True, port=5000)
