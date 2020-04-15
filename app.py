# app.py
from flask import Flask, jsonify
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


def get_color(name, trend):
    if len(trend[name][-3:]) == 3:
        t0, t1, t2 = trend[name][-3:]
        print(name, t0, t1, t2)
        if t0 >= t1 >= t2:  # Croud decreasing
            return "#228B22"  # Green color
        elif t0 <= t1 <= t2:  # Croud increasing
            return "#DC143C"  # Red color
        else:
            return "#0088cc"
    else:
        return "#0088cc"


def normalize_size(current_pop):
    if current_pop == 0:
        return 0
    return (current_pop / 100) + 0.5


@app.route("/")
def index():
    return "<h1>Popularplaces API<h1>"


# @app.route("/delete/")
# def delete():
#     for k in r.scan_iter():
#         r.delete(k)
#     return jsonify("deleted")


@app.route("/raw/")
def raw_data():
    keys = [
        int(k.decode("utf-8"))
        for k in r.scan_iter()
        if k.decode("utf-8") != "last_created_time"
    ]
    keys.sort()  # Earliest date first
    data = [json.loads(r.get(str(k)).decode("utf-8")) for k in keys]
    return jsonify(data)


# Display data collected
@app.route("/data/")
def display_data():

    keys = [
        int(k.decode("utf-8"))
        for k in r.scan_iter()
        if k.decode("utf-8") != "last_created_time"
    ]
    keys.sort()  # Earliest date first
    data = [json.loads(r.get(str(k)).decode("utf-8")) for k in keys]

    trend = {location["name"]: [] for location in data[-1]}
    for t in data:
        for location in t:
            if trend.get(location["name"]) != None:
                trend[location["name"]].append(location.get("current_popularity", 0))

    map_data = [
        {
            "title": location["name"],
            "latitude": location["coordinates"]["lat"],
            "longitude": location["coordinates"]["lng"],
            "size": normalize_size(location.get("current_popularity", 0)),
            "color": get_color(location["name"], trend),
        }
        for location in data[-1]
    ]

    line_data = [
        {
            "location": k,
            "popularity": [{"popularity": hour} for hour in v],
            "current": v[-1],
            "previous": v[-2],
        }
        for i, (k, v) in enumerate(trend.items())
    ]
    return jsonify(
        {
            "mapData": map_data,
            "lastUpdatedTime": datetime.fromtimestamp(keys[-1]),
            "lineData": line_data,
        }
    )


if __name__ == "__main__":
    # Threaded option to enable multiple instances for multiple user access support
    app.run(threaded=True, port=5000)
