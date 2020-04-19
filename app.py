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


def normalize_size(current_pop, high_threshold):
    if current_pop < high_threshold:
        return 0
    return (current_pop / 100) + 1


def get_color(location, trend, high_threshold, gain_threshold):
    if trend[location]["current"] >= high_threshold:
        return "#DC143C"  # High crowd area , return red color
    if trend[location]["difference"] >= gain_threshold:
        return "#0088cc"
    return "#E3E3E3"


@app.route("/")
def index():
    return "<h1>Popularplaces API<h1>"


@app.route("/raw/")
def raw_data():
    keys = [int(k.decode("utf-8")) for k in r.scan_iter()]
    keys.sort()  # Earliest date first
    data = [json.loads(r.get(str(k)).decode("utf-8")) for k in keys]
    return jsonify(data)


# Display data collected
@app.route("/data/")
def display_data():

    keys = [int(k.decode("utf-8")) for k in r.scan_iter()]
    keys.sort()  # Earliest date first
    data = [json.loads(r.get(str(k)).decode("utf-8")) for k in keys]

    latest_data = data[-1]  # latest data
    places_types = [
        "Park",
        "Market",
        "Shopping Mall",
        "Mrt Station",
    ]  # different places types

    # Places covered data
    total_places_covered = [location["type"] for location in latest_data]
    places_covered = {t: total_places_covered.count(t) for t in places_types}
    places_covered["Total"] = len(total_places_covered)

    # High crowd data
    high_threshold = 30
    high_crowd_places = [
        location["type"]
        for location in latest_data
        if location.get("current_popularity", 0) > high_threshold
    ]
    high_crowd = {t: high_crowd_places.count(t) for t in places_types}
    high_crowd["Total"] = len(high_crowd_places)

    # Initialize required data for gaining crowd and line data
    trend = {
        location["name"]: {
            "popularity": [],
            "address": location["address"],
            "type": location["type"],
            "current": location.get("current_popularity", 0),
        }
        for location in latest_data
    }

    for t in data:
        for location in t:
            if trend.get(location["name"]) != None:
                trend[location["name"]]["popularity"].append(
                    location.get("current_popularity", 0)
                )

    for k, v in trend.items():
        if len(v["popularity"]) > 1:
            trend[k]["previous"] = v["popularity"][-2]
            trend[k]["difference"] = trend[k]["current"] - trend[k]["previous"]
        else:
            trend[k]["previous"] = "No previous record"
            trend[k]["difference"] = 0

    # Gaining crowd data
    gain_threshold = 5
    gaining_crowd_places = [
        val["type"]
        for location, val in trend.items()
        if val["difference"] >= gain_threshold
    ]
    gaining_crowd = {t: gaining_crowd_places.count(t) for t in places_types}
    gaining_crowd["Total"] = len(gaining_crowd_places)

    # Map data
    map_data = [
        {
            "title": location["name"],
            "latitude": location["coordinates"]["lat"],
            "longitude": location["coordinates"]["lng"],
            "size": normalize_size(
                location.get("current_popularity", 0), high_threshold
            ),
            "color": get_color(location["name"], trend, high_threshold, gain_threshold),
        }
        for location in latest_data
    ]

    # Table data
    shorten_type = {
        "Park": "Park",
        "Shopping Mall": "Mall",
        "Mrt Station": "Mrt",
        "Market": "Market",
    }
    table_data = [
        {
            "Location": k,
            "Current crowd": v["current"],
            "Crowd changes": v["difference"],
            "Type": shorten_type[v["type"]],
        }
        for k, v in trend.items()
    ]

    # Line data
    time_range = [
        "1hr 45mins ago",
        "1hr 30mins ago",
        "1hr 15mins ago",
        "1hr ago",
        "45 mins ago",
        "30 mins ago",
        "15 mins ago",
        "Current crowd",
    ]

    line_data = {
        typ: [
            {
                "location": k,
                "popularity": [
                    {"popularity": hour, "time": t}
                    for t, hour in zip(time_range, v["popularity"])
                ],
                "current": v["current"],
                "previous": v["previous"],
            }
            for k, v in trend.items()
            if v["type"] == typ
        ]
        for typ in places_types
    }

    final = {
        "lastUpdatedTime": datetime.fromtimestamp(keys[-1]),
        "placesCovered": places_covered,
        "highCrowd": high_crowd,
        "gainingCrowd": gaining_crowd,
        "mapData": map_data,
        "tableData": table_data,
        "lineData": line_data,
    }
    return jsonify(final)


if __name__ == "__main__":
    # Threaded option to enable multiple instances for multiple user access support
    app.run(threaded=True, port=5000)
