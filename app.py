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
r = redis.StrictRedis(
    host=url.hostname,
    port=url.port,
    password=url.password,
    charset="utf-8",
    decode_responses=True,
)


def get_usual(redis_data, place, created_time):
    if redis_data[place]["popular_times"]:
        return redis_data[place]["popular_times"][
            datetime.fromtimestamp(created_time).weekday()
        ]["data"][datetime.fromtimestamp(created_time).hour]
    return 0


def get_ratio(current, usual):
    if usual:
        return int(current / usual * 100)
    return 0


@app.route("/")
def index():
    return "<h1>Popularplaces API<h1>"


@app.route("/raw/")
def raw_data():
    last_updated = int(r.get("last_updated"))
    data = json.loads(r.get("data"))
    return jsonify({"data": data, "last_updated": last_updated})


# Display data collected
@app.route("/data/")
def display_data():

    last_updated = int(r.get("last_updated"))
    redis_data = json.loads(r.get("data"))
    with open("places.json", "r") as f:
        places = json.load(f)

    # Places covered data
    places_covered = {k: len(places[k]) for k in places}
    places_covered["total"] = sum(places_covered.values())

    # High crowd data
    high_threshold = 10
    high_crowd = {k: 0 for k in places}
    for place_type in places:
        for place in places[place_type]:
            if redis_data[place]["current_popularity"][-1] > high_threshold:
                high_crowd[place_type] += 1
                redis_data[place]["size"] = (
                    redis_data[place]["current_popularity"][-1] / 100
                ) + 1
                redis_data[place]["color"] = "#DC143C"

    high_crowd["total"] = sum(high_crowd.values())

    # Gaining crowd data
    gaining_threshold = 5
    gaining_crowd = {k: 0 for k in places}
    for place_type in places:
        for place in places[place_type]:
            if (
                redis_data[place]["current_popularity"][-1]
                - redis_data[place]["current_popularity"][-2]
                > gaining_threshold
            ):
                gaining_crowd[place_type] += 1
                if not redis_data[place].get("color"):
                    redis_data[place]["size"] = (
                        redis_data[place]["current_popularity"][-1] / 100
                    ) + 1
                    redis_data[place]["color"] = "#FF7F50"

    gaining_crowd["total"] = sum(gaining_crowd.values())

    # Map data
    map_data = []
    for place_type in places:
        for place in places[place_type]:
            map_data.append(
                {
                    "place": place,
                    "latitude": places[place_type][place][0],
                    "longitude": places[place_type][place][1],
                    "size": redis_data[place].get("size", 0),
                    "color": redis_data[place].get("color", "#E3E3E3"),
                }
            )

    # Table data
    table_data = []
    for place_type in places:
        for place in places[place_type]:
            usual = get_usual(redis_data, place, last_updated)
            current = redis_data[place]["current_popularity"][-1]
            table_data.append(
                {
                    "place": place,
                    "current": current,
                    "usual": usual,
                    "ratio": get_ratio(current, usual),
                    "changes": current - redis_data[place]["current_popularity"][-2],
                    "type": place_type.capitalize(),
                }
            )

    # Line data
    time_range = [
        "2hrs ago",
        "1hr 45mins ago",
        "1hr 30mins ago",
        "1hr 15 mins ago",
        "1hr ago",
        "45 mins ago",
        "30 mins ago",
        "15 mins ago",
        "Current crowd",
    ]
    line_data = {k: [] for k in places}
    for place_type in places:
        for place in places[place_type]:
            line_data[place_type].append(
                {
                    "place": place,
                    "popularity": [
                        {"popularity": pop, "time": t}
                        for pop, t in zip(
                            redis_data[place]["current_popularity"], time_range
                        )
                    ],
                    "current": redis_data[place]["current_popularity"][-1],
                    "previous": redis_data[place]["current_popularity"][-2],
                }
            )
    for place_type in line_data:
        line_data[place_type] = sorted(
            line_data[place_type],
            key=lambda k: k["popularity"][-1]["popularity"],
            reverse=True,
        )

    final = {
        "lastUpdatedTime": datetime.fromtimestamp(last_updated),
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
