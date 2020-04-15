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


def get_color(name, trend):
    t0, t1, t2 = trend[name]
    print(name, t0, t1, t2)
    if t0 >= t1 >= t2:  # Croud decreasing
        return "#228B22"  # Green color
    elif t0 <= t1 <= t2:  # Croud increasing
        return "#DC143C"  # Red color
    else:
        return "#0088cc"


def normalize_size(current_pop):
    if current_pop == 0:
        return 0
    return (current_pop / 100) + 0.5


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

    trend = {location["name"]: [] for location in data[-1][:-1]}
    for t in data[-3:]:
        for location in t[:-1]:
            trend[location["name"]].append(location.get("current_popularity", 0))

    trend_8 = {location["name"]: [] for location in data[-1][:-1]}
    for t in data:
        for location in t[:-1]:
            trend_8[location["name"]].append(location.get("current_popularity", 0))

    map_data = [
        {
            "title": location["name"],
            "latitude": location["coordinates"]["lat"],
            "longitude": location["coordinates"]["lng"],
            "size": normalize_size(location.get("current_popularity", 0)),
            "color": get_color(location["name"], trend),
        }
        for location in data[-1][:-1]
    ]

    line_data = [
        {
            "location": k,
            "popularity": [{"popularity": hour} for hour in v],
            "current": v[-1],
            "previous": v[-2],
        }
        for i, (k, v) in enumerate(trend_8.items())
    ]
    return jsonify(
        {
            "mapData": map_data,
            "lastUpdatedTime": datetime.fromtimestamp(data[-1][-1]),
            "lineData": line_data,
        }
    )


# Get data from google api
@app.route("/fetch/")
def fetch_data():

    # places = [
    #     "ChIJjyjjwCAX2jERxYHvTxAw4X0",  # Bishan park
    #     "ChIJk_idN3oU2jEReqhHxnv3lgI",  # Chongpang market
    #     "ChIJMcwh6o0Z2jERNxsLqnSIvlw",  # Ion Orchard mall
    #     "ChIJP7z00McZ2jERJztQqXkRIC4",  # Mustafa centre
    #     "ChIJeRqAraYX2jERQpyIAXSU1SU",  # Nex shopping mall
    # ]

    places = {
        "Parks": [
            "ChIJjyjjwCAX2jERxYHvTxAw4X0",  # Bishan AMK Park
            "ChIJs3oprUQQ2jERp8A9mbkPZJE",  # Bukit Batok Nature Park
            "ChIJNTlhccg92jERBftThrByZK0",  # Pasir Ris Town Park
            "ChIJVSYjJKIZ2jERpRFinATD52s",  # Fort Canning Park
            "ChIJOXfMU-ob2jERylk7sJHYvIY",  # Labrador Nature Reserve
        ],
        "Market": [
            "ChIJk_idN3oU2jEReqhHxnv3lgI",  # Chong Pang Market
            "ChIJBYEYQR0X2jERTlmOUoDyW5c",  # 409 AMK Market
            "ChIJwwQ3jwET2jERQ7cbQ6ZHDpQ",  # Marsiling Market
            "ChIJyzfPULoZ2jERCEVmhtL9I8g",  # Albert Centre Market
            "ChIJHabbDPsP2jEROQtuba0GHhc",  # Taman Jurong Market
        ],
        "Malls": [
            "ChIJP7z00McZ2jERJztQqXkRIC4",  # Mustafa
            "ChIJR1Fyfr0Z2jERzwO-AZiJ-HM",  # Plaza Singapura
            "ChIJMcwh6o0Z2jERNxsLqnSIvlw",  # ION orchard
            "ChIJb20nHg8Q2jERuBnOGGnuq-s",  # JEM
            "ChIJzxwZERgY2jER2FX37qmG49Q",  # Paya Lebar Square
        ],
        "Mrt": [
            "ChIJMSKsChcX2jERPC0Poz3FmWg",  # Serangoon Station
            "ChIJC4cARUIa2jERBJvOk5gwWlM",  # Bouna Vista Station
            "ChIJuz41c7AZ2jERgl25Ot0ZuJw",  # Bugis Station
            "ChIJCZRupukR2jERCglyJsNXaHE",  # CCK Station
            "ChIJiWOMEd4V2jERGpO2In3q6iw",  # Yishun Station
        ],
    }


    creation_time = int(time())  # Get time of crawl without milliseconds
    if not r.exists("last_created_time"):
        # res = [populartimes.get_id(API_KEY, place) for place in places]
        res = []

        for key in places:
            for p in places[key]:
                res.append(populartimes.get_id(API_KEY, p))
        res.append(creation_time)
        r.set(name="last_created_time", value=creation_time)
        r.setex(name=creation_time, time=timedelta(hours=2), value=json.dumps(res))
    else:
        last_created_time = int(r.get("last_created_time").decode("utf-8"))
        if creation_time - last_created_time < 720:
            return jsonify("Failed, time too recent")
        else:
            res = []
            for key in places:
                for p in places[key]:
                    res.append(populartimes.get_id(API_KEY, p))
            r.set(name="last_created_time", value=creation_time)
            r.setex(name=creation_time, time=timedelta(hours=2), value=json.dumps(res))

    return jsonify("Success")


if __name__ == "__main__":
    # Threaded option to enable multiple instances for multiple user access support
    app.run(threaded=True, port=5000)
