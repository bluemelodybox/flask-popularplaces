from apscheduler.schedulers.blocking import BlockingScheduler
import populartimes
import os, json
from time import time
from datetime import timedelta, datetime
from urllib.parse import urlparse
import redis

sched = BlockingScheduler()


@sched.scheduled_job("interval", minutes=15)
def timed_job():
    url = urlparse(os.environ.get("REDISCLOUD_URL"))
    r = redis.Redis(host=url.hostname, port=url.port, password=url.password)
    API_KEY = os.environ["GOOGLE_API_KEY"]
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
            "ChIJP6_oVKEX2jER8tuSBXrr2es",  # Serangoon Station
            "ChIJsQvqY0Ia2jER_QrezH_adnY",  # Bouna Vista Station
            "ChIJv7v_qp0Z2jER41auJd1oiSg",  # Bugis Station
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
        r.set(name="last_created_time", value=creation_time)
        r.setex(name=creation_time, time=timedelta(hours=2), value=json.dumps(res))
    else:
        last_created_time = int(r.get("last_created_time").decode("utf-8"))
        if creation_time - last_created_time < 280:
            return "Time too close"
        else:
            res = []
            for key in places:
                for p in places[key]:
                    res.append(populartimes.get_id(API_KEY, p))
            r.set(name="last_created_time", value=creation_time)
            r.setex(name=creation_time, time=timedelta(hours=2), value=json.dumps(res))

    return "success"


sched.start()
