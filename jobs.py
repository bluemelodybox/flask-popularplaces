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
            "ChIJ0QX_Brki2jER-pZKNdqk_a8",  # East Coast Park
            "ChIJQ8uk4vwa2jERqMrT6OFBMuw",  # West Coast Park
            "ChIJjyjjwCAX2jERxYHvTxAw4X0",  # Bishan AMK Park
            "ChIJvWDbfRwa2jERgNnTOpAU3-o",  # Singapore Botanic Gardens
            "ChIJVSYjJKIZ2jERpRFinATD52s",  # Fort Canning Park
        ],
        "Market": [
            "ChIJk_idN3oU2jEReqhHxnv3lgI",  # Chong Pang Market
            "ChIJo6zVvtwP2jERyf2Nuhm97cM",  # Jurong West 505 Market & Food Centre
            "ChIJ9YySoRAY2jERp62zayDnKuM",  # Malay Market
            "ChIJseQsTQ0Z2jERqpBTWF0Zf84",  # Maxwell Food Centre
            "ChIJyzfPULoZ2jERCEVmhtL9I8g",  # Albert Centre Market
        ],
        "Malls": [
            "ChIJ20X__K4Z2jERRE8GRs-d8HE",  # Suntec City
            "ChIJMcwh6o0Z2jERNxsLqnSIvlw",  # ION orchard
            "ChIJK7xLl1gZ2jERP_GdUY9XNLo",  # Vivo City
            "ChIJeRqAraYX2jERQpyIAXSU1SU",  # Nex Shopping Mall
            "ChIJa9YM2-wP2jERmOUStQKyiS0",  # Jurong Point 
        ],
        "Mrt": [
            "ChIJCZRupukR2jERCglyJsNXaHE",  # CCK Station
        ],
    }

    creation_time = int(time())  # Get time of crawl without milliseconds
    if not r.exists("last_created_time"):
        res = []

        for key in places:
            for p in places[key]:
                res.append(populartimes.get_id(API_KEY, p))
        for p in res:
            p["created_time"] = creation_time
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
            for p in res:
                p["created_time"] = creation_time
            r.set(name="last_created_time", value=creation_time)
            r.setex(name=creation_time, time=timedelta(hours=2), value=json.dumps(res))
    return "success"


sched.start()
