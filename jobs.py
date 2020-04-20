from apscheduler.schedulers.blocking import BlockingScheduler
import populartimes
import os, json
from time import time
from datetime import timedelta
from urllib.parse import urlparse
import redis

sched = BlockingScheduler()


@sched.scheduled_job("interval", minutes=15)
def timed_job():
    url = urlparse(os.environ.get("REDISCLOUD_URL"))
    r = redis.Redis(host=url.hostname, port=url.port, password=url.password)
    API_KEY = os.environ["GOOGLE_API_KEY"]

    places = {
        "Park": [
            "ChIJ0QX_Brki2jER-pZKNdqk_a8",  # East Coast Park
            "ChIJMxZ-kwQZ2jERdsqftXeWCWI",  # Gardens by the Bay
            "ChIJNTlhccg92jERBftThrByZK0",  # Pasir Ris Park
            "ChIJvWDbfRwa2jERgNnTOpAU3-o",  # Singapore Botanic Gardens
            "ChIJjyjjwCAX2jERxYHvTxAw4X0",  # Bishan AMK Park
            "ChIJ-T0BVdkb2jERLHDkcItuv4o",  # Mount Faber Park
            "ChIJQ8uk4vwa2jERqMrT6OFBMuw",  # West Coast Park
            "ChIJr_uWXrEb2jERw4hpdv17mZc",  # Kent Ridge Park
            "ChIJs3oprUQQ2jERp8A9mbkPZJE",  # Bukit Batok Nature Park
            "ChIJa1WN9dob2jERWFxNMq2ZVgM",  # Telok Blangah Hill Park
            "ChIJUzbaYws82jERXHKMRlUKIug",  # Changi Beach Park
            "ChIJNRelbgMT2jERD6Ak-zoK23U",  # Admirality Park
            "ChIJVSYjJKIZ2jERpRFinATD52s",  # Fort Canning Park
        ],
        "Market": [
            # Hawker
            "ChIJWzTvXEcY2jERvSzB8XNKKCk",  # Old Airport Road Food Centre
            "ChIJ9YySoRAY2jERp62zayDnKuM",  # Malay Market
            "ChIJu0Kj3f8W2jERpHVeB5eORMM",  # Chomp Chomp Food Centre
            "ChIJ1ZbzmxIZ2jERLxbskKJNx8Q",  # Amoy Street Food Centre
            "ChIJ04DTdbQZ2jERFt4kBQi-E60",  # Golden Mile Food Centre
            "ChIJseQsTQ0Z2jERqpBTWF0Zf84",  # Maxwell Food Centre
            "ChIJwQ8GHooQ2jER5Ucy4Sx80Wk",  # Bukit Timah Market & Food Centre
            "ChIJq-d3hRY82jERfOWbep52mCs",  # Changi Village Hawker Centre
            "ChIJ_c6vFoMZ2jERBWATXfBQ7Aw",  # Zion Riverside Food Centre
            "ChIJyzfPULoZ2jERCEVmhtL9I8g",  # Albert Centre Market
            "ChIJwdRlwNkZ2jER5VFwn2Iks3c",  # Whampoa Hawker Centre
            "ChIJRwKzawsZ2jERKxpszLl54EU",  # Hong Lim Market & Food Centre
            "ChIJUQ7wM9IZ2jERaks089nXQoA",  # Bendemeer Market & Food Centre
            "ChIJH1HG6tEW2jERGsch7FbDL5g",  # Sembawang Hills Food Centre
            "ChIJx6JYenEY2jERgilO3SwweQg",  # 84 Marine Parade Central Market and Food Centre
            "ChIJv4Mk4QYa2jERlGncaOIvpW4",  # Adam Road Food Centre
            "ChIJo6zVvtwP2jERyf2Nuhm97cM",  # Jurong West 505 Market & Food Centre
            # Market
            "ChIJk_idN3oU2jEReqhHxnv3lgI",  # Chong Pang Market
        ],
        "Shopping Mall": [
            "ChIJl_sOMpIZ2jERKgmuCf4FCs0",  # Paragon Shopping Centre
            "ChIJ20X__K4Z2jERRE8GRs-d8HE",  # Suntec City
            "ChIJeRqAraYX2jERQpyIAXSU1SU",  # Nex Shopping Mall
            "ChIJMcwh6o0Z2jERNxsLqnSIvlw",  # ION orchard
            "ChIJK7xLl1gZ2jERP_GdUY9XNLo",  # Vivo City
            "ChIJZ1omp8gZ2jERgDUAb_hBKLY",  # City Square Mall
            "ChIJa9YM2-wP2jERmOUStQKyiS0",  # Jurong Point
            "ChIJ_7pt85EZ2jERPfKpmuwrzys",  # Ngee Ann City
            "ChIJa2nb2g0Q2jERlcUMLtMVyoI",  # IMM
            "ChIJx4wPggYZ2jERnT8vOV1XU5k",  # The shoppers at MBS
            "ChIJb0Ex86gZ2jERediPOjPxqJo",  # Marina Square
            "ChIJR1Fyfr0Z2jERzwO-AZiJ-HM",  # Plaza Singapura
            "ChIJxyqHo7oZ2jERNQtDRIuckqU",  # Bugis Junction
        ],
        "Mrt Station": [
            # Red
            "ChIJCZRupukR2jERCglyJsNXaHE",  # CCK
            "ChIJn0ZzleYW2jERBt5hM27FAkw",  # AMK
            # Green
            "ChIJQ2MJTysa2jERGZ-q0JXY0qg",  # Rehill
            "ChIJU1XTRiI92jERegtsZETZgPU",  # Simei
            "ChIJ8RPWtkE92jERA2c5rW_gvkQ",  # Tanah Merah
            "ChIJ9Qq5NwcY2jERMkhHdGleiKk",  # Kembangan
            "ChIJA08Q0gQY2jERrq60-mOgmxQ",  # Eunos
            "ChIJU-xrGhgY2jERLX7VN6dXpsA",  # Paya Lebar
            "ChIJ580nZDQY2jERNmMG5GQHQMI",  # Kallang
            "ChIJR_ZtkaYZ2jERV9J2Cytl6ZM",  # City Hall
        ],
    }

    res = []
    creation_time = int(time())  # Get time of crawl without milliseconds
    for key in places:
        for p in places[key]:
            places_data = populartimes.get_id(API_KEY, p)
            places_data["created_time"] = creation_time
            places_data["type"] = key
            res.append(places_data)
    r.setex(name=creation_time, time=timedelta(hours=2), value=json.dumps(res))
    return "success"


sched.start()
