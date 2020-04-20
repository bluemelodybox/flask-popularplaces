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
            "ChIJIdel7DsW2jERWep5ioblaeg",  # Punggol Park
            "ChIJJ9TgVLwU2jERIYaBgBL-vn0",  # Sembawang Park
            "ChIJf7-nelA92jERV0jVMjWeteQ",  # Bedok Town Park
            "ChIJBVVBo_EV2jER-jKR3Hy6qUk",  # Punggol Waterway Park
            "ChIJn_BWwVUS2jERyHkVTD_-_vw",  # Marsiling Park
            "ChIJi090zPwS2jERiJCqFWoP8Z4",  # Woodlands Waterfront Park
            "ChIJQ6n9Ygw92jERM5o8IkMEhyU",  # Sun Plaza Park
            "ChIJq_5RjPUQ2jERQZR7bzih7tw",  # Hindhede Nature Park
            "ChIJqVUlbe0P2jER-8gqnsq4eNk",  # Jurong Central Park
            "ChIJD_tH7GIU2jERYpWt_zq8Adw",  # Yishun Neighbourhood Park
            "ChIJRzglD-wR2jERgCmyEeAecXI",  # Choa Chu Kang Park
            "ChIJ59JC2zAR2jERBRxd5jtZKL8",  # Upper Peirce Reservior Park
            "ChIJJeg_k0oY2jER3tyt8xkYb9M",  # Kallang Riverside Park
            "ChIJQ86JKWEX2jERaRixYj7hl_U",  # Toa Payoh Town Park
            "ChIJ____rBoU2jERyE5AP5-iwrc",  # Lower Seletar Reservoir Park
            "ChIJbV8dJE8b2jERF_R5y03Hiv4",  # National Orchid Garden
            "ChIJpwd8kn8Z2jERptmG8rTWrsI",  # Tiong Bahru Park
            "ChIJC_hOgcgb2jERfF9HNB10xfI",  # Hort Park
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
            # Central
            "ChIJvUUHX2sZ2jERFw3Rybv_wwY",  # 100 AM
            "ChIJHSrGJ5EZ2jERTzbUyme7KuQ",  # 313 @ Somerset
            "ChIJFx0yu8sZ2jERgoVtEwS7Igs",  # Aperial Hotel
            "ChIJe5TGTaUZ2jERK_5_SabSeuk",  # Bugis Cube
            "ChIJwSNJ77oZ2jERuX8Bl5bU2tI",  # Bugis +
            "ChIJ4UnKpqYZ2jERhkj2F-wL-qI",  # Capitol Plaza
            "ChIJe3kMD5EZ2jERFGYVqhnc6DY",  # Cathay Cineleisure
            "ChIJ18C-mwoZ2jERlpSawcucgk0",  # Clark Quay Central
            "ChIJRZ1c0JYZ2jER6cpyNpOyf3M",  # Centrepoint
            "ChIJZ1omp8gZ2jERgDUAb_hBKLY",  # City Square Mall
            "ChIJE6mq47MZ2jER_HX8IQ9k_dI",  # City Gate Mall
            "ChIJCeMaMKYZ2jER7P0rzkEtRbE",  # City Link Mall
            "ChIJoXpYbrAZ2jER5klV-uEO2xk",  # DUO Galleria
            "ChIJPZTUpZIZ2jERk5GUzK_g9_0",  # Far East Plaza
            "ChIJqz4D-d8Z2jERj4HjyaJtGvI",  # Funan Mall
            "ChIJqz4D-d8Z2jERj4HjyaJtGvI",  # Great World City
            "ChIJMcwh6o0Z2jERNxsLqnSIvlw",  # ION orchard
            "ChIJb-ErmBYX2jERBl1gF7cPIj4",  # Junction 8
            "ChIJ3SrTf58Z2jER8DY5cemLKM8",  # Liang Court
            "ChIJ6xFibZIZ2jERlMTu3Bo6a9c",  # Lucky Plaza
            "ChIJHXaCig4Z2jER6U4Qy8aX7Co",  # Marina Bay Link Mall
            "ChIJb0Ex86gZ2jERediPOjPxqJo",  # Marina Square
            "ChIJo_z7IqkZ2jERE1tkSZmYR8o",  # Millenia Walk
            "ChIJP7z00McZ2jERJztQqXkRIC4",  # Mustafa Centre
            "ChIJ_7pt85EZ2jERPfKpmuwrzys",  # Ngee Ann City
            "ChIJQfAL2JYZ2jER9v06BxkNw1k",  # Orchard Central
            "ChIJR1Fyfr0Z2jERzwO-AZiJ-HM",  # Plaza Singapura
            "ChIJl6MA_AMZ2jERWANMx2T6CTk",  # Raffles City Shopping Centre
            "ChIJB5c91rsZ2jERPX9_QUwrSsc",  # Sim Lim Square
            "ChIJoavrZ-cZ2jEROHbTzqYlVM4",  # Square 2
            "ChIJ20X__K4Z2jERRE8GRs-d8HE",  # Suntec City
            "ChIJmXU4W4oZ2jERDul4uxP0pYc",  # Tanglin Mall
            "ChIJbV9Xho0Z2jERrixofz0u32Q",  # Tangs
            "ChIJRX3v4ecZ2jERD0BlqjhXKCc",  # United Square Shopping Mall
            "ChIJO_5acOcZ2jERMTtjb_W_6Go",  # Velocity
            "ChIJmbzds40Z2jERALKuryaSSVI",  # Wisma
            "ChIJZdlKPGAX2jERZlOP1cN5tpE",  # Zhongshan Mall
            # East
            "ChIJQyaM-6892jER4DhQn_hqaYE",  # White sands
            "ChIJHeeu2LU92jER7QLDVe4tKf0",  # Downtown East
            "ChIJCXOkiBE92jERm5iCnMRS4_g",  # Century Square
            "ChIJTVVVIA492jER3TWGPpL6gRk",  # Tampines 1
            "ChIJ89lf6hE92jERQXt7c5QOFnY",  # Tampines Mall
            "ChIJu487hQ092jER1DJRGc0isoE",  # Our Tampines Hub
            "ChIJ483Qk9YX2jERA0VOQV7d1tY",  # Changi Airport
            "ChIJ7egDUyI92jERegSiojEDekc",  # Eastpoint Mall
            "ChIJR80Wwcoi2jER8yMuAZodQFk",  # Bedok Mall
            "ChIJgWBomLQi2jERyU-GZEHw_NI",  # Bedok Point
            "ChIJzxwZERgY2jER2FX37qmG49Q",  # Paya Lebar Square
            "ChIJC8ructcZ2jER2WPSgnCqzoc",  # SingPost Centre
            "ChIJpYsWrRcY2jERBZAPQskkaAw",  # City plaza
            "ChIJR9HlRHIY2jER9RAgYi5Pqbs",  # Katong Square
            "ChIJC3pECnIY2jERnT-FiMVHPIk",  # Katong V
            "ChIJjQcl90sY2jER0d9ui1pFp9s",  # Kallang Wave Mall
            "ChIJLd1Z2E4Y2jERUHJ7xqnUnoI",  # Leisure Park Kallang
            "ChIJO78EpL492jERuWZcaOf26DA",  # Elias Mall
            "ChIJxzqK6cg92jERB-QagzzIEzs",  # Loyang Point
            "ChIJw3l-FL4X2jERw2pScvHQCbg",  # Jewel Changi Airport
            # North
            "ChIJIf-M9qAT2jERd0qCLuYmM3E",  # 888 Plaza
            "ChIJT80kwuYW2jERhogn4ZW0fG4",  # AMK Hub
            "ChIJg0iEvagT2jERLpza72gaJ08",  # Causeway Point
            "ChIJLWB1kOcW2jERY7Rq2bBgbuo",  # Broadway Plaza
            "ChIJ_VQ0s-cW2jERd1Ub9pthffE",  # Jubilee Square
            "ChIJ9RT-5DYV2jERKbQXNwU4m6A",  # Northpoint City
            "ChIJ5-zrBoEU2jERhLnuZ0bgvUw",  # Sembawang Shopping Centre
            "ChIJAQAQzRQU2jERLmj7diPcCTI",  # Wisteria Mall
            # North East
            "ChIJLTCPpuQV2jERYMYF_2uprDE",  # Waterway Point
            "ChIJy5hMcg8X2jERZ88iA4l4aBY",  # Compass One
            "ChIJTzOjnDcW2jERHyoyj5LFmQ4",  # Hougang Mall
            "ChIJ1V9s-bMX2jERQL59mMdHw7I",  # Heartland mall
            "ChIJeRqAraYX2jERQpyIAXSU1SU",  # Nex Shopping Mall
            "ChIJ1-hkimEW2jERyc5cKHfC3Sw",  # Greenwich V
            "ChIJ0ZkQLVsW2jERY9DgZurtpl8",  # Hougang 1
            "ChIJpwNkYAIW2jERFRkVIyvEAao",  # Punggol Plaza
            "ChIJmQIrNAUW2jER6VgPAnslDhU",  # Rivervale Mall
            "ChIJg6GcaBcW2jER_Vyx8tDZ1JU",  # Rivervale Plaza
            "ChIJRwKG2mQW2jER13VL9P5QOws",  # Seletar Mall
            # North West
            "ChIJ78vtZqkR2jERKX0SUR60iGA",  # Beauty World Plaza
            "ChIJe3kTN6MR2jERr5FbmFCuxPI",  # Bukit Panjang Plaza
            "ChIJ3f_4K_8R2jER2TvFneukn0s",  # Bukit Timah Plaza
            "ChIJM7Kvx3QZ2jER7umUFjan98g",  # Fajar Shopping Centre
            "ChIJ_8HAraQR2jERaLrfjzD5k7Y",  # Hillion Mall
            "ChIJQSyjW7wR2jERvUB8B9ktx20",  # Junction 10
            "ChIJSZQvzO8R2jER3y3YPr6U1bQ",  # Limbang Shopping Centre
            "ChIJs5CBlukR2jER-uyFhQ2NFbU",  # Lot One Shoppers Mall
            "ChIJSX0T2lMQ2jERa6Ixq0d5hkA",  # The Rail Mail
            "ChIJ83fjadsR2jERGC3nsNM2g7s",  # Sunshine Place
            "ChIJU5KbYb8R2jERt0l-XxTMFgI",  # Tech Whye Shopping Centre
            "ChIJxzbY1z4Q2jER7drH__TXbOI",  # West Mall
            "ChIJL3h_-fMR2jERaA6mW3u-wNU",  # YewTee Point
            # South
            "ChIJM9EuaOIb2jERA_WsaQ7eLkE",  # Harbourfront Centre
            "ChIJwz3yHMEb2jERHclcbs5KL4A",  # Alexandra Retail Centre
            "ChIJK7xLl1gZ2jERP_GdUY9XNLo",  # Vivo City
            # West
            "ChIJi1x76o0a2jERRpEEL3FuXJk",  # 321 Clementi
            "ChIJq8UGJo4a2jER0ypiQDLiXpg",  # The Clementi Mall
            "ChIJa2nb2g0Q2jERlcUMLtMVyoI",  # IMM
            "ChIJh7rQjgUQ2jERSAcxrKMcMg0",  # Jcube
            "ChIJ82AoVA8Q2jERkHnr1RPW9B4",  # Westgate
            "ChIJa9YM2-wP2jERmOUStQKyiS0",  # Jurong Point
            "ChIJ67mVKpcP2jERmahUvC-ZIwc",  # Pioneer Mall
            "ChIJ91hVlEIa2jERm_cxYZ2WKow",  # The star vista
            "ChIJe5qyMnkZ2jERYRN7QP_pCMQ",  # Tiong Bahru Plaza
            "ChIJ_1WtiMwb2jERZKYs6ALsFxc",  # Alexandra Central Mall
            "ChIJVXvuuswb2jER8BZU_kcS2ds",  # Anchorpoint Shopping Centre
            "ChIJgz0VpRYX2jER-1qtnt9Cxdc",  # Fairprice Hub
            "ChIJ58WuUbwP2jERmHrSoc33hp4",  # Gek Poh Shopping Centre
            "ChIJEaBAGEMa2jERttBFiHpl_UE",  # Rochester Mall
            "ChIJDVS-D_sP2jERQRJHuRjWdpQ",  # Taman Jurong Shopping Centre
            "ChIJdxS7gPEa2jER1iMe8Ai90Ts",  # West Coast Plaza
            "ChIJpeuce8sb2jERad66-Hwmus8",  # Queensway Shopping Centre
        ],
        "Mrt Station": [
            # Red Line
            "ChIJL52pCg8Q2jERgiWsODe3X9I",  # Jurong East
            "ChIJmYDhHjYQ2jERn_eQZlaFlkw",  # Bukit Gombak
            "ChIJCZRupukR2jERCglyJsNXaHE",  # CCK
            "ChIJg9zFkjAR2jERXmWQPEs55Kg",  # Yew Tee
            "ChIJOfqWIFMS2jERa9KO1y1xmCM",  # Marsiling
            "ChIJYx1I6qgT2jERIxNhwuTCugQ",  # Woodlands
            "ChIJ9dSHDHUT2jER2OAwCRnDDKk",  # Admiralty
            "ChIJs-mIfYQU2jERIv9P7DRoqto",  # Canberra
            "ChIJrxLCiA0U2jER-R_D8ehBje4",  # Khatib
            "ChIJn0tVp-oW2jERdiqBIe4em60",  # Yio Chu Kang
            "ChIJn0ZzleYW2jERBt5hM27FAkw",  # AMK
            # Green Line
            "ChIJQ2MJTysa2jERGZ-q0JXY0qg",  # Rehill
            "ChIJU1XTRiI92jERegtsZETZgPU",  # Simei
            "ChIJ8RPWtkE92jERA2c5rW_gvkQ",  # Tanah Merah
            "ChIJ9Qq5NwcY2jERMkhHdGleiKk",  # Kembangan
            # "ChIJA08Q0gQY2jERrq60-mOgmxQ",  # Eunos
            "ChIJU-xrGhgY2jERLX7VN6dXpsA",  # Paya Lebar
            "ChIJ580nZDQY2jERNmMG5GQHQMI",  # Kallang
            "ChIJR_ZtkaYZ2jERV9J2Cytl6ZM",  # City Hall
            # Purple Line
            # Circle Line
            # Downtown Line
        ],
    }

    res = []
    creation_time = int(time())  # Get time of crawl without milliseconds
    for key in places:
        for p in places[key]:
            places_data = populartimes.get_id(API_KEY, p)
            places_data["created_time"] = creation_time
            places_data["type"] = key
            if key == "Mrt Station" and "MRT" not in places_data["name"]:
                places_data["name"] += " MRT"

            res.append(places_data)
    r.setex(name=creation_time, time=timedelta(hours=2), value=json.dumps(res))
    return "success"


sched.start()
