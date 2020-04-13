# app.py
from flask import Flask, request, jsonify
import populartimes
import os

app = Flask(__name__)


@app.route("/getmsg/", methods=["GET"])
def respond():
    # Retrieve the name from url parameter
    name = request.args.get("name", None)

    # For debugging
    print(f"got name {name}")

    response = {}

    # Check if user sent a name at all
    if not name:
        response["ERROR"] = "no name found, please send a name."
    # Check if the user entered a number not a name
    elif str(name).isdigit():
        response["ERROR"] = "name can't be numeric."
    # Now the user entered a valid name
    else:
        response["MESSAGE"] = f"Welcome {name} to our reawesome platform!!"

    # Return the response in json format
    return jsonify(response)


# A welcome message to test our server
@app.route("/")
def index():
    API_KEY = os.environ["GOOGLE_API_KEY"]
    places = [
        "ChIJk_idN3oU2jEReqhHxnv3lgI",  # Chongpang market
        "ChIJjyjjwCAX2jERxYHvTxAw4X0",  # Bishan park
        "ChIJeRqAraYX2jERQpyIAXSU1SU",  # Nex shopping mall
    ]
    res = [populartimes.get_id(API_KEY, place) for place in places]

    return jsonify(res)


if __name__ == "__main__":
    # Threaded option to enable multiple instances for multiple user access support
    app.run(threaded=True, port=5000)
