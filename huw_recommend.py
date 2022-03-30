from flask import Flask, request, session, render_template, redirect, url_for, g
from flask_restful import Api, Resource, reqparse
import os
from pymongo import MongoClient
from dotenv import load_dotenv
import content_filtering
import collaborative_filtering

app = Flask(__name__)
api = Api(app)

# We define these variables to (optionally) connect to an external MongoDB
# instance.
envvals = ["MONGODBUSER", "MONGODBPASSWORD", "MONGODBSERVER"]
dbstring = 'mongodb+srv://{0}:{1}@{2}/test?retryWrites=true&w=majority'

# Since we are asked to pass a class rather than an instance of the class to the
# add_resource method, we open the connection to the database outside of the 
# Recom class.
load_dotenv()
if os.getenv(envvals[0]) is not None:
    envvals = list(map(lambda x: str(os.getenv(x)), envvals))
    client = MongoClient(dbstring.format(*envvals))
else:
    client = MongoClient()
database = client.huwebshop


class Recom(Resource):
    """ This class represents the REST API that provides the recommendations for
    the webshop. At the moment, the API simply returns a random set of products
    to recommend."""

    def get(self, profileid, count):
        """ This function represents the handler for GET requests coming in
        through the API. It currently returns a random sample of products. """
        product_id = content_filtering.recommend_products(profileid)
        items_fetched = len(product_id)
        if items_fetched <= 4:
            product_id.append(collaborative_filtering.recommend_products_profile(product_id, (4 - items_fetched)))
        items_fetched = len(product_id)
        if items_fetched <= 4:
            product_id.append(collaborative_filtering.recently_bought_products(product_id - 4))

        return product_id, 200


# This method binds the Recom class to the REST API, to parse specifically
# requests in the format described below.
api.add_resource(Recom, "/<string:profileid>/<int:count>")
