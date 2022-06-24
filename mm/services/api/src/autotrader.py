
import sys
from datetime import datetime
sys.path.append("/libs")

from pulsar_manager import PulsarManager

from flask import Blueprint,request,jsonify,make_response
import os

from listingScraper import listingScraper

from topic import producer,consumer

from database import Database

import pymongo

scraper = listingScraper()

pm = PulsarManager()



import json


autotrader = Blueprint("autotrader",__name__)

auth_token = os.environ.get("FLASK_AUTH_TOKEN")

finderTopic = pm.topics.FL_LISTINGS_FIND.value

dealerScraperTopic = 'motormarket.scraper.autotrader.dealer.scrape'

@autotrader.route("/car-cutter-webhook",methods=['GET','POST'])
def car_cutter():
    db = Database()
    data = request.get_json()
    
    db.car_cutter.insert_one(data)
    
    return jsonify({})

@autotrader.route("/listing-count",methods=['GET'])
def listing_count():
    db = Database()
    
    perPage = 10
    
    page = int(request.args.get("p",0))
    
    skip = page * perPage
    
    where = {
        "type":"insert"
    }
    
    logs = list(db.listing_count.find(where,{"_id":0}).sort("updatedAt",pymongo.DESCENDING).skip(skip).limit(perPage))
    
    for log in logs:
        log["updatedAt"] = datetime.timestamp(log["updatedAt"])
        log["createdAt"] = datetime.timestamp(log["createdAt"])
                           
    
    data = {
        "data":logs,
        "status":True,
        "page":page
    }
    
    response = make_response(json.dumps(data,indent=4))
    response.status_code = 200
    response.headers['Content-Type'] = 'application/json; charset=utf-8'
    response.headers['mimetype'] = 'application/json'
    
    return response


@autotrader.route("/scrape-listing",methods=['POST'])
def scrapeListing():
    jsonData = request.json
    finderProducer = producer.Producer(finderTopic)
    finderProducer.produce(
        jsonData
    )
    
    return jsonify({
        "status":True,
        "data":jsonData,
        "message":"listing will be added in website soon. you can track status on /track endpoint"
    })
    
    

@autotrader.route("/scrape-dealer",methods=['POST'])
def scrapeDealer():
    jsonData = request.json
    dealerScraperProducer = producer.Producer(dealerScraperTopic)
    dealerScraperProducer.produce(
        jsonData
    )
    
    return jsonify({
        "status":True,
        "data":jsonData,
        "message":"listing will be added in website soon. you can track status on /track endpoint"
    })
    

    # url = jsonData.get("url",None)
    
    # if url == None:
    #     return jsonify({
    #         "status":False,
    #         "data":None,
    #         "message":"please include url in json body."
    #     })
    
    # sourceId = url.strip().strip("/").split("/")[-1]
    
    # if sourceId.isdigit() == False:
    #     return jsonify({
    #         "status":False,
    #         "data":None,
    #         "message":"please recheck the url.autoTraderId can't be extracted."
    #     })
    
    # customPrice = jsonData.get("customPrice",None)
    
    
    
    # data = {
    #     "sourceUrl":url,
    #     "sourceId":sourceId,
    #     "customPrice":customPrice,
    #     "scraperType":"normal"
    # }
    
    # if customPrice != None:
    #     data["customPriceEnabled"] = True
    # else:
    #     data["customPriceEnabled"] = False
    
    
    

# @autotrader.route("/listing/manual-entry")
# def addManualEntry():
#     jsonData = request.json
    
#     url = jsonData.get("url",None)
    
#     if url == None:
#         return jsonify({
#             "status":False,
#             "data":None,
#             "message":"please include url in json body."
#         })
    
#     data = {}
#     meta = {
#         "url":url,
#         "manualEntry":True
#     }
    
#     pulsar.produce(
#         {
#             "data":data,
#             "meta":meta
#         }
#     )
    
#     return jsonify({
#         "status":True,
#         "data":meta,
#         "message":"listing will be added in website soon. you can track status on /track endpoint"
#     })
        


@autotrader.route("/graphql")
def graphqlEndpoint():
    response = {
        "data":None,
        "message":None,
        "status":None
    }
    
    token = request.args.get("token")
    
    query_type = request.args.get("type",None)
    
    if token == None:
        response["status"] = False
        response["message"] = "please add token in query string 'token'."
        return jsonify(response)
    if token != auth_token:
        response["status"] = False
        response["message"] = "the token is invalid."
        response["auth"]
        return jsonify(response)
    
    
    listing_id = request.args.get("id")
    message = "200"
    status = False
    data = None
    try:
        data = scraper.scrapeById(listing_id,query_type)
        status = True
    except Exception as e:
        message = f'error : {str(e)}'
    
    response["data"] = data
    response["message"] = message
    response["status"] = status
    
    return jsonify(response)
