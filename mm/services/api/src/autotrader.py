
from flask import Blueprint,request,jsonify
import os

from listingScraper import listingScraper

from topic import producer,consumer

scraper = listingScraper()

autotrader = Blueprint("autotrader",__name__)

auth_token = os.environ.get("FLASK_AUTH_TOKEN")

finderTopic = 'motormarket.scraper.autotrader.listing.database.finder'

dealerScraperTopic = 'motormarket.scraper.autotrader.dealer.scrape'

finderProducer = producer.Producer(finderTopic)

dealerScraperProducer = producer.Producer(dealerScraperTopic)


@autotrader.route("/scrape-listing",methods=['POST'])
def scrapeListing():
    jsonData = request.json
    
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
    
    query_type = request.args.get("type")
    
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
    try:
        data = scraper.scrapeById(listing_id)
        status = True
    except Exception as e:
        data = None
        message = f'error : {str(e)}'
    
    response["data"] = data
    response["message"] = message
    response["status"] = status
    
    return jsonify(response)