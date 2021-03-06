
from flask import Blueprint,request,jsonify
import os
from graphqlUtils import graphqlUtils

from pulsarHandler import pulsarHandler

gutils = graphqlUtils()

pulsar = pulsarHandler()

autotrader = Blueprint("autotrader",__name__)

auth_token = os.environ.get("FLASK_AUTH_TOKEN")


@autotrader.route("/listing/manual-entry")
def addManualEntry():
    jsonData = request.json
    
    url = jsonData.get("url",None)
    
    if url == None:
        return jsonify({
            "status":False,
            "data":None,
            "message":"please include url in json body."
        })
    
    autoTraderId = url.strip().strip("/").split("/")[-1]
    
    if autoTraderId.isdigit() == False:
        return jsonify({
            "status":False,
            "data":None,
            "message":"please recheck the url.autoTraderId can't be extracted."
        })
    
    customPrice = jsonData.get("customPrice",None)
    
    data = {}
    
    meta = {
        "url":url,
        "autoTraderId":autoTraderId,
        "manualEntry":True,
        "customPrice":customPrice
    }
    
    pulsar.produce(
        {
            "data":data,
            "meta":meta,
            "event":[]
        },
        pulsar.topicScrape
    )
    
    return jsonify({
        "status":True,
        "data":meta,
        "message":"listing will be added in website soon. you can track status on /track endpoint"
    })
    

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
        return jsonify(response)
    
    
    listing_id = request.args.get("id")
    message = "200"
    status = False
    try:
        data = gutils.fetch(listing_id,query_type)
        status = True
    except Exception as e:
        data = None
        message = f'error : {str(e)}'
    
    response["data"] = data
    response["message"] = message
    response["status"] = status
    
    return jsonify(response)