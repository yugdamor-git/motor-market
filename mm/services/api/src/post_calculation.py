
import sys

sys.path.append("/libs")
sys.path.append("/post_calculation")

from calculation import Calculation

from pulsar_manager import PulsarManager

from flask import Blueprint,request,jsonify
import os

post_calculation = Calculation()

pm = PulsarManager()

calculation = Blueprint("calculation",__name__)

auth_token = os.environ.get("FLASK_AUTH_TOKEN")

@calculation.route("/ltv",methods=['POST'])
def ltv():
    json_data = request.json
    
    ID = int(json_data.get("ID"))
    
    mmPrice = int(json_data.get("mmPrice"))
    
    sourcePrice = int(json_data.get("sourcePrice"))
    
    registration = json_data.get("registration")
    
    mileage = int(json_data.get("mileage"))
    
    websiteId = json_data.get("websiteId")
    
    tmp = {
        "mmPrice":mmPrice,
        "price":sourcePrice,
        "registrationStatus":True,
        "websiteId":websiteId,
        "registration":registration,
        "mileage":mileage
    }
    
    post_calculation.calculateLtv(tmp)
    
    return jsonify(tmp)
