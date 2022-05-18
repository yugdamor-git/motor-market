
import sys

sys.path.append("/libs")
sys.path.append("/post_calculation")

from calculation import Calculation

from pulsar_manager import PulsarManager

from flask import Blueprint,request,jsonify
import os

post_calculation = Calculation()

calculation = Blueprint("calculation",__name__)

auth_token = os.environ.get("FLASK_AUTH_TOKEN")

@calculation.route("/ltv",methods=['POST'])
def ltv():
    
    json_data = request.json
    
    token = request.args.get("token")
    
    if token != auth_token:
        return jsonify(
            {
                "status":False,
                "message":"invalid auth token",
                "data":None
            }
        )
    
    ID = int(json_data.get("ID"))
    
    update_in_db = json_data.get("update_in_db",0)
    
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
        "predictedRegistration":registration,
        "mileage":mileage
    }
    
    post_calculation.calculateLtv(tmp)
    
    if update_in_db == True and ID != None:
        what = tmp["ltv"]
        what["updated_at"] = {"func":"now()"}
        where = {"ID":ID}
        
        post_calculation.db.connect()
        try:
            post_calculation.db.recUpdate("fl_listings",what,where)
        except Exception as e:
            print(f'error: {str(e)}')
            
        post_calculation.db.disconnect()
            
    return jsonify({
        "status":True,
        "message":"200",
        "data":tmp
    })
