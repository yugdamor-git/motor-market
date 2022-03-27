
from flask import Blueprint,request,jsonify
import os
from graphqlUtils import graphqlUtils

gutils = graphqlUtils()

graphql = Blueprint("graphql",__name__)

auth_token = os.environ.get("FLASK_AUTH_TOKEN")

@graphql.route("/graphql")

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