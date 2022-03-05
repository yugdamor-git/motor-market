from flask import Flask,jsonify,request
from GraphqlApi import GraphqlApi
import os

graphql_api = GraphqlApi()

app = Flask(__name__)

auth_token = "1d9f0cf413468abf271ccd3f483bf3ef"

@app.route("/autotrader/graphql")
def graphql():
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
        data = graphql_api.fetch(listing_id,query_type)
        status = True
    except Exception as e:
        data = None
        message = f'error : {str(e)}'
    
    response["data"] = data
    response["message"] = message
    response["status"] = status
    
    return jsonify(response)


if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)