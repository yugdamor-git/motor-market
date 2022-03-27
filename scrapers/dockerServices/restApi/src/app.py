from flask import Flask,jsonify,request
import os

from graphql import graphql
app = Flask(__name__)

app.register_blueprint(graphql, url_prefix='/autotrader')

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)