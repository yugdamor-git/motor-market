import json
from graphql import graphql
import requests


class listingScraper:
    
    def __init__(self) -> None:
        self.graphql = graphql.Graphql()
        self.proxy = self.graphql.proxy
        self.maxRetry = 5
    
    def scrapeById(self,id):
        
        query = self.graphql.requiredFieldsQuery
        
        payload = self.graphql.getPayload(id,query)
        
        for retry in range(0,self.maxRetry):
            try:
                response = requests.post(
                    url = self.graphql.url,
                    headers=self.graphql.headers,
                    data = json.dumps(payload),
                    proxies=self.proxy
                )
                
                if not "cloudflare" in response.text.lower():
                    jsonData = response.json()
                    message = "200"
                    break
                
            except Exception as e:
                print(f'error : {str(e)}')
                jsonData = None
                message = str(e)
        
        if jsonData == None:
            return {
                "status":False,
                "data":None,
                "message":message
            }
        
        try:
            data = self.graphql.extractDataFromJson(jsonData)
            message = "200"
        except Exception as e:
            print(f'error : {str(e)}')
            data = None
            message = str(e)
        
        if data == None:
            return {
                "status":False,
                "data":None,
                "message":message
            }
        
        return {
                "status":True,
                "data":data,
                "message":message
            }