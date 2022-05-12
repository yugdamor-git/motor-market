import json
from graphql import graphql
import requests


class listingScraper:
    
    def __init__(self) -> None:
        self.graphql = graphql.Graphql()
        
        self.proxy = self.graphql.proxy
        
        self.maxRetry = 3
        
        self.accountId = 24898
        
        self.planId = 26
        
        self.featureId = 26
        
        self.websiteId = "17"
        
        self.priority = 109
    
    def scrapeById(self,id,scraperType):
        
        if scraperType == "normal":
            query = self.graphql.requiredFieldsQuery
        elif scraperType == "validator":
            query = self.graphql.priceFieldQuery
            
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
            if scraperType == "normal":
                data = self.graphql.extractDataFromJson(jsonData)
            elif scraperType == "validator":
                data = self.graphql.extractValidatorDataFromJson(jsonData)
            elif scraperType == None:
                data = jsonData
                
            message = "200"
        except Exception as e:
            print(f'error {__file__} : {str(e)}')
            data = None
            message = str(e)
        
        if data == None:
            return {
                "status":False,
                "data":None,
                "message":message
            }
        
        data["accountId"] = self.accountId
        
        data["websiteId"] = self.websiteId
        
        data["featuredId"] = self.featureId
        
        data["planId"] = self.planId
        
        data["priority"] = self.priority
        
        return {
                "status":True,
                "data":data,
                "message":message
            }
        
if __name__ == "__main__":
    # testing
    s = listingScraper()
    print(s.proxy)
    print(s.scrapeById("202204124568625","normal"))