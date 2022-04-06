from email import header
from gc import callbacks
import scrapy
import pulsar
from datetime import datetime
import json
import os

# schema : input
    




class ListingSpider(scrapy.Spider):
    name = 'listing'
    
    topic = 'motormarket.scraper.autotrader.listing.scrape'
    
    uri = 'pulsar://pulsar'
    
    graphql = Graphql()
    
    proxy = graphql.proxy["http"]
    
    print(f'proxy : {proxy}')
    
    client = pulsar.Client(uri)
    
    consumer = client.subscribe(topic, 'ListingSpider-subscription')
    
    custom_settings = {
         "ROBOTSTXT_OBEY":False,
         "RETRY_ENABLED":True,
         "CONCURRENT_REQUESTS":16,
         "COOKIES_ENABLED":False,
         "RETRY_TIMES":3,
         "LOG_LEVEL":"DEBUG",
         "ITEM_PIPELINES" : {
            ListingSpiderPipeline: 300,
        }
    }

    def start_requests(self):
        while True:
            
            # message = self.consumer.receive()
            
            try:
                # data = json.loads(message.data)
                
                # self.consumer.acknowledge(message)
                data = {
                    "data":{},
                    "meta":{
                        "autoTraderId":"202203253932273",
                        "url":"https://www.autotrader.co.uk/car-details/202203253932273"
                    }
                }
                id = data["meta"]["autoTraderId"]
                
                if id == None:
                    # log error here
                    continue
                
                query = self.graphql.requiredFieldsQuery
                
                payload = self.graphql.getPayload(id,query)
                
                yield scrapy.Request(
                    url = self.graphql.url,
                    headers = self.graphql.headers,
                    method="POST",
                    body = json.dumps(payload),
                    callback = self.parseListing,
                    meta = {
                        "proxy":self.proxy,
                        "data":data
                    }
                )
                
                break
                
            except Exception as e:
                print(f'invalid message : {str(e)} ')

    def parseListing(self,response):
        
        data = response.meta["data"]
        
        jsonData = response.json()
        
        listingData = self.graphql.extractDataFromJson(jsonData)
        
        yield {
            "scraped":listingData,
            "data":data
        }