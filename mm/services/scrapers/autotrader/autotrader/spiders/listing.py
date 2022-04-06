import scrapy

from .graphql import graphql

import json

from .pipelines import listing

from .topic import consumer

class ListingSpider(scrapy.Spider):
    name = 'listing'
    
    topic = 'motormarket.scraper.autotrader.listing.scrape'
    
    consumer = consumer.Consumer(topic)
    
    graphql = graphql.Graphql()
    
    proxy = graphql.proxy["http"]
    
    custom_settings = {
         "ROBOTSTXT_OBEY":False,
         "RETRY_ENABLED":True,
         "CONCURRENT_REQUESTS":16,
         "COOKIES_ENABLED":False,
         "RETRY_TIMES":3,
         "LOG_LEVEL":"DEBUG",
         "ITEM_PIPELINES" : {
            listing.ListingSpiderPipeline: 300,
        }
    }

    def start_requests(self):
        while True:
            
            try:
                data = self.consumer.consume()
                
                id = data["meta"]["autoTraderId"]
                
                if id == None:
                    continue
                
                query = self.graphql.requiredFieldsQuery
                
                payload = self.graphql.getPayload(id,query)
                
                yield scrapy.Request(
                    url = self.graphql.url,
                    headers = self.graphql.headers,
                    method = "POST",
                    body = json.dumps(payload),
                    callback = self.parseListing,
                    meta = {
                        "proxy":self.proxy,
                        "data":data
                    },
                    dont_filter=True
                )
                
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
