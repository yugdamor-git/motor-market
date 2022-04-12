import scrapy

from .graphql import graphql

import json

from .pipelines import listing

from .topic import consumer,producer




class ListingSpider(scrapy.Spider):
    name = 'listing'
    
    topic = 'motormarket.scraper.autotrader.listing.scrape'
    
    consumer = consumer.Consumer(topic)
    
    logsTopic = "motormarket.scraper.logs"
    
    logsProducer = producer.Producer(logsTopic)
    
    graphql = graphql.Graphql()
    
    proxy = graphql.proxy["http"]
    
    custom_settings = {
         "ROBOTSTXT_OBEY":True,
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
                # self.crawler.pause()
                # data = self.consumer.consume()
                # self.crawler.resume()
                data = {'data': {}, 'meta': {'uniqueId': '202203253932273', 'sourceUrl': 'https://www.autotrader.co.uk/car-details/202203253932273', 'websiteId': 's21'}}
                meta = data["meta"]
                
                id = meta["uniqueId"]
                
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
                        "data":data,
                        'download_timeout': 5
                    },
                    dont_filter=True
                )
                
                break
                
            except Exception as e:
                print(f'invalid message : {str(e)}')
                log = {}
                log["errorMessage"] = str(e)
                log["service"] = "services.scrapers.autotrader.listing.spider"
                log["sourceUrl"] = meta["sourceUrl"]
                
                self.logsProducer.produce(log)
                

    def parseListing(self,response):
        
        try:
        
            data = response.meta["data"]
            meta = data["meta"]
            jsonData = response.json()
            
            listingData = self.graphql.extractDataFromJson(jsonData)
            
            yield {
                "scraped":listingData,
                "data":data
            }
        except Exception as e:
            log = {}
            log["errorMessage"] = str(e)
            log["service"] = "services.scrapers.autotrader.listing.spider"
            log["sourceUrl"] = meta["sourceUrl"]
            
            self.logsProducer.produce(log)
