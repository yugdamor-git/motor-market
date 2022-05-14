import scrapy
import sys
sys.path.append("...")
from topic import producer,consumer

class DealerListingValidatorSpider(scrapy.Spider):
    name = 'dealer-listing-validator'
    
    logsTopic = "motormarket.scraper.logs"
            
    logsProducer = producer.Producer(logsTopic)
    
    
    def start_requests(self):
        for i in range(0,5):
            log = {}
            log["sourceUrl"] = "test"
            log["service"] = 'motormarket.scraper.autotrader.url'
            log["errorMessage"] = "test"
            
            self.logsProducer.produce({
                "eventType":"insertLog",
                "data":log
            })
            
            print(log)
            
            yield scrapy.Request("https://www.google.com")
