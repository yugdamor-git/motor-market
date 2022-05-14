import scrapy

from autotrader_scrapy.topic.producer import Producer

class DealerListingValidatorSpider(scrapy.Spider):
    name = 'dealer-listing-validator'
    
    logsTopic = "motormarket.scraper.logs"
            
    logsProducer = Producer.Producer(logsTopic)
    
    
    def start_requests(self):
        log = {}
        log["sourceUrl"] = "test"
        log["service"] = 'motormarket.scraper.autotrader.url'
        log["errorMessage"] = "test"
        
        self.logsProducer.produce({
            "eventType":"insertLog",
            "data":log
        })
        
        print(log)
