from datetime import datetime


from ..topic import producer

class ListingSpiderPipeline:
    
    def __init__(self):  
        self.topic = 'motormarket.scraper.autotrader.listing.transform'
        
        logsTopic = "motormarket.scraper.logs"
    
        self.logsProducer = producer.Producer(logsTopic)
        
        self.producer = producer.Producer(self.topic)
        
    def process_item(self, item, spider):
        try:
            data = item["data"]
            
            meta = data["meta"]
            
            scraped = item["scraped"]
            
            data["data"].update(scraped)
            
            self.producer.produce(data)
            
        except Exception as e:
            log = {}
            log["errorMessage"] = str(e)
            log["service"] = "services.scrapers.autotrader.listing.pipeline"
            log["sourceUrl"] = meta["sourceUrl"]
            
            self.logsProducer.produce(log)
        
        return item