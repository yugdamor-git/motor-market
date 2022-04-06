from datetime import datetime


from ..topic import producer

class ListingSpiderPipeline:
    
    def __init__(self):  
        self.topic = 'motormarket.scraper.autotrader.listing.transform'
        
        self.producer = producer.Producer(self.topic)
        
    def process_item(self, item, spider):
        print("---------------------------------- pipeline ----------------------------------")
        data = item["data"]
        
        scraped = item["scraped"]
        
        data["data"].update(scraped)
        
        if not "event" in data:
            data["event"] = []
        
        data["event"].append({
            "topic":self.topic,
            "timestamp":str(datetime.now())
        })
        
        print(data)
        
        self.producer.produce(data)
        
        return item