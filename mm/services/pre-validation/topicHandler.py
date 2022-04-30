from validation import Validation

from topic import producer,consumer

import traceback


class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.prevalidation'
        
        self.publish = 'motormarket.scraper.autotrader.listing.predict.makemodel'
        
        logsTopic = "motormarket.scraper.logs"
        
        self.urlScraperTopic = 'motormarket.scraper.autotrader.listing.database.urlscaper'
        
        self.producer = producer.Producer(self.publish)
        
        self.consumer = consumer.Consumer(self.subscribe)
        
        self.logsProducer = producer.Producer(logsTopic)
        
        self.urlScraperProducer = producer.Producer(self.urlScraperTopic)
        
        self.validator = Validation(self.logsProducer)
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                status,log = self.validator.validate(data["data"])
                
                if status == False:
                    print('we are not taking this listing')
                    
                    scraperName = data["data"].get("scraperName",None)
                    listingId = data["data"].get("listingId",None)
                    
                    if scraperName == None or listingId == None:
                        continue
                    
                    what = {
                        "status":3,
                        "errorMessage":log["errorMessage"]
                    }
                    
                    where = {
                        "id":listingId
                    }
                    
                    eventType = "update"
                    
                    self.urlScraperProducer.produce({
                        "what":what,
                        "where":where,
                        "eventType":eventType
                    })
                    
                    continue
                
                print(data)
                
                self.producer.produce(data)
                
                # break
                
            except Exception as e:
                print(f'error : {str(e)}')
                
                log = {}
                
                log["sourceUrl"] = data["data"]["sourceUrl"]
                log["service"] = self.subscribe
                log["errorMessage"] = traceback.format_exc()
                
                self.logsProducer.produce({
                    "eventType":"insertLog",
                    "data":log
                })
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()