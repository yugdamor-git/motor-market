from Database import Database

from topic import producer,consumer

import traceback

import os

class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.database.finder'
        
        self.publish = 'motormarket.scraper.autotrader.listing.scrape'

        self.db = Database()
        
        
        logsTopic = "motormarket.scraper.logs"
    
        self.logsProducer = producer.Producer(logsTopic)
        
        self.producer = producer.Producer(self.publish)
        
        self.consumer = consumer.Consumer(self.subscribe)
        
        self.env = os.environ.get("environ","prod")
    
    
    
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                skipFinder = data["data"].get("skipFinder",False)
                
                if skipFinder == True:
                    
                    data["data"]["scraperType"] = "validator"
                    self.producer.produce(data)
                    
                    continue
                
                sourceId = data["data"]["sourceId"]
                
                self.db.connect()
                
                result = self.db.recCustomQuery(f'SELECT ID,dealer_id,Status,predictedMake,predictedModel,engineCylindersCC,mileage,built,registrationStatus,predictedRegistration FROM fl_listings WHERE sourceId="{sourceId}"')
                
                if len(result) > 0:
                    data["data"]["scraperType"] = "validator"
                    data["data"].update(result[0])
                else:
                    data["data"]["scraperType"] = "normal"
                
                self.db.disconnect()
                
                if self.env == "dev":
                    print(data)
                
                self.producer.produce(data)
                
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