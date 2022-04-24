from Database import Database

from topic import producer,consumer

import traceback

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
    
    
    
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                sourceId = data["data"]["sourceId"]
                
                self.db.connect()
                
                result = self.db.recCustomQuery(f'SELECT ID,Status,predictedMake,predictedModel,engineCylindersCC,mileage,built,registrationStatus,predictedRegistration FROM fl_listings WHERE sourceId="{sourceId}"')
                
                if len(result) > 0:
                    data["data"]["scraperType"] = "validator"
                    data["data"].update(result[0])
                else:
                    data["data"]["scraperType"] = "normal"
                
                self.db.disconnect()
                
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