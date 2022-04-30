from Database import Database

from topic import producer,consumer

import traceback

class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.database.urlscaper'

        self.db = Database()
        
        logsTopic = "motormarket.scraper.logs"
    
        self.logsProducer = producer.Producer(logsTopic)
        
        self.consumer = consumer.Consumer(self.subscribe)
    
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                eventType = data["eventType"]
                
                if eventType == "update":
                    what = data["what"]
                    
                    where = data["where"]
                    
                    self.db.connect()
                    
                    self.db.recUpdate("AT_urls",what,where)
                    
                    self.db.disconnect()
                    
                    continue
                
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