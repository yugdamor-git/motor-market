from calculation import Calculation
from topic import producer,consumer
import json

import traceback

class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.post.calculation'
        
        self.publish = 'motormarket.scraper.autotrader.listing.database.production'

        logsTopic = "motormarket.scraper.logs"
    
        self.logsProducer = producer.Producer(logsTopic)
        
        self.calculation = Calculation()
        
        self.producer = producer.Producer(self.publish)
        
        self.consumer = consumer.Consumer(self.subscribe)
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                # pcp apr
                pcpapr = self.calculation.calculatePcpApr(data["data"])
                
                data["data"]["pcpapr"] = pcpapr
                
                
                # ltv
                ltv = self.calculation.calculateLtv(data["data"])
                
                data["data"]["ltv"] = ltv
                
                # categoryId
                categoryId = self.calculation.calculateCategoryId(data["data"])
                
                if categoryId == None:
                    
                    log = {}
                
                    log["sourceUrl"] = data["data"]["sourceUrl"]
                    log["service"] = self.subscribe
                    log["errorMessage"] = "categoryId is None."
                    
                    self.logsProducer.produce({
                        "eventType":"insertLog",
                        "data":log
                    })
                    
                    continue
                    # log this event
                data["data"]["categoryId"] = categoryId
                
                # video id
                videoId = self.calculation.calculateVideoId(data["data"])
                
                data["data"]["videoId"] = videoId
                
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