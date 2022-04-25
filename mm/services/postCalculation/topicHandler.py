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
                
                scraperType = data["data"].get("scraperType")
                
                if scraperType == "validator":
                    
                    # admin fee
                    self.calculation.updateAdminFee(data["data"])
                    
                    # source price
                    self.calculation.calculateSourcePrice(data["data"])
                    
                    # margin
                    self.calculation.calculateMargin(data["data"])
                    
                    # mmPrice
                    self.calculation.calculateMMprice(data["data"])
                    
                    # pcp apr
                    self.calculation.calculatePcpApr(data["data"])
                    
                    # ltv
                    self.calculation.calculateLtv(data["data"])
                    
                elif scraperType == "normal":
                    
                    # admin fee
                    self.calculation.updateAdminFee(data["data"])
                    
                    # source price
                    self.calculation.calculateSourcePrice(data["data"])
                    
                    # margin
                    self.calculation.calculateMargin(data["data"])
                    
                    # mmPrice
                    self.calculation.calculateMMprice(data["data"])
                
                    # pcp apr
                    self.calculation.calculatePcpApr(data["data"])
                    
                    # ltv
                    self.calculation.calculateLtv(data["data"])
                    
                    # categoryId
                    self.calculation.calculateCategoryId(data["data"])
                    
                    categoryId = data["data"].get("categoryId",None)
                    
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
                    
                    # video id
                    self.calculation.calculateVideoId(data["data"])
                
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