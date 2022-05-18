import sys

sys.path.append("/libs")

from pulsar_manager import PulsarManager

from calculation import Calculation
import json

import traceback

class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        pulsar_manager = PulsarManager()
        
        self.topics = pulsar_manager.topics
        
        self.consumer = pulsar_manager.create_consumer(self.topics.LISTING_POST_CALCULATION)
        
        self.producer = pulsar_manager.create_producer(self.topics.FL_LISTINGS_INSERT)
        
        self.logs_producer = pulsar_manager.create_producer(self.topics.LOGS)
            
        self.calculation = Calculation()
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume_message()
                
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
                
                self.producer.produce_message(data)
                
            except Exception as e:
                print(f'error : {str(e)}')
                log = {}
                
                log["sourceUrl"] = data["data"]["sourceUrl"]
                log["service"] = self.topics.LISTING_POST_CALCULATION.value
                log["errorMessage"] = traceback.format_exc()
                
                self.logs_producer.produce_message({
                    "eventType":"insertLog",
                    "data":log
                })
                
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()