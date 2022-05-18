import sys

sys.path.append("/libs")

from pulsar_manager import PulsarManager

from validation import Validation

from topic import producer,consumer

import traceback


class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        pulsar_manager = PulsarManager()
        
        self.topics = pulsar_manager.topics
        
        self.consumer = pulsar_manager.create_consumer(pulsar_manager.topics.LISTING_PREVALIDATION)
        
        self.producer = pulsar_manager.create_producer(pulsar_manager.topics.LISTING_PREDICT_MAKEMODEL)
        
        self.logs_producer = pulsar_manager.create_producer(pulsar_manager.topics.LOGS)
        
        self.at_urls_update_producer = pulsar_manager.create_producer(pulsar_manager.topics.AT_URLS_UPDATE)
         
        self.validator = Validation(self.logs_producer)
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume_message()
                
                status,log = self.validator.validate(data["data"])
                
                if status == False:
                    print('we are not taking this listing')
                    
                    scraperName = data["data"].get("scraperName",None)
                    
                    listingId = data["data"].get("listingId",None)
                    
                    if scraperName == None or listingId == None:
                        continue
                    
                    what = {
                        "scraped":3,
                        "errorMessage":log["errorMessage"]
                    }
                    
                    where = {
                        "id":listingId
                    }
                    
                    data = {
                            "data":{
                                "what":what,
                                "where":where,
                            }
                        }
                    
                    if scraperName == "url-scraper":
                        self.at_urls_update_producer.produce_message(data)
                    
                    print(data)
                    
                    continue
                
                print(data)
                
                self.producer.produce_message(data)
                
                # break
                
            except Exception as e:
                print(f'error : {str(e)}')
                
                log = {}
                
                log["sourceUrl"] = data["data"]["sourceUrl"]
                log["service"] = self.topics.LISTING_PREVALIDATION
                log["errorMessage"] = traceback.format_exc()
                
                self.logs_producer.produce_message({
                    "eventType":"insertLog",
                    "data":log
                })
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()