import sys

sys.path.append("/libs")

from pulsar_manager import PulsarManager

from listingScraper import listingScraper

import traceback

class topicHandler:
    def __init__(self):
        print("image prediction handler init")
        
        pulsar_manager = PulsarManager()
        
        self.topics = pulsar_manager.topics
        
        self.consumer = pulsar_manager.create_consumer(pulsar_manager.topics.AUTOTRADER_LISTING_SCRAPER)
        
        self.producer = pulsar_manager.create_producer(pulsar_manager.topics.LISTING_TRANSFORM)
        
        self.fl_listings_update = pulsar_manager.create_producer(pulsar_manager.topics.FL_LISTINGS_UPDATE)
        
        self.logs_producer = pulsar_manager.create_producer(pulsar_manager.topics.LOGS)
        
        self.scraper = listingScraper()
    
    def expireListing(self,id,tradeLifecycleStatus):
        
        what = {
            "Status":"expired",
            "why":"no longer active on auto trader",
            
        }
        
        if tradeLifecycleStatus != None:
            what["tradeLifecycleStatus"] = tradeLifecycleStatus
            
        where = {
            "sourceId":id
        }
        
        data = {
            "data":{
                "what":what,
                "where":where
            }
        }
        
        self.fl_listings_update.produce_message(data)
    
    def main(self):
        
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume_message()
                
                scraperType = data["data"].get("scraperType")
                
                id = data["data"]["sourceId"]
                # print(id)
                # continue
                
                print(f'processing : {data["data"]["sourceUrl"]}')
                
                scrapedData = self.scraper.scrapeById(id,scraperType)
                
                
                if scraperType == "validator":
                    if scrapedData["status"] == False:
                        # expire this listing , it is no longer active on source site.
                        self.expireListing(id,None)
                        continue
                    
                    tradeLifecycleStatus = scrapedData["data"].get("tradeLifecycleStatus",None)
                    
                    if tradeLifecycleStatus in ["WASTEBIN","SALE_IN_PROGRESS"]:
                        self.expireListing(id,tradeLifecycleStatus)
                        continue
                    
                if scraperType == "normal":
                    if scrapedData["status"] == False:
                        # this listing was processed by normal scraper no need to update this kind of listings.
                        continue
                
                data["data"].update(scrapedData["data"])
                
                print(data)
                
                self.producer.produce_message(data)
                
            except Exception as e:
                print(f'error : {str(e)}')
                log = {}
                log["sourceUrl"] = data["data"]["sourceUrl"]
                log["service"] = self.topics.AUTOTRADER_LISTING_SCRAPER.value
                log["errorMessage"] = traceback.format_exc()
                
                self.logs_producer.produce_message({
                    "eventType":"insertLog",
                    "data":log
                })
                
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()