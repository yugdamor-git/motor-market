from topic import producer,consumer
from listingScraper import listingScraper

import traceback

class topicHandler:
    def __init__(self):
        print("image prediction handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.scrape'
        
        self.publish = 'motormarket.scraper.autotrader.listing.transform'
        
        self.fl_listings = 'motormarket.scraper.autotrader.listing.database.production'

        logsTopic = "motormarket.scraper.logs"
        
        self.scraper = listingScraper()
            
        self.logsProducer = producer.Producer(logsTopic)
        
        self.producer = producer.Producer(self.publish)
        
        self.fl_listings_producer = producer.Producer(self.fl_listings)
        
        self.consumer = consumer.Consumer(self.subscribe)
    
    def expireListing(self,id,tradeLifecycleStatus):
        event = "update"
        what = {
            "Status":"expired",
            "why":"no longer active on auto trader",
            "tradeLifecycleStatus":tradeLifecycleStatus
        }
        where = {
            "sourceId":id
        }
        data = {
            "event":event,
            "eventData":{
                "what":what,
                "where":where
            }
        }
        
        self.fl_listings_producer.produce(data)
    
    def main(self):
        
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                scraperType = data["data"].get("scraperType")
                
                id = data["data"]["sourceId"]
                
                scrapedData = self.scraper.scrapeById(id,scraperType)
                if scraperType == "validator":
                    if scrapedData["status"] == False:
                        # expire this listing , it is no longer active on source site.
                        self.expireListing(id)
                        continue
                    
                    if scrapedData["data"]["tradeLifecycleStatus"] in ["WASTEBIN","SALE_IN_PROGRESS"]:
                        self.expireListing(id,scrapedData["data"]["tradeLifecycleStatus"])
                        continue
                    
                if scraperType == "normal":
                    if scrapedData["status"] == False:
                        # this listing was processed by normal scraper no need to update this kind of listings.
                        continue
                
                data["data"].update(scrapedData["data"])
                
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