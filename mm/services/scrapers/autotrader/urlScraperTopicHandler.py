
import sys
sys.path.append("/libs")

from pulsar_manager import PulsarManager

from Database import Database

import traceback

import time

class topicHandler:
    def __init__(self):
        print("image prediction handler init")
        
        pulsar_manager = PulsarManager()
        
        self.topics = pulsar_manager.topics
        
        self.producer = pulsar_manager.create_producer(self.topics.FL_LISTINGS_FIND)
        
        self.logs_producer = pulsar_manager.create_producer(self.topics.LOGS)
        
        self.db = Database()
        
    
    def getPendingListings(self):
        pendingListings = []
        try:
            query = f'SELECT id,url,custom_price_enable,custom_price_value FROM `AT_urls` WHERE status ="active" AND scraped=0'
            
            pendingListings = self.db.recCustomQuery(query)
            
        except Exception as e:
            print(f'error : {__file__} : {str(e)}')
        
        return pendingListings

    def preprocess(self,listings):
        
        processedListings = []
        
        for listing in listings:
            try:
                tmp = {}
                id = listing["url"].strip("/").split("/")[-1]
                int(id)
                tmp["sourceId"] = id
                tmp["sourceUrl"] = listing["url"]
                tmp["scraperName"] = "url-scraper"
                tmp["websiteId"] = "17"
                tmp["listingId"] = listing["id"]
                
                if listing["custom_price_enable"] == 1:
                    customPrice = int(float(listing["custom_price_value"]))
                    
                    tmp["customPriceEnabled"] = True
                    tmp["customPrice"] = customPrice
                
                processedListings.append(tmp)
                
            except Exception as e:
                print(f'error : {__file__} : {str(e)}')
                
                log = {}
                log["sourceUrl"] = listing["url"]
                log["service"] = 'motormarket.scraper.autotrader.url'
                log["errorMessage"] = traceback.format_exc()
                
                self.logsProducer.produce({
                    "eventType":"insertLog",
                    "data":log
                })
                
        return processedListings
                
                
    
    def main(self):
        
        while True:
            self.db.connect()
            try:
                
                rawListings = self.getPendingListings()
                
                pendingListings = self.preprocess(rawListings)
                
                for listing in pendingListings:
                    print(listing)
                    self.producer.produce_message(
                        {
                            "data":listing
                        }
                    )
                    
                    self.db.recUpdate("AT_urls",{"scraped":2},{"id":listing["listingId"]})
                    
            except Exception as e:
                print(f'error : {str(e)}')
                log = {}
                log["sourceUrl"] = f'{__file__}'
                log["service"] = 'motormarket.scraper.autotrader.url'
                log["errorMessage"] = traceback.format_exc()
                
                self.logs_producer.produce_message({
                    "eventType":"insertLog",
                    "data":log
                })
                
            self.db.disconnect()
            time.sleep(3)
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()