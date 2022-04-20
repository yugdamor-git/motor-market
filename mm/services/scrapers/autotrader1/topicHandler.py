from topic import producer,consumer
from listingScraper import listingScraper

import traceback

class topicHandler:
    def __init__(self):
        print("image prediction handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.scrape'
        
        self.publish = 'motormarket.scraper.autotrader.listing.transform'

        logsTopic = "motormarket.scraper.logs"
        
        self.scraper = listingScraper()
            
        self.logsProducer = producer.Producer(logsTopic)
        
        self.producer = producer.Producer(self.publish)
        
        self.consumer = consumer.Consumer(self.subscribe)
        
    def main(self):
        
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                id = data["data"]["sourceId"]
                
                scrapedData = self.scraper.scrapeById(id)
                
                if scrapedData["status"] == False:
                    # log message here
                    # expire listing if present
                    continue
                
                data["rawData"] = scrapedData["data"]
                
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