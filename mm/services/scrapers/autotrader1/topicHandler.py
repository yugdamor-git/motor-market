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
                
                meta = data["meta"]
                
                id = meta["uniqueId"]
                
                scrapedData = self.scraper.scrapeById(id)
                
                if scrapedData["status"] == False:
                    # log message here
                    print(f'requestNo : {meta["requestId"]} - failed')
                    continue
                
                print(f'requestNo : {meta["requestId"]} - success')
                
                data["data"].update(scrapedData["data"])
                
                self.producer.produce(data)
                
                # break
                
            except Exception as e:
                print(f'error : {str(e)}')
                
                traceback.print_exc()
                
                log = {}
                log["errorMessage"] = str(e)
                log["service"] = "services.machine.learning.image"
                log["sourceUrl"] = meta["sourceUrl"]
                
                self.logsProducer.produce(log)
                
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()