from topic import producer,consumer
from dealerScraper import dealerScraper

import traceback

class topicHandler:
    def __init__(self):
        print("image prediction handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.dealer.scrape'
        
        self.publish = 'motormarket.scraper.autotrader.listing.database.finder'

        logsTopic = "motormarket.scraper.logs"
        
        self.scraper = dealerScraper()
            
        self.logsProducer = producer.Producer(logsTopic)
        
        self.producer = producer.Producer(self.publish)
        
        self.consumer = consumer.Consumer(self.subscribe)
    
    def main(self):
        
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                dealerId = data["data"]["dealerId"]
                
                scrapedData = self.scraper.scrapeByDealerId(dealerId)
                
                for item in scrapedData:
                    tmp = {
                        "data":{
                            "sourceId":item,
                            "sourceUrl":f'https://www.autotrader.co.uk/car-details/{item}',
                            "scraperName":"Dealer Scraper",
                            "websiteId":"17"
                        }
                    }
                    
                    print(tmp)
                    
                    self.producer.produce(tmp)
                
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