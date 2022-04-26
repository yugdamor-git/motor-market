from validation import Validation

from topic import producer,consumer

import traceback


class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.prevalidation'
        
        self.publish = 'motormarket.scraper.autotrader.listing.predict.makemodel'
        
        logsTopic = "motormarket.scraper.logs"
        
        self.producer = producer.Producer(self.publish)
        
        self.consumer = consumer.Consumer(self.subscribe)
        
        self.logsProducer = producer.Producer(logsTopic)
        
        self.validator = Validation(self.logsProducer)
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                if self.validator.validate(data["data"]) == False:
                    print('we are not taking this listing')
                    continue
                
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