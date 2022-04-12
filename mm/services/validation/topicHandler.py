from validation import Validation

from topic import producer,consumer

import traceback


class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.validation'
        
        self.publish = 'motormarket.scraper.autotrader.listing.calculation'
        
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
                
                print(data)
                
                if self.validator.validate(data["data"],data["meta"]) == False:
                    print('we are not taking this listing')
                    
                self.producer.produce(data)
                
                # break
                
            except Exception as e:
                print(f'error : {str(e)}')
                traceback.print_exc()
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()