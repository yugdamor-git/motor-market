from topic import producer,consumer

from predictor import Predictor

import traceback

class topicHandler:
    def __init__(self):
        print("image prediction handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.predict.image'
        
        self.publish = 'motormarket.scraper.autotrader.listing.validation'

        logsTopic = "motormarket.scraper.logs"
        
        self.predictor = Predictor()
            
        self.logsProducer = producer.Producer(logsTopic)
        
        self.producer = producer.Producer(self.publish)
        
        self.consumer = consumer.Consumer(self.subscribe)
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                meta = data["meta"]
                
                images = data["data"]["images"]
                
                if len(images) == 0:
                    print(f'this listing does not have any images')
                    
                    log = {}
                    
                    log["errorMessage"] = "this listing does not have any images."
                    log["service"] = "services.machine.learning.image"
                    log["sourceUrl"] = meta["sourceUrl"]
                    
                    self.logsProducer.produce(log)
                    
                    continue
                
                predictions = self.predictor.predict(images)
                
                data["data"]["images"] = predictions
                
                print(data)
                
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