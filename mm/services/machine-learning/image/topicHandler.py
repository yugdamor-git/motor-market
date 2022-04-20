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
                
                websiteId = data["data"]["websiteId"]
                
                sourceId = data["data"]["sourceId"]
                
                rawData = data["rawData"]
                
                images = rawData["images"]
                
                predictions = self.predictor.predict(images,websiteId,sourceId)
                
                data["data"]["images"] = predictions
                
                print(data)
                
                self.producer.produce(data)
                
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