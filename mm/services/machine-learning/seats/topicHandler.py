from topic import producer,consumer

from predictor import Predictor

import traceback


class topicHandler:
    def __init__(self):
        print("seat topic handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.predict.seat'
        
        self.publish = 'motormarket.scraper.autotrader.listing.predict.image'

        self.predictor = Predictor()
        
        logsTopic = "motormarket.scraper.logs"
    
        self.logsProducer = producer.Producer(logsTopic)
        
        self.producer = producer.Producer(self.publish)
        
        self.consumer = consumer.Consumer(self.subscribe)
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                           
                make = data["data"]["predictedMake"]
                
                model = data["data"]["predictedModel"]
                
                prediction = self.predictor.predict(make,model)
                
                data["data"].update(prediction)
                
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