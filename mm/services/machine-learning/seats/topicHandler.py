import sys

sys.path.append("/libs")

from pulsar_manager import PulsarManager

from predictor import Predictor

import traceback


class topicHandler:
    def __init__(self):
        print("seat topic handler init")
        pulsar_manager = PulsarManager()
        
        self.topics = pulsar_manager.topics
        
        self.consumer = pulsar_manager.create_consumer(pulsar_manager.topics.LISTING_PREDICT_SEAT)
        
        self.producer = pulsar_manager.create_producer(pulsar_manager.topics.LISTING_PREDICT_IMAGE)
        
        self.logs_producer = pulsar_manager.create_producer(pulsar_manager.topics.LOGS)
        
        self.predictor = Predictor()
     
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume_message()
                           
                make = data["data"]["predictedMake"]
                
                model = data["data"]["predictedModel"]
                
                seats = data["data"].get("seats",None)
                
                if seats == None:
                    
                    prediction = self.predictor.predict(make,model)
                    
                    data["data"].update(prediction)
                
                print(data)
                
                self.producer.produce_message(data)
                
            except Exception as e:
                print(f'error : {str(e)}')
                log = {}
                
                log["sourceUrl"] = data["data"]["sourceUrl"]
                log["service"] = self.topics.LISTING_PREDICT_SEAT.value
                log["errorMessage"] = traceback.format_exc()
                
                self.logs_producer.produce_message({
                    "eventType":"insertLog",
                    "data":log
                })
                
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()