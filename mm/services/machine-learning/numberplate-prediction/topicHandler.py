import sys

sys.path.append("/libs")

from pulsar_manager import PulsarManager

from handler import Handler
from redisHandler import redisHandler
import json
import traceback

class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        pulsar_manager = PulsarManager()
        
        self.topics = pulsar_manager.topics
        
        self.consumer = pulsar_manager.create_consumer(self.topics.LISTING_PREDICT_NUMBERPLATE)
        
        self.producer = pulsar_manager.create_producer(self.topics.LISTING_POST_CALCULATION)
        
        self.logs_producer = pulsar_manager.create_producer(self.topics.LOGS)
        
        self.predictor = Handler()
        
        self.redis = redisHandler()
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume_message()
                
                images = data["data"]["images"]
                
                rawRegistration = data["data"]["orignalRegistration"]
                
                if "*" in rawRegistration:
                
                    sourceId = data["data"]["sourceId"]
                    
                    registrationData = self.redis.get(f'numberplate.predict.{sourceId}')
                    
                    if registrationData != None:
                        registrationData = json.loads(registrationData)
                    
                    if registrationData == None:
                        registrationData = self.predictor.getRegistrationFromImages(images,rawRegistration)
                        self.redis.set(f'numberplate.predict.{sourceId}',json.dumps(registrationData))
                else:
                    registrationData = {
                        "registrationStatus":True,
                        "predictedRegistration":rawRegistration
                    }
                
                
                if registrationData["registrationStatus"] != None:
                    registrationData["number_plate_flag"] = not registrationData["registrationStatus"]
                else:
                    registrationData["number_plate_flag"] = 1
                    
                data["data"].update(registrationData)
                
                print(data)
                
                self.producer.produce_message(data)
            except Exception as e:
                print(f'error : {str(e)}')
                
                log = {}
                
                log["sourceUrl"] = data["data"]["sourceUrl"]
                log["service"] = self.topics.LISTING_PREDICT_NUMBERPLATE.value
                log["errorMessage"] = traceback.format_exc()
                
                self.logs_producer.produce_message({
                    "eventType":"insertLog",
                    "data":log
                })
                
                
                
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()