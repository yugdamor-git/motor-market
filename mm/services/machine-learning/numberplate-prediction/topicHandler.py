
from topic import producer,consumer
from handler import Handler
from redisHandler import redisHandler
import json
import traceback

class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.predict.numberplate'
        
        self.publish = 'motormarket.scraper.autotrader.listing.post.calculation'

        logsTopic = "motormarket.scraper.logs"
        
        
        
        self.predictor = Handler()
        
        self.redis = redisHandler()
        
        self.producer = producer.Producer(self.publish)
        
        self.logsProducer = producer.Producer(logsTopic)
        
        self.consumer = consumer.Consumer(self.subscribe)
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
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