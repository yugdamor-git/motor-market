
from topic import producer,consumer
from handler import Handler
from redisHandler import redisHandler
import json

class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.predict.numberplate'
        
        self.publish = 'motormarket.scraper.autotrader.listing.post.calculation'

        logsTopic = "motormarket.scraper.logs"
        
        self.predictor = Handler()
        
        self.redis = redisHandler()
    
        self.logsProducer = producer.Producer(logsTopic)
        
        self.producer = producer.Producer(self.publish)
        
        self.consumer = consumer.Consumer(self.subscribe)
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                images = data["data"]["images"]
                
                rawRegistration = data["data"]["registration"]
                
                sourceId = data["data"]["sourceId"]
                
                registrationData = self.redis.get(f'numberplate.predict.{sourceId}')
                
                if registrationData != None:
                    registrationData = json.load(registrationData)
                
                if registrationData == None:
                    registrationData = self.predictor.getRegistrationFromImages(images,rawRegistration)
                    self.redis.set(f'numberplate.predict.{sourceId}',json.dumps(registrationData))
                    
                self.producer.produce(data)
                
            except Exception as e:
                print(f'error : {str(e)}')
                log = {}
                log["errorMessage"] = str(e)
                log["service"] = "services.calculation"
                
                self.logsProducer.produce(log)
                
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()