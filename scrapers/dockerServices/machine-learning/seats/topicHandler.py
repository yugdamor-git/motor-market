from SeatPredictor import SeatPredictor
from redisHandler import redisHandler
import pulsar
import json


class topicHandler:
    def __init__(self):
        print("make model topic handler init")
        
        self.topicSubscribe = 'motormarket.scraper.autotrader.listing.predict.seat'
        
        self.topicPublish = 'motormarket.scraper.autotrader.listing.validation'
        
        self.predictor = SeatPredictor()
        
        self.redis = redisHandler()
        
        self.uri = 'pulsar://pulsar'
        
        self.client = pulsar.Client(self.uri)
        
        self.producer = self.client.create_producer(self.topicPublish)
        
        self.consumer = self.client.subscribe(self.topicSubscribe, 'make-model-prediction-subscription')
        
    def consume(self):
        
        while True:
            message = self.consumer.receive()
            
            try:
                data = json.loads(message.data)
                
                self.consumer.acknowledge(message)
                
                make = data["data"]["predictedMake"]
                model = data["data"]["predictedModel"]
                
                prediction = self.predictor.predict(make,model)
                
                data["data"].update(prediction)

                self.produce(prediction)
                
            except Exception as e:
                print(f'error : {str(e)}')
    
    def produce(self,data):
        
        self.producer.send(
            json.dumps(data).encode("utf-8")
        )
        
    def __del__(self):
        self.client.close()
        
        
        
        