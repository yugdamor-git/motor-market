
import pulsar
import json

from validation import Validation

class topicHandler:
    def __init__(self):
        print("make model topic handler init")
        
        self.topicSubscribe = 'motormarket.scraper.autotrader.listing.validation'
        
        self.topicPublish = 'motormarket.scraper.autotrader.listing.calculation'
        
        self.topicLogs = 'motormarket.scraper.autotrader.listing.log'
        
        self.uri = 'pulsar://pulsar'
        
        self.validation = Validation()
        
        self.client = pulsar.Client(self.uri)
        
        self.producer = self.client.create_producer(self.topicPublish)
        
        self.consumer = self.client.subscribe(self.topicSubscribe, 'validate-subscription')
        
    def consume(self):
        
        while True:
            message = self.consumer.receive()
            
            try:
                data = json.loads(message.data)
                
                self.consumer.acknowledge(message)
                
                if self.validation.validate(data["data"]) == False:
                    continue
                    # log error here
                
                
                self.produce(data)
                
            except Exception as e:
                print(f'error : {str(e)}')
    
    def produce(self,data):
        
        self.producer.send(
            json.dumps(data).encode("utf-8")
        )
        
    def __del__(self):
        self.client.close()
        
        
        
        