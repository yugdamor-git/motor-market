import pulsar
import json
from .constants import URI

class Producer:
    def __init__(self,topic):
        
        print("producer init")
        
        print(f'topic : {topic}')
        
        self.topic = topic
        
        self.uri =  URI
        
        self.client = pulsar.Client(self.uri)
        
        self.producer = self.client.create_producer(self.topic)
    
    def produce(self,data):
        
        self.producer.send(
            json.dumps(data).encode("utf-8")
        )
    
    def __del__(self):
        self.client.close()