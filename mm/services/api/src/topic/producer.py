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
        
        
    
    def produce(self,data):
        producer = self.client.create_producer(self.topic)
        producer.send(
            json.dumps(data).encode("utf-8")
        )
        self.client.close()