import pulsar
import json
from .constants import URI

class Consumer:
    def __init__(self,topic):
        
        print("producer init")
        
        print(f'topic : {topic}')
        
        self.topic = topic
        
        self.uri = URI
        
        self.client = pulsar.Client(self.uri)
        
        self.consumer = self.client.subscribe(self.topic,f'{self.topic.split(".")[-1]}-subscription',pulsar.ConsumerType.Shared)
    
    def consume(self):
        
        message = self.consumer.receive()
        print(f'message id : {message.message_id()}')
        data = json.loads(message.data())
            
        self.consumer.acknowledge(message)
            
        return data
    
    def __del__(self):
        self.client.close()