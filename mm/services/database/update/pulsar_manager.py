import pulsar
import json
from enum import Enum

URI='pulsar://pulsar'

class Topics(Enum):
    LOGS = "motormarket.scraper.logs"
    FL_LISTINGS_UPDATE = 'motormarket.database.fllistings.update'

class Producer:
    def __init__(self,producer_client) -> None:
        self.producer_client = producer_client
    
    def produce_message(self,data):
        
        self.producer_client.send(
            json.dumps(data).encode("utf-8")
        )
        
class Parser:
    def __init__(self) -> None:
        pass
    
    def json_parser(self,data):
        return json.loads(data)
    

class Consumer:
    def __init__(self,consumer_client) -> None:
        self.consumer_client = consumer_client
        self.parser = Parser()
    
    def print(self,message):
        print(message)
    
    def consume_message(self):
        message = self.consumer_client.receive()
        self.consumer_client.acknowledge(message)
        self.print(f'message_id : {message.message_id()}')
        return self.parser.json_parser(message)
    
class PulsarManager:
    def __init__(self):
        self.topics = Topics
        self.uri =  URI
        self.client = pulsar.Client(self.uri)
        
    def create_producer(self,topic:Topics):
        return Producer(self.client.create_producer(topic.value))
    
    def create_consumer(self,topic:Topics):
        return Consumer(self.client.subscribe(topic.value,f'{topic.name}-subscription',pulsar.ConsumerType.Shared))