import pulsar
import json
from enum import Enum
import os

URI='pulsar://pulsar'

SCRAPER_NAME = os.environ.get("SCRAPER_NAME","")

class Topics(Enum):
    LOGS = "motormarket.scraper.logs"
    
    FL_LISTINGS_UPDATE = 'motormarket.database.fllistings.update'
    FL_LISTINGS_INSERT = 'motormarket.database.fllistings.insert'
    
    FL_LISTINGS_FIND = f'motormarket{SCRAPER_NAME}.database.fllistings.find'
    
    LISTING_TRANSFORM = f'motormarket{SCRAPER_NAME}.scraper.listing.transform'
    
    LISTING_PREVALIDATION = f'motormarket{SCRAPER_NAME}.scraper.listing.prevalidation'
    
    LISTING_POST_CALCULATION= f'motormarket{SCRAPER_NAME}.scraper.listing.postcalculation'
    
    LISTING_PREDICT_MAKEMODEL= f'motormarket{SCRAPER_NAME}.scraper.listing.predict.makemodel'
    
    FL_LISTING_PHOTOS_INSERT = 'motormarket.database.fllistingphotos.insert'
    
    AT_URLS_UPDATE = 'motormarket.database.aturls.update'
    
    GENERATE_IMAGE = 'motormarket.listing.generate.image'
    
    AUTOTRADER_LISTING_SCRAPER = 'motormarket.scraper.autotrader.listing.scrape'

class Producer:
    def __init__(self,producer_client) -> None:
        self.producer_client = producer_client
    
    def produce_message(self,data):
        
        self.producer_client.send(
            json.dumps(data).encode("utf-8")
        )

class Parser:
    def json_parser(self,message):
        return json.loads(message.data())
    

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