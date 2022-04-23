
import pulsar
import json

class pulsarHandler:
    def __init__(self) -> None:
        print("pulsar handler init")
        
        self.uri = 'pulsar://pulsar'
        
        self.topicScrape = "motormarket.scraper.autotrader.listing.scrape"
        
        self.client = pulsar.Client(self.uri)
        
    def produce(self,data,topic):
        
        producer = self.client.create_producer(topic)
        
        producer.send(
            json.dumps(data).encode("utf-8")
        )
        
    def __del__(self):
        self.client.close()