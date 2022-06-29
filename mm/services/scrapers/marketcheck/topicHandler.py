import sys

sys.path.append("/libs")

from pulsar_manager import PulsarManager

from transform import Transform

import traceback

from market_check import MarketCheck


class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        pulsar_manager = PulsarManager()
        
        self.topics = pulsar_manager.topics
        
        self.producer = pulsar_manager.create_producer(pulsar_manager.topics.LISTING_TRANSFORM)
        
        self.logs_producer = pulsar_manager.create_producer(pulsar_manager.topics.LOGS)
        
        self.transform = Transform()
        
        self.marketcheck = MarketCheck()
        
    def main(self):
        print("listening for new messages")
        
        listings = self.marketcheck.parse_csv()
        
        for listing in listings:
            self.producer.produce_message({
                "data":listing
            })
        
if __name__ == "__main__":
    th = topicHandler()
    th.main()