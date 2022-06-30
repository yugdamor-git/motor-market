import sys

sys.path.append("/libs")

from pulsar_manager import PulsarManager

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
        
        status,listings,dealers = self.marketcheck.parse_csv()
        
        if status == False:
            return
        
        for listing in listings:
            self.producer.produce_message({
                "data":listing
            })
            break
            
        
if __name__ == "__main__":
    th = topicHandler()
    th.main()