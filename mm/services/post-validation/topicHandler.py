
import sys

sys.path.append("/libs")

from pulsar_manager import PulsarManager


from validation import Validation

import traceback


class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        pulsar_manager = PulsarManager()
        
        self.topics = pulsar_manager.topics
        
        self.consumer = pulsar_manager.create_consumer(pulsar_manager.topics.LISTING_POSTVALIDATION)
        
        self.producer = pulsar_manager.create_producer(pulsar_manager.topics.LISTING_PREDICT_NUMBERPLATE)
        
        self.logs_producer = pulsar_manager.create_producer(pulsar_manager.topics.LOGS)
        
        self.validator = Validation(self.logs_producer)
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume_message()
                
                if self.validator.validate(data["data"]) == False:
                    print('we are not taking this listing')
                    continue
                
                print(data)
                
                self.producer.produce_message(data)
                
                # break
                
            except Exception as e:
                print(f'error : {str(e)}')
                
                log = {}
                
                log["sourceUrl"] = data["data"]["sourceUrl"]
                log["service"] = self.topics.LISTING_POSTVALIDATION
                log["errorMessage"] = traceback.format_exc()
                
                self.logs_producer.produce_message({
                    "eventType":"insertLog",
                    "data":log
                })
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()