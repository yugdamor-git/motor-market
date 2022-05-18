import sys

sys.path.append("/libs")

from pulsar_manager import PulsarManager

from transform import Transform

import traceback


class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        pulsar_manager = PulsarManager()
        
        self.topics = pulsar_manager.topics
        
        self.consumer = pulsar_manager.create_consumer(pulsar_manager.topics.LISTING_TRANSFORM)
        
        self.producer = pulsar_manager.create_producer(pulsar_manager.topics.LISTING_PREVALIDATION)
        
        self.logs_producer = pulsar_manager.create_producer(pulsar_manager.topics.LOGS)
        
        self.transform = Transform()

        self.post_calculation_producer = pulsar_manager.create_producer(pulsar_manager.topics.LISTING_POST_CALCULATION)
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume_message()
                
                print(f'processing : {data["data"]["sourceUrl"]}')
                
                scraperType = data["data"].get("scraperType")
                
                if scraperType == "validator":
                    transformedData = self.transform.transformValidatorData(data["data"])
                    data["data"].update(transformedData)
                    print(data)
                    self.post_calculation_producer.produce_message(data)
                    continue
                
                elif scraperType == "normal":
                    transformedData = self.transform.transformData(data["data"])
                
                    data["data"].update(transformedData)
                
                print(data)
                
                self.producer.produce_message(data)
                
            except Exception as e:
                print(f'error : {str(e)}')
                
                log = {}
                
                log["sourceUrl"] = data["data"]["sourceUrl"]
                log["service"] = self.topics.LISTING_TRANSFORM.value
                log["errorMessage"] = traceback.format_exc()
                
                self.logs_producer.produce_message({
                    "eventType":"insertLog",
                    "data":log
                })
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()