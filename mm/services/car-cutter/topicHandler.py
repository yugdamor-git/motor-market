import sys
sys.path.append("/libs")


from pulsar_manager import PulsarManager
from car_cutter_api import CarCutter
import traceback

class topicHandler:
    def __init__(self):
        print("image prediction handler init")
        
        pulsar_manager = PulsarManager()
        
        self.topics = pulsar_manager.topics
        
        self.consumer = pulsar_manager.create_consumer(pulsar_manager.topics.CAR_CUTTER)

        self.generate_image_producer = pulsar_manager.create_producer(pulsar_manager.topics.GENERATE_IMAGE)
        
        self.logs_producer = pulsar_manager.create_producer(pulsar_manager.topics.LOGS)
        
        self.car_cutter = CarCutter()
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume_message()
                
                images = data["data"]["car_cutter_images"]
                
                websiteId = data["data"]["websiteId"]
                
                listingId = data["data"]["ID"]
                
                cc_total_img = 0
                
                data["data"]["car_cutter"] = 0
                
                if data["data"]["ltvStatus"] == 1 and data["data"]["sourcePrice"] > 10000:
                    processed_images,cc_total_img = self.car_cutter.process_images(images,websiteId,listingId)
                    data["data"]["images"] = processed_images
                    data["data"]["car_cutter"] = 1
                
                data["data"]["cc_total_img"] = cc_total_img

                self.generate_image_producer.produce_message(data)
            
            except Exception as e:
                print(f'error : {str(e)}')
                log = {}
                log["sourceUrl"] = data["data"]["sourceUrl"]
                log["service"] = self.topics.CAR_CUTTER.value
                log["errorMessage"] = traceback.format_exc()
                
                self.logs_producer.produce_message({
                    "eventType":"insertLog",
                    "data":log
                })
                
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()