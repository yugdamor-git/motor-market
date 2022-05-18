import json
import sys

sys.path.append("/libs")

from Database import Database

from pulsar_manager import PulsarManager

import traceback

class topicHandler:
    def __init__(self):
        print("topic handler init")
        
        pulsar_manager = PulsarManager()
        
        self.topics = pulsar_manager.topics
        
        self.logs_producer = pulsar_manager.create_producer(pulsar_manager.topics.LOGS)
        
        self.fl_listingphotos_update_consumer = pulsar_manager.create_consumer(pulsar_manager.topics.FL_LISTING_PHOTOS_INSERT)
        
        self.db = Database()
        
    def handle_insert_event(self,data):
        
        self.db.connect()
        
        images = data["data"]["images"]
                
        id = data["data"]["ID"]
        
        for img in images:
            tmp = {}
            
            tmp["Listing_ID"] = id
            
            tmp["Position"] = img["position"]
            
            tmp["Photo"] = img["large"]["path"]
            
            tmp["Thumbnail"] = img["thumb"]["path"]
            
            tmp["Original"] = img["org"]["path"]
            
            tmp["Status"] = "active"
            
            tmp["Type"] = "picture"
            
            tmp["create_ts"] = {"func":"now()"}
            
            tmp["delete_banner_flag"] = 0
            
            tmp["approved_from_dashboard"] = 1
            
            try:
                print(tmp)
                self.db.recInsert("fl_listing_photos",tmp)
            except Exception as e:
                print(f'error : {str(e)}')

        self.db.disconnect()
            
    def main(self):
        
        while True:
            try:
                message =  self.fl_listingphotos_update_consumer.consume_message()
                
                source_url = message["data"].get("sourceUrl")
                
                self.handle_insert_event(message)
                
            except Exception as e:
                print(f'error : {str(e)}')
                
                log = {}
                
                log["sourceUrl"] = source_url
                
                log["service"] = self.topics.FL_LISTING_PHOTOS_INSERT.value
                
                log["errorMessage"] = traceback.format_exc()
                
                self.logs_producer.produce_message({
                    "eventType":"insertLog",
                    "data":log
                })

if __name__ == "__main__":
    th = topicHandler()
    th.main()