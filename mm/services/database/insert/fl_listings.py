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
        
        self.fl_listings_update_consumer = pulsar_manager.create_consumer(pulsar_manager.topics.FL_LISTINGS_INSERT)
        
        self.generate_image_producer = pulsar_manager.create_producer(pulsar_manager.topics.GENERATE_IMAGE)
        
        self.db = Database()
    
    def load_column_map_json(self):
        data = None
        with open("insert_column_mapping.json","r") as f:
            data = json.loads(f)
        return data
    
    def map_columns(self,data):
        columnMapping = self.load_column_map_json()
        
        mappedData = {}
        
        dataTmp = {}
        
        dataTmp.update(data["data"])
        dataTmp.update(data["data"]["pcpapr"])
        dataTmp.update(data["data"]["ltv"])
        
        for item in columnMapping:
            key = item["key"]
            val = item["value"]
            if key in dataTmp:
                mappedData[val] = dataTmp[key]
                    
        return mappedData
        
    def increase_insert_count(self):
        self.logs_producer.produce_message(
                {
                    "eventType":"listingCount",
                    "data":{
                            "countFor":"insert"
                        }
                    }
                )
        
    def handle_insert_event(self,data):
        
        websiteId = data["data"]["websiteId"]
                    
        sourceId = data["data"]["sourceId"]
        
        if sourceId == None:
            return
        
        where = {
            "Website_ID":websiteId,
            "sourceId":sourceId
        }
        
        self.db.connect()
        
        try:
            records = self.db.recSelect("fl_listings",where)
            
            if len(records) == 0:
                
                mappedData = self.map_columns(data)
                
                mappedData["updated_at"] = {"func":"now()"}
                
                mappedData["Status"] = "to_parse"
                
                if data["data"]["registrationStatus"] == False:
                    mappedData["Status"] = "pending"
                
                if data["data"]["ltv"]["ltvStatus"] == 0:
                    mappedData["Status"] = "pending"
                    
                id = self.db.recInsert("fl_listings",mappedData)
                make = mappedData["make"]
                model = mappedData["model"]
                title = mappedData["title"]
                
                mmUrl = self.urlGenerator.generateMMUrl(make,model,title,id)
                
                data["data"]["status"] = "to_parse"
                
                data["data"]["mmUrl"] = mmUrl
                
                data["data"]["upsert"] = "insert"
                
                self.increase_insert_count()
                
                self.generate_image_producer.produce_message(data)
                
        except Exception as e:
            print(f'error : {str(e)}')

        self.db.disconnect()
            
    def main(self):
        
        while True:
            try:
                message =  self.fl_listings_update_consumer.consume_message()
                source_url = message["data"].get("sourceUrl")
                self.handle_update_event(message)
                
            except Exception as e:
                print(f'error : {str(e)}')
                
                log = {}
                
                log["sourceUrl"] = source_url
                
                log["service"] = self.topics.FL_LISTINGS_INSERT.value
                
                log["errorMessage"] = traceback.format_exc()
                
                self.logs_producer.produce_message({
                    "eventType":"insertLog",
                    "data":log
                })

if __name__ == "__main__":
    th = topicHandler()
    th.main()