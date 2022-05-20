import json
import sys

sys.path.append("/libs")

from Database import Database

from pulsar_manager import PulsarManager

from MMUrlGenerator import MMUrlGenerator

import traceback

class topicHandler:
    def __init__(self):
        print("topic handler init")
        
        pulsar_manager = PulsarManager()
        
        self.topics = pulsar_manager.topics
        
        self.logs_producer = pulsar_manager.create_producer(pulsar_manager.topics.LOGS)
        
        self.fl_listings_update_consumer = pulsar_manager.create_consumer(pulsar_manager.topics.FL_LISTINGS_INSERT)
        
        self.generate_image_producer = pulsar_manager.create_producer(pulsar_manager.topics.GENERATE_IMAGE)
        
        self.at_urls_update_producer = pulsar_manager.create_producer(self.topics.AT_URLS_UPDATE)
        
        self.columnMapping = self.load_column_map_json()
        
        self.urlGenerator = MMUrlGenerator()
        
        self.db = Database()
    
    def load_column_map_json(self):
        data = None
        with open("insert_column_mapping.json","r") as f:
            data = json.loads(f.read())
        return data
    
    def map_columns(self,data):
        
        mappedData = {}
        
        dataTmp = {}
        
        dataTmp.update(data["data"])
        dataTmp.update(data["data"]["pcpapr"])
        dataTmp.update(data["data"]["ltv"])
        
        for item in self.columnMapping:
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
        
    def handleAtUrl(self,status,id,listingId,registrationStatus):
        
        what = {
            
        }
        
        if status == "to_parse":
            what["listing_status"] = "active"
        else:
            what["listing_status"] = status
        what["scraped"] = 1
        what["listing_id"] = id
        what["updated_at"] = {"func":"now()"}
        
        if registrationStatus == 1:
            what["number_plate_flag"] = 0
        elif registrationStatus == None:
            what["number_plate_flag"] = 2
        else:
            what["number_plate_flag"] = 2
        
        
        where = {
            "id":listingId
        }
        
        
        self.at_urls_update_producer.produce_message({
            "data":{
                "what":what,
                "where":where,
            }
           
        })
        
    def handle_insert_event(self,data):
        
        websiteId = data["data"]["websiteId"]
                    
        sourceId = data["data"]["sourceId"]
        
        scraperName = data["data"].get("scraperName")
        
        if sourceId == None:
            return
        
        where = {
            "Website_ID":websiteId,
            "sourceId":sourceId
        }
        
        self.db.connect()
        
        mappedData = self.map_columns(data)
        
        try:
            records = self.db.recSelect("fl_listings",where)
            
            if len(records) == 0:
                
                mappedData["updated_at"] = {"func":"now()"}
                
                mappedData["Status"] = "to_parse"
                
                if data["data"]["registrationStatus"] == False:
                    mappedData["Status"] = "pending"
                
                if data["data"]["ltv"]["ltvStatus"] == 0:
                    mappedData["Status"] = "pending"
                    
                id = self.db.recInsert("fl_listings",mappedData)
                
                data["data"]["ID"] = id
                
                make = mappedData["make"]
                model = mappedData["model"]
                title = mappedData["title"]
                
                mmUrl = self.urlGenerator.generateMMUrl(make,model,title,id)
                
                data["data"]["status"] = "to_parse"
                
                data["data"]["mmUrl"] = mmUrl
                
                data["data"]["upsert"] = "insert"
                
                self.increase_insert_count()
                
                print(data)
                
                self.generate_image_producer.produce_message(data)
            
            if scraperName == "url-scraper":
                status = None
                if "Status" in mappedData:
                    status = mappedData["Status"]
                elif "status" in mappedData:
                    status = mappedData["status"]
                
                listingId = data["data"].get("listingId")
                
                registrationStatus = data["data"].get("registrationStatus",None)
                
                self.handleAtUrl(status,data["data"]["ID"],listingId,registrationStatus)
                
        except Exception as e:
            print(f'error : {str(e)}')

        self.db.disconnect()
            
    def main(self):
        
        while True:
            try:
                message =  self.fl_listings_update_consumer.consume_message()
                print(message)
                source_url = message["data"].get("sourceUrl")
                self.handle_insert_event(message)
                
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