import sys

sys.path.append("/libs")

from Database import Database

from pulsar_manager import PulsarManager

import traceback

import json

class topicHandler:
    def __init__(self):
        print("topic handler init")
        
        pulsar_manager = PulsarManager()
        
        self.topics = pulsar_manager.topics
        
        self.logs_producer = pulsar_manager.create_producer(pulsar_manager.topics.LOGS)
        
        self.fl_listings_update_consumer = pulsar_manager.create_consumer(pulsar_manager.topics.FL_LISTINGS_UPDATE)
        
        self.at_urls_update_producer = pulsar_manager.create_producer(self.topics.AT_URLS_UPDATE)
        
        self.columnMapping = self.load_column_map_json()
        
        self.db = Database()
    
    def load_column_map_json(self):
        data = None
        with open("update_column_mapping.json","r") as f:
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
    
    def increase_expired_count(self):
        self.logs_producer.produce_message(
                {
                    "eventType":"listingCount",
                    "data":{
                            "countFor":"expired"
                        }
                    }
                )
    
    def increase_update_count(self):
        self.logs_producer.produce_message(
                {
                    "eventType":"listingCount",
                    "data":{
                            "countFor":"update"
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
    
    def handle_update_event(self,what,where):
        self.db.connect()
        
        what["updated_at"] = {"func":"now()"}
        
        if "Status" in what:
            if what["Status"] == "expired":
                self.increase_expired_count()
            else:
                self.increase_update_count()
        else:
            self.increase_update_count()
            
        if "status" in what:
            if what["status"] == "expired":
                self.increase_expired_count()
            else:
                self.increase_update_count()
        else:
            self.increase_update_count()
        print(where)
        print(what)
        try:
            self.db.recUpdate("fl_listings",what,where)
        except Exception as e:
            print(f'error : {str(e)}')
        
        self.db.disconnect()
            
    def main(self):
        
        while True:
            try:
                message =  self.fl_listings_update_consumer.consume_message()
                
                data = message.get("data",None)
                
                what = data.get("what",None)
                
                where = data.get("where",None)
                
                self.handle_update_event(what,where)
                                
            except Exception as e:
                print(f'error : {str(e)}')
                
                log = {}
                
                log["sourceUrl"] = None
                
                log["service"] = self.topics.FL_LISTINGS_UPDATE.value
                
                log["errorMessage"] = traceback.format_exc()
                
                self.logs_producer.produce_message({
                    "eventType":"insertLog",
                    "data":log
                })

if __name__ == "__main__":
    th = topicHandler()
    th.main()