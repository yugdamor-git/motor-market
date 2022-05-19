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
        
        self.fl_listings_find_consumer = pulsar_manager.create_consumer(pulsar_manager.topics.FL_LISTINGS_FIND)
        
        self.producer = pulsar_manager.create_producer(pulsar_manager.topics.AUTOTRADER_LISTING_SCRAPER)
        
        self.at_urls_update_producer = pulsar_manager.create_producer(self.topics.AT_URLS_UPDATE)
        
        self.db = Database()
        
    def handle_find_event(self,data):
        
        self.db.connect()
        
        source_id = data["data"].get("sourceId")
        
        scraper_name = data["data"].get("scraperName",None)

        try:
            
            result = self.db.recCustomQuery(f'SELECT ID,dealer_id,Status,predictedMake,predictedModel,engineCylindersCC,mileage,built,registrationStatus,predictedRegistration FROM fl_listings WHERE sourceId="{source_id}"')
            
            
            if len(result) > 0:
                
                if scraper_name == "url-scraper":
                    what = {
                        "listing_id":result[0]["ID"],
                        "listing_status":result[0]["Status"],
                        "scraped":1,
                        "updated_at":{
                            "func":"now()"
                        },
                        "errorMessage":"listing already exists."
                    }
                    
                    if result[0]["Status"] == "pending":
                        what["number_plate_flag"] = 2
                    
                    where = {
                        "id":data["data"].get("listingId")
                    }
                    
                    self.at_urls_update_producer.produce_message({
                        "data":{
                            "what":what,
                            "where":where
                        }
                    })
                    
                
                if result[0]["Status"] in ["sold","manual_expire","pending","approval","to_parse"]:
                    return
                
                data["data"]["scraperType"] = "validator"
                
                data["data"].update(result[0])
            else:
                data["data"]["scraperType"] = "normal"
            
            self.producer.produce_message(data)
            
        except Exception as e:
            print(f'error : {str(e)}')
        
        self.db.disconnect()
        
    def main(self):
        
        while True:
            try:
                message =  self.fl_listings_find_consumer.consume_message()
                
                skip_find = message["data"].get("skip_find",False)
                
                if skip_find == True:
                    self.producer.produce_message(message)
                    continue
                    
                
                source_url = message["data"].get("sourceUrl")
                
                print(f'processing : {source_url}')
                
                self.handle_find_event(message)
                
                

            except Exception as e:
                print(f'error : {str(e)}')
                
                log = {}
                
                log["sourceUrl"] = source_url
                
                log["service"] = self.topics.FL_LISTINGS_FIND.value
                
                log["errorMessage"] = traceback.format_exc()
                
                self.logs_producer.produce_message({
                    "eventType":"insertLog",
                    "data":log
                })

if __name__ == "__main__":
    th = topicHandler()
    th.main()