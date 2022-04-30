from Database import Database

from topic import producer,consumer

from MMUrlGenerator import MMUrlGenerator

import traceback


class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.database.production.photo'
        
        self.db = Database()
        
        logsTopic = "motormarket.scraper.logs"
    
        self.logsProducer = producer.Producer(logsTopic)
        
        self.consumer = consumer.Consumer(self.subscribe)
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                print(data)
                
                self.db.connect()
                
                images = data["data"]["images"]
                
                id = data["data"]["id"]
                
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
                        self.db.recInsert("fl_listing_photos",tmp)
                    except Exception as e:
                        print(f'error : {str(e)}')
                        
                self.db.disconnect()
                
            except Exception as e:
                print(f'error : {str(e)}')
                
                log = {}
                
                log["sourceUrl"] = data["data"]["sourceUrl"]
                
                log["service"] = self.subscribe
                
                log["errorMessage"] = traceback.format_exc()
                
                self.logsProducer.produce({
                    "eventType":"insertLog",
                    "data":log
                })
                
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()