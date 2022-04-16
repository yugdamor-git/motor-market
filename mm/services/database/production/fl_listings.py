from Database import Database

from topic import producer,consumer


class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.database.fllistings'
        # table name
        
        self.publish = 'motormarket.scraper.autotrader.listing.generate.image'

        self.db = Database()
        
        self.producer = producer.Producer(self.publish)
        
        self.consumer = consumer.Consumer(self.subscribe)
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                self.db.connect()
                
                event = data.get("event",None)
                
                if event != None:
                    if event == "update":
                        eventData = data["eventData"]
                        what = eventData["what"]
                        where = eventData["where"]
                        self.db.recUpdate("fl_listings",what,where)
                        
                        continue
                
                websiteId = data["data"]["websiteId"]
                
                sourceUrl = data["data"]["sourceUrl"]
                
                where = {
                    "websiteId":websiteId,
                    "product_url":sourceUrl
                }
                
                records = self.db.recSelect("fl_listings",where)
                
                id = None
                
                upsert = None
                
                if len(records) > 0:
                    # update
                    id = records[0]["ID"]
                    mappedData = self.mapColumnsUpdate(data)
                    where = {
                        "ID":id
                    }
                    
                    status = records[0]["Status"] 
                    
                    if status == "sold":
                        continue
                    elif status == "pending":
                        self.db.recUpdate("fl_listings",mappedData,where)
                    elif status == "manual_expire":
                        continue
                    elif status == "to_parse":
                        self.db.recUpdate("fl_listings",mappedData,where)
                    elif status == "active":
                        self.db.recUpdate("fl_listings",mappedData,where)
                    data["data"]["status"] = status
                    upsert = "update"
                    
                else:
                    # insert
                    mappedData = self.mapColumnsInsert(data)
                    mappedData["Status"] = "to_parse"
                    id = self.db.recInsert("fl_listings",mappedData)
                    data["data"]["status"] = "to_parse"
                    upsert = "insert"
                
                data["data"]["upsert"] = upsert
                self.db.disconnect()
                
                data["data"]["id"] = id
                        
                print(data)
                
                self.producer.produce(data)
                
            except Exception as e:
                print(f'error : {str(e)}')
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()