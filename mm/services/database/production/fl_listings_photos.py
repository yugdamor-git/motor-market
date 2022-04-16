from Database import Database

from topic import producer,consumer


class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.database.fllistingphotos'
        
        self.db = Database()
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
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
                    self.db.recInsert("fl_listings_photos",tmp)
                    
                self.db.disconnect()
                
            except Exception as e:
                print(f'error : {str(e)}')
                
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()