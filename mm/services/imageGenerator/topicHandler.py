from topic import producer,consumer

from imageGenerator import imageGenerator

import traceback

class topicHandler:
    def __init__(self):
        print("image prediction handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.generate.image'

        self.publish_fl_listings = "motormarket.scraper.autotrader.listing.database.fllistings"
        
        self.publish_fl_listing_photos = "motormarket.scraper.autotrader.listing.database.fllistingphotos"
        
        logsTopic = "motormarket.scraper.logs"
        
        self.generator = imageGenerator()
            
        self.logsProducer = producer.Producer(logsTopic)
        
        self.producerFlListings = producer.Producer(self.publish_fl_listings)
        
        self.producerFlListingPhotos = producer.Producer(self.publish_fl_listing_photos)
        
        self.consumer = consumer.Consumer(self.subscribe)
    
    
      
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                images = data["data"]["images"]
                
                websiteId = data["data"]["websiteId"]
                
                listingId = data["data"]["id"]
                
                upsert = data["data"]["upsert"]
                
                status = data["data"]["status"]
                
                images = self.generator.generateImages(images,websiteId,listingId)
                
                tmp = []
                
                for item in images:
                    if item["status"] == False:
                        continue
                        # log that image was not processed with error message
                    if item["exists"] == True:
                        continue
                    
                    tmp.append(item)
                    
                if len(tmp) > 0:
                    # insert into fl listing photos
                    data["data"]["images"] = tmp
                    
                    self.producerFlListingPhotos.produce(data)
                    
                    # update thumbnail if it's insert
                    
                    where = {
                        "ID":id
                    }
                    
                    mainPhoto = None
                    
                    for index,img in enumerate(tmp):
                        if index == img["position"]:
                            mainPhoto = img["thumb"]["path"]
                            break
                    
                    if mainPhoto == None:
                        mainPhoto = tmp[0]["thumb"]["path"]
                    
                    what = {
                    }
                    
                    
                    if upsert == "insert":
                        
                        data["event"] = "update"
                        
                        what["Main_photo"] = mainPhoto
                        what["Status"] = "active"
                        
                        eventData = {
                            "what":what,
                            "where":where
                        }
                        
                        data["eventData"] = eventData
                        
                        self.producerFlListings.produce(data)
                        
                        # update thumbnail in fl_listings and update status
                
            except Exception as e:
                print(f'error : {str(e)}')
                
                traceback.print_exc()
                
                log = {}
                log["errorMessage"] = str(e)
                log["service"] = "services.machine.learning.image"
                
                self.logsProducer.produce(log)
                
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()