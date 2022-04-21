from topic import producer,consumer

from imageGenerator import imageGenerator

import traceback

class topicHandler:
    def __init__(self):
        print("image prediction handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.generate.image'

        self.publish_fl_listings = "motormarket.scraper.autotrader.listing.database.production"
        
        self.publish_fl_listing_photos = "motormarket.scraper.autotrader.listing.database.production.photo"
        
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
                
                images = self.generator.processListing(images,websiteId,listingId)
                
                tmp = []
                
                for index,item in enumerate(images):
                    if item["status"] == False:
                        images.pop(index)
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
                        "id":listingId
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
                        
                        what["Main_photo"] = mainPhoto
                        
                        what["mm_product_url"] = data["data"]["mmUrl"]
                        
                        if status in ["to_parse","expired"]:
                            what["Status"] = "active"
                        
                        eventData = {
                            "what":what,
                            "where":where
                        }
                        
                        data["event"] = "update"
                        data["eventData"] = eventData
                        
                        self.producerFlListings.produce(data)
                else:
                    where = {
                                "ID":listingId
                            }
                    
                    if len(images) > 0:
                        if status in ["to_parse","expired"]:
                            
                            what = {
                                "Status":"active"
                            }
                            
                            eventData = {
                                "what":what,
                                "where":where
                            }
                            data["event"] = "update"
                            data["eventData"] = eventData
                            
                            self.producerFlListings.produce(data)
                        
                    else:
                        what = {
                                "Status":"expired"
                            }
                        
                        eventData = {
                                "what":what,
                                "where":where
                            }
                        data["event"] = "update"
                        data["eventData"] = eventData
                        
                        self.producerFlListings.produce(data)
                        
                        
                        
                        # update thumbnail in fl_listings and update status
                
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