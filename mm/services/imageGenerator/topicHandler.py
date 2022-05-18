import sys
sys.path.append("/libs")


from pulsar_manager import PulsarManager

from imageGenerator import imageGenerator

import traceback

class topicHandler:
    def __init__(self):
        print("image prediction handler init")
        
        pulsar_manager = PulsarManager()
        
        self.topics = pulsar_manager.topics
        
        self.consumer = pulsar_manager.create_consumer(pulsar_manager.topics.GENERATE_IMAGE)

        self.fl_listings_update_producer = pulsar_manager.create_producer(pulsar_manager.topics.FL_LISTINGS_UPDATE)
        
        self.fl_listingphotos_producer = pulsar_manager.create_producer(pulsar_manager.topics.FL_LISTING_PHOTOS_INSERT)
        
        self.generator = imageGenerator()
        
        self.logs_producer = pulsar_manager.create_producer(pulsar_manager.topics.LOGS)
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume_message()
                
                images = data["data"]["images"]
                
                websiteId = data["data"]["websiteId"]
                
                listingId = data["data"]["ID"]
                
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
                    
                    self.fl_listingphotos_producer.produce_message(data)
                    # update thumbnail if it's insert
                    
                    where = {
                        "ID":listingId
                    }
                    
                    mainPhoto = None
                    
                    for index,_ in enumerate(tmp):
                        for img in tmp:
                            if index == img["position"]:
                                mainPhoto = img["thumb"]["path"]
                                break
                        if mainPhoto != None:
                            break
                    
                    if mainPhoto == None:
                        mainPhoto = tmp[0]["thumb"]["path"]
                    
                    what = {
                    
                    }
                        
                    what["Main_photo"] = mainPhoto
                    
                    what["mm_product_url"] = data["data"]["mmUrl"]
                    
                    if status in ["to_parse","expired"]:
                        what["Status"] = "active"
                    
                    if data["data"]["registrationStatus"] == False:
                        what["status"] = "pending"
                    
                    
                    
                    data["data"]["what"] = what
                    data["data"]["where"] = where
                    
                else:
                    where = {
                                "ID":listingId
                            }
                    
                    if len(images) > 0:
                        if status in ["to_parse","expired"]:
                            
                            what = {
                                "Status":"active"
                            }
                            
                            if data["data"]["registrationStatus"] == False:
                                what["status"] = "pending"
                            
                            data["data"]["what"] = what
                            data["data"]["where"] = where
                        
                    else:
                        what = {
                                "Status":"expired"
                            }
                        
                        data["data"]["what"] = what
                        
                        data["data"]["where"] = where
                            
                self.fl_listings_update_producer.produce_message(data)
            
            except Exception as e:
                print(f'error : {str(e)}')
                log = {}
                log["sourceUrl"] = data["data"]["sourceUrl"]
                log["service"] = self.topics.GENERATE_IMAGE.value
                log["errorMessage"] = traceback.format_exc()
                
                self.logs_producer.produce_message({
                    "eventType":"insertLog",
                    "data":log
                })
                
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()