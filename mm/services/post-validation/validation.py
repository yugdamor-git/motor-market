
from Database import Database

class Validation:
    def __init__(self,logsProducer) -> None:
        print("validation init")
        
        self.logsProducer = logsProducer
        
        self.database = Database()
        
    def validate(self,data):
        
        sourceUrl = data["sourceUrl"]
        
       
        images = data["images"]
        
        log = {}
        log["service"] = "motormarket.scraper.autotrader.listing.postvalidation"
        log["sourceUrl"] = sourceUrl

        status,message = self.imageValidation(images)
        if status == False:
            log["errorMessage"] = message
            
            self.logsProducer.produce_message({
                    "eventType":"insertLog",
                    "data":log
                })
            
            return False
        
        return True
    
    
    def imageValidation(self,images):
        if len(images) == 0:
            return False,"there are no images."
        
        return True,None