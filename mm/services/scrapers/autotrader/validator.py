import sys

sys.path.append("/libs")

from pulsar_manager import PulsarManager

from datetime import datetime


from Database import Database

class validator:
    def __init__(self):
        print(f'listing validator init')

        self.websiteId = 17
        
        self.maxRetry = 5
        
        pulsar_manager = PulsarManager()
        
        self.topics = pulsar_manager.topics
        
        self.database = Database()
        
        self.producer = pulsar_manager.create_producer(self.topics.FL_LISTINGS_FIND)
        
    def getListings(self):
        listings = []
        self.database.connect()
        for retry in range(0,self.maxRetry):
            try:
                listings = self.database.recCustomQuery(f'SELECT ID,sourceId,scraperName,sourceUrl,dealer_id,Status,predictedMake,predictedModel,engineCylindersCC,mileage,built,registrationStatus,predictedRegistration FROM `fl_listings` WHERE Status="active" AND Website_ID={self.websiteId} AND sourceId IS NOT NULL')
                break
            except Exception as e:
                print(f'error : {__file__} -> {str(e)}')
            
        self.database.disconnect()
        
        return listings
    
    def main(self):
        
        listings = self.getListings()
        
        for listing in listings:
            tmp = listing.copy()
            
            tmp["skip_find"] = True
            
            tmp["websiteId"] = self.websiteId
            
            tmp["scraperType"] = "validator"
            
            self.producer.produce_message({
                "data":tmp
            })
            
            print(tmp["ID"])

def is_working_hour():
    start = 5
    end = 20
    now = datetime.now()
    
    if now.hour >= start and now.hour <= end:
        return True
    else:
        return False

if __name__ == "__main__":
    
    if is_working_hour() == True:
        v = validator()
        v.main()
    else:
        print(f'skipping execution, its not working hour... ')