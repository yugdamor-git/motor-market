
from Database import Database
from topic import producer,consumer

class validator:
    def __init__(self):
        print(f'listing validator init')

        self.websiteId = 17
        
        self.maxRetry = 5
        
        self.database = Database()
        
        self.publish = 'motormarket.scraper.autotrader.listing.database.finder'
        
        self.producer = producer.Producer(self.publish)
        
        
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
            
            tmp["skipFinder"] = True
            
            tmp["websiteId"] = self.websiteId
            
            
            self.producer.produce({
                "data":tmp
            })


if __name__ == "__main__":
    v = validator()
    v.main()