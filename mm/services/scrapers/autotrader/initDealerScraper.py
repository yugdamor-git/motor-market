
from Database import Database
from topic import producer,consumer

class init_dealer_scraper:
    def __init__(self):
        print(f'listing init_dealer_scraper init')

        self.websiteId = 17
        
        self.maxRetry = 5
        
        self.database = Database()
        
        self.publish = 'motormarket.scraper.autotrader.dealer.scrape'
        
        self.producer = producer.Producer(self.publish)
        
        
    def getDealers(self):
        dealers = []
        self.database.connect()
        for retry in range(0,self.maxRetry):
            try:
                dealers = self.database.recCustomQuery(f'SELECT dealer_id FROM `fl_dealer_scraper` WHERE Status="active"')
                break
            except Exception as e:
                print(f'error : {__file__} -> {str(e)}')
            
        self.database.disconnect()
        
        return dealers
    
    def getBlackListedDealers(self):
        blacklist_dealers = {}
        self.database.connect()
        for retry in range(0,self.maxRetry):
            try:
                dealers = self.database.recCustomQuery(f'SELECT dealer_id FROM `fl_dealer_blacklist`')
                for dealer in dealers:
                    blacklist_dealers[str(dealer["dealer_id"])] = 1
                break
            except Exception as e:
                print(f'error : {__file__} -> {str(e)}')
            
        self.database.disconnect()
        
        return blacklist_dealers
    
    
    def main(self):
        
        dealers = self.getDealers()
        
        blacklist_dealers = self.getBlackListedDealers()
        
        for dealer in dealers:
            dealerId = str(dealer["dealer_id"])
            if not dealerId in blacklist_dealers:
                
                tmp = {}
                
                tmp["dealerId"] = dealerId
                
                self.producer.produce({
                    "data":tmp
                })
                
                print(f'not blacklisted : {dealerId}')
                
            else:
                print(f'blacklisted : {dealerId}')

if __name__ == "__main__":
    v = init_dealer_scraper()
    v.main()