from dealerScraper import dealerScraper

from Database import Database

class dealerListingValidator:
    
    def __init__(self) -> None:
        
        self.db = Database()
        
        self.ds = dealerScraper()
        
    
    def get_all_listing(self):
        
        retry = 5
        listings = []
        self.db.connect()
        for i in range(0,retry):
            try:
                listings = self.db.recCustomQuery("SELECT sourceId,dealer_id FROM fl_listings WHERE Status='active'")
                break
            except:
                pass
        self.db.disconnect()
        
        return listings
    
    def group_by_dealer_id(self,listings):
        groupByDealerId = {}

        for listing in listings:
            if not listing["dealer_id"] in groupByDealerId:
                groupByDealerId[listing["dealer_id"]] = []
            groupByDealerId[listing["dealer_id"]].append(listing["sourceId"])
        
        return groupByDealerId
    

    def main(self):
        listings = self.get_all_listing()
        
        groupByDealerId = self.group_by_dealer_id(listings)
        
        for dealerId in groupByDealerId:
            oldListingIds = {str(id) for id in groupByDealerId[dealerId]}
            newlistingIds = {str(id) for id in self.ds.scrapeByDealerId(dealerId)}

            for old_id in oldListingIds:
                if not old_id in newlistingIds:
                    print(f'expired : {old_id}')
                    break

if __name__ == "__main__":
    dlv = dealerListingValidator()
    dlv.main()