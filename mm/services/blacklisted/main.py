import sys

sys.path.append("/libs")

from Database import Database

class HandleBlackListedDealers:
    
    def __init__(self) -> None:
        
        self.db = Database()
        
    
    def execute_sql(self,sql_query):
        self.db.connect()
        result = []
        try:
            result = self.db.recCustomQuery(sql_query)
        except Exception as e:
            print(f'error : {str(e)}')
        
        self.db.disconnect()
        
        return result
    
    
    def get_blacklisted_dealers_id(self):
        dealers_id = {}
        
        sql_query = "SELECT dealer_id FROM `fl_dealer_blacklist` WHERE 1"
        
        dealers = self.execute_sql(sql_query)
        
        for dealer in dealers:
            dealers_id[str(dealer["dealer_id"])] = 1
        
        return dealers_id

    def get_active_listings(self):
        listings= []
        
        sql_query = "SELECT ID,dealer_id FROM `fl_listings` WHERE Status='active' AND Website_ID=17"
        
        listings = self.execute_sql(sql_query)
        
        for listing in listings:
            listing["dealer_id"] = str(listing["dealer_id"]).strip()
            
        return listings

    def get_manual_expire_listings(self):
        listings= []

        sql_query = "SELECT ID,dealer_id FROM `fl_listings` WHERE Status='manual_expire' AND Website_ID=17"
        
        listings = self.execute_sql(sql_query)
        
        for listing in listings:
            listing["dealer_id"] = str(listing["dealer_id"]).strip()
            
        return listings
    
    def get_blacklisted_registration(self):
        listings= []

        sql_query = "SELECT registration from fl_blacklist"
        
        listings = self.execute_sql(sql_query)
        
        for listing in listings:
            listing[str(listing["registration"]).strip().lower()] = 1
            
        return listings
    
    def get_all_registration(self):
        listings= []

        sql_query = "SELECT ID,registration,Status from fl_listings"
        
        listings = self.execute_sql(sql_query)
        
        return listings
    
    def handle_blacklist_registration(self):
        
        blacklisted_reg = self.get_blacklisted_registration()
        
        all_reg = self.get_all_registration()
        
        expire = []
        print(f'total blacklisted registration : {len(blacklisted_reg)}')
        for item in all_reg:
            if item["registration"] == None:
                continue
            
            reg = item["registration"].lower().strip()
            
            if reg in blacklisted_reg:
                
                if item["Status"] in ["active","expired","to_parse"]:
                    expire.append({
                        "what":{
                            "Status":"manual_expire",
                            "why":"registration present in fl_blacklist"
                        },
                        "where":{
                            "ID":item["ID"]
                        }

                    })
        self.db.connect()
        
        for item in expire:
            self.db.recUpdate("fl_listings",item["what"],item["where"])
            print(item)
            
        self.db.disconnect()
        

    def main(self):
        blacklisted_dealers = self.get_blacklisted_dealers_id()
        active_listings = self.get_active_listings()
        manual_expire = self.get_manual_expire_listings()
        
        expire_listings = []
        activate_listings = []
        
        for listing in active_listings:
            if listing["dealer_id"] in blacklisted_dealers:
                expire_listings.append(listing)
        
        for listing in manual_expire:
            if not listing["dealer_id"] in blacklisted_dealers:
                activate_listings.append(listing)
        
        self.db.connect()
        for listing in activate_listings:
            self.db.recUpdate("fl_listings",{"Status":"active"},{"ID":listing["ID"]})
            print(f'status updated : {listing["ID"]} - active')
        
        for listing in expire_listings:
            self.db.recUpdate("fl_listings",{"Status":"manual_expire"},{"ID":listing["ID"]})
            print(f'status updated : {listing["ID"]} - manual_expire')
        self.db.disconnect()

if __name__ == "__main__":
    m = HandleBlackListedDealers()
    m.main()
    m.handle_blacklist_registration()