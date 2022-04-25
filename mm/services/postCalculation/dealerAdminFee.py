


class dealerAdminFee:
    def __init__(self,db) -> None:
        self.db = db
        
    
    def getAdminFee(self,dealerId):
        adminFee = 0
        
        self.db.connect()
        
        try:
            result = self.db.recCustomQuery(f'SELECT admin_fee FROM fl_dealer_scraper WHERE status="active" AND dealer_id={dealerId}')
            
            if len(result) > 0:
                adminFee = int(result["admin_fee"])
        except:
            pass
        
        self.db.disconnect()

        return adminFee
    
