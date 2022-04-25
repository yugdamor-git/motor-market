


class dealerAdminFee:
    def __init__(self,db) -> None:
        self.db = db
        
    
    def getAdminFee(self,dealerId):
        adminFee = 0
        
        self.db.connect()
        
        try:
            result = self.db.recCustomQuery(f'SELECT admin_fee FROM fl_dealer_scraper WHERE status="active" AND dealer_id={dealerId}')
            
            if len(result) > 0:
                adminFee = int(result[0]["admin_fee"])
        except Exception as e:
            print(f'error : {__file__} : {str(e)}')
        
        self.db.disconnect()

        return adminFee
    
