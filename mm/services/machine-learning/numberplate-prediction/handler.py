from pathlib import Path
from Database  import Database
from predictor import Predictor
from PlateRecognizer import PlateRecognizer


class Handler:
    def __init__(self) -> None:
        self.database = Database()
        self.predictor = Predictor()
        self.pathPrefix = Path('/usr/src/app/files')
        self.PlateRecognizer = PlateRecognizer()
        
    def test(self):
        self.database.connect()
        try:
            listings = self.database.recUpdate("AT_urls",{"number_plate_flag":2},{"url":"https://www.autotrader.co.uk/car-details/202203243888660"})
        except Exception as e:
            print(f'error : {str(e)}')
            
        self.database.disconnect()
        
        print(listings)
    
    def getPendingListings(self):
        listings = []
        self.database.connect()
        try:
            listings = self.database.recCustomQuery("SELECT ID,registration,product_url FROM `fl_listings` WHERE number_plate_flag=1 AND Status='active' LIMIT 100")
        except Exception as e:
            print(f'error : {str(e)}')
        self.database.disconnect()
        return listings
    
    def getListingImages(self,listingId):
        listingImages = []
        self.database.connect()
        try:
            listingImages = self.database.recCustomQuery(f'SELECT Original FROM `fl_listing_photos` WHERE Listing_ID={listingId}')
        except Exception as e:
            print(f'error : {str(e)}')
        self.database.disconnect()
        return listingImages
    
    def validateRegistration(self,predicted,actual):
        regNo = str(predicted).strip().upper()
        
        print(f'actual : {actual}')
        print(f'predicted : {regNo}')
        print(f'{actual[-2:-1]}')
        
        if len(regNo) != 7:
            return False,regNo
        
        if not regNo.startswith(actual[0]):
            return False,regNo
        
        if not regNo.endswith(actual[len(regNo) - 2 : len(regNo)]):
            return False,regNo
        
        return True,regNo
    
    def getRegistrationFromImages(self,images,rawRegistration):
        
        registration = None
        
        status = False
        
        for image in images:
            
            imgPath = Path(image["path"])
            
            if imgPath.exists() == False:
                continue
            
            isNumberPlateVisible = self.predictor.predict(imgPath)
            
            if isNumberPlateVisible == False:
                continue
            
            
            with open(imgPath,"rb") as f:
                result = self.PlateRecognizer.fetchRegistrationNumber(f)

            if result["status"] == False:
                continue
            
            status,regno = self.validateRegistration(result["registration"],rawRegistration)
            
            if status == True:
                registration = regno
                break
            else:
                registration = regno
                
        return {
            "registration":registration,
            "registrationStatus":status
        }   
                    
if __name__ == "__main__":
    h = Handler()
    h.main()