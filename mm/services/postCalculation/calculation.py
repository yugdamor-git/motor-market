from pcpAprCalculation import pcpAprCalculation
from ltvCalculationRules import ltvCalculationRules
from Database import Database
from dealerForecourt import dealerForecourt
from categoryId import categoryId
from videoId import videoId


class Calculation:
    def __init__(self) -> None:
        print("apr pcp init")
        
        self.pcpaprCalc = pcpAprCalculation()
        
        self.ltvCalc = ltvCalculationRules()
        
        self.db = Database()
        
        self.dealerForecourtCalc = dealerForecourt(self.db)
        
        self.categoryIdCalc = categoryId(self.db)
        
        self.videoIdCalc = videoId()
        
    def calculatePcpApr(self,data):
        
        price = data["price"]
        
        mileage = data["mileage"]
        
        built = data["built"]
        
        pcpapr = self.pcpaprCalc.calculate_apr_pcp(price,mileage,built)
        
        return pcpapr
    
    def calculateLtv(self,data):
        ltv = {}
        
        mmPrice = data.get("price") + data.get("margin",0)
        sourcePrice = data.get("price") + data.get("adminFee",0)
        registrationStatus = data.get("registrationStatus")
        
        if sourcePrice < 10000:
            if registrationStatus == True:
                glassPrice = self.calculateDealerForecourt(data)
                
                ltv = self.ltvCalc.calculate(mmPrice,glassPrice)
            else:
                ltv = self.ltvCalc.getDefaultValues()
        else:
            ltv = self.ltvCalc.getDefaultValues()
        
        return ltv
        
    
    def calculateCategoryId(self,data):
        make = data.get("make")
        model = data.get("model")
        
        categoryId = self.categoryIdCalc.getCategoryId(make,model)
        
        return categoryId
    
    def calculateDealerForecourt(self,data):
        
        websiteId = data.get("websiteId")
        
        registration = data.get("registration")
        
        mileage = data.get("mileage")
        
        dealerForecourtPrice = self.dealerForecourtCalc.get_dealerforecourt_price(registration,mileage,websiteId)
        
        return dealerForecourtPrice
    
    def calculateVideoId(self,data):
        
        make = data.get("make")
        
        model = data.get("model")
        
        built = data.get("built",None)
        try:
            videoId = self.videoIdCalc.get_video_id(make,model,built)
        except Exception as e:
            print(f'error : not able to get video id : {str(e)}')
            videoId = None
            
        return videoId
    

    
    
    