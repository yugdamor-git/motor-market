from pcpAprCalculation import pcpAprCalculation
from ltvCalculationRules import ltvCalculationRules
from Database import Database
from dealerForecourt import dealerForecourt
from categoryId import categoryId
from videoId import videoId
import json


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
        
        price = data["mmPrice"]
        
        mileage = data["mileage"]
        
        built = data["built"]
        
        pcpapr = self.pcpaprCalc.calculate_apr_pcp(price,mileage,built)
        
        return pcpapr
    
    def calculateLtv(self,data):
        ltv = {}
        
        mmPrice = data.get("mmPrice")
        
        sourcePrice = data.get("sourcePrice")
        
        registrationStatus = data.get("registrationStatus")
        
        try:
            if sourcePrice < 10000:
                if registrationStatus == True:
                    glassPrice,dealerForecourtResponse = self.calculateDealerForecourt(data)
                    if glassPrice == None:
                        ltv = {}
                        ltv["ltvStatus"] = 0
                        ltv["dealerForecourtResponse"] = json.dumps(dealerForecourtResponse)
                    else:
                        ltv = self.ltvCalc.calculate(mmPrice,glassPrice)
                        ltv["dealerForecourtPrice"] = glassPrice
                        ltv["ltvStatus"] = 1
                else:
                    ltv = {}
                    ltv["ltvStatus"] = 0
            else:
                ltv = self.ltvCalc.getDefaultValues()
        except Exception as e:
            print(f'error : {str(e)}')
            ltv = {}
            ltv["ltvStatus"] = 0
        
        return ltv
        
    
    def calculateCategoryId(self,data):
        make = data.get("predictedMake")
        model = data.get("predictedModel")
        
        categoryId = self.categoryIdCalc.getCategoryId(make,model)
        
        return categoryId
    
    def calculateDealerForecourt(self,data):
        
        websiteId = data.get("websiteId")
        
        registration = data.get("predictedRegistration")
        
        mileage = data.get("mileage")
        
        dealerForecourtPrice,DealerForecourtResponse = self.dealerForecourtCalc.get_dealerforecourt_price(registration,mileage,websiteId)
        
        return dealerForecourtPrice,DealerForecourtResponse
    
    def calculateVideoId(self,data):
        
        make = data.get("predictedMake")
        
        model = data.get("predictedModel")
        
        built = data.get("built",None)
        try:
            videoId = self.videoIdCalc.get_video_id(make,model,built)
        except Exception as e:
            print(f'error : not able to get video id : {str(e)}')
            videoId = None
            
        return videoId
    

    
    
    