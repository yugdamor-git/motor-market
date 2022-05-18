from pcpAprCalculation import pcpAprCalculation
from ltvCalculationRules import ltvCalculationRules
from Database import Database
from dealerForecourt import dealerForecourt
from categoryId import categoryId
from videoId import videoId
from dealerAdminFee import dealerAdminFee
import json

from marginCalculation import marginCalculation


class Calculation:
    def __init__(self) -> None:
        print("apr pcp init")
        
        self.pcpaprCalc = pcpAprCalculation()
        
        self.ltvCalc = ltvCalculationRules()
        
        self.db = Database()
        
        self.dealerForecourtCalc = dealerForecourt(self.db)
        
        self.categoryIdCalc = categoryId(self.db)
        
        self.marginCalc = marginCalculation()
        
        self.videoIdCalc = videoId()
        
        self.dealerAdminFeeHandler = dealerAdminFee(self.db)
    
    def updateAdminFee(self,data):
        
        dealerId = data.get("dealerId",None)
        
        sourceAdminFee = data.get("adminFee",0)
        
        if dealerId == None:
            return
        
        dbAdminFee = self.dealerAdminFeeHandler.getAdminFee(dealerId)
        
        if sourceAdminFee > dbAdminFee:
            data["adminFee"] = sourceAdminFee
        else:
            data["adminFee"] = dbAdminFee
            
            
    def calculatePcpApr(self,data):
        
        price = data["mmPrice"]
        
        mileage = data["mileage"]
        
        built = data["built"]
        
        pcpapr = self.pcpaprCalc.calculate_apr_pcp(price,mileage,built)
        
        data["pcpapr"] = pcpapr
    
    def calculateMargin(self,data):
        
        make = data.get("predictedMake")
        model = data.get("predictedModel")
        engineCylindersCC = data.get("engineCylindersCC")
        
        margin = self.marginCalc.calculateMargin(make,model,engineCylindersCC)
        
        data["margin"] = margin
    
    def calculateMMprice(self,data):
        # mm price
        
        customPriceEnabled = data.get("customPriceEnabled",None)
        
        margin = data.get("margin",0)
        
        if customPriceEnabled == True:
            customPrice =  data.get("customPrice")
            data["mmPrice"] =customPrice
            data["margin"] = customPrice - data["sourcePrice"]
            
        else:
            data["mmPrice"] = data.get("price") + margin
            
    def calculateSourcePrice(self,data):
        
        atPrice = data.get("price",0)
        
        adminFee = data.get("adminFee",0)
        
        data["sourcePrice"] = atPrice + adminFee
        
    
    def calculateLtv(self,data):
        ltv = {}
        try:
            mmPrice = data.get("mmPrice")
            
            sourcePrice = data.get("price")
            
            registrationStatus = data.get("registrationStatus")
            
            if sourcePrice < 10000:
                if registrationStatus == True:
                    glassPrice,dealerForecourtResponse = self.calculateDealerForecourt(data)
                    if glassPrice == None:
                        ltv = {}
                        ltv["ltvStatus"] = 0
                        ltv["dealerForecourtResponse"] = json.dumps(dealerForecourtResponse)
                        ltv["status"] = "approval"
                        
                        ltv.update(self.ltvCalc.getNullValues())
                    else:
                        ltv = self.ltvCalc.calculate(mmPrice,glassPrice)
                        ltv["dealerForecourtPrice"] = glassPrice
                        ltv["ltvStatus"] = 1
                else:
                    ltv = {}
                    ltv["ltvStatus"] = 0
                    ltv.update(self.ltvCalc.getNullValues())
            else:
                ltv = self.ltvCalc.getDefaultValues()
                ltv["ltvStatus"] = None
                
        except Exception as e:
            print(f'error - calculation.py : {str(e)}')
            ltv = {}
            ltv["ltvStatus"] = 0
            ltv["status"] = "approval"
            ltv.update(self.ltvCalc.getNullValues())
        
        data["ltv"] = ltv
        
    
    def calculateCategoryId(self,data):
        make = data.get("predictedMake")
        model = data.get("predictedModel")
        
        categoryId = self.categoryIdCalc.getCategoryId(make,model)
        
        data["categoryId"] = categoryId
    
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
            print(f'error - calculation.py : not able to get video id : {str(e)}')
            videoId = None
            
        if videoId != None:
            data["videoId"] = videoId
    

    
    
    