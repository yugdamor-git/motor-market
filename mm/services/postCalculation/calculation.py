from pcpAprCalculation import pcpAprCalculation
from ltvCalculationRules import ltvCalculationRules
from Database import Database
from dealerForecourt import dealerForecourt
from categoryId import categoryId
from videoId import videoId
from dealerAdminFee import dealerAdminFee
import json

from marginCalculation import marginCalculation

from new_ltv_calc import MarketCheckLtvCalculationRules


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
        
        self.mc_calc_rules  = MarketCheckLtvCalculationRules()
    
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
            data["mmPrice"] = data.get("sourcePrice") + margin
            
    def calculateSourcePrice(self,data):
        
        atPrice = data.get("price",0)
        
        adminFee = data.get("adminFee",0)
        
        data["sourcePrice"] = atPrice + adminFee
        
    
    def calculate_ltv(self,data):
        
        source_mrp = data["sourcePrice"]
        
        registration = data["predictedRegistration"]
        
        mileage = data["mileage"]
        
        website_id = data["websiteId"]
        
        registrationStatus = data.get("registrationStatus")
        
        # make = data["predictedMake"]
        
        # model = data["predictedModel"]
        
        # engine_cc = data.get("engineCylindersCC",None)
        if registrationStatus == False:
            ltv = {}
            ltv["ltvStatus"] = 0
            ltv.update(self.ltvCalc.getNullValues())
            data["ltv"] = ltv
            return True

        ltv_resp = self.mc_calc_rules.calculate(source_mrp,registration,mileage,website_id)
        
        # new_source price
        # new_margin
        # new_price
        # new_cal_price_from_file
        customPriceEnabled = data.get("customPriceEnabled",None)
        if customPriceEnabled == True:
            data["ltv"] = self.mc_calc_rules.old_ltv.getDefaultValues()
            data["margin"] = 0
            data["registrationStatus"] = 1
            data["ltv_percentage"] = 69
            return True

        if ltv_resp["status"] == True:
            mm_price = ltv_resp["mm_price"]
            margin = ltv_resp["margin"]
            ltv_percentage = ltv_resp["ltv_percentage"]
            old_ltv_values = ltv_resp["ltv"]
            ltv_status =ltv_resp["ltv_status"]
            data["ltv"] = {}
            if ltv_resp["forecourt_call"] == True:
                response = ltv_resp["response"]
                data["ltv"]["dealerForecourtResponse"] = response
                data["dealerForecourtResponse"] = response
                
                data["ltv"]["dealerForecourtPrice"] = ltv_resp["forecourt_price"]
            
            data["mmPrice"] = mm_price
            data["margin"] = margin
            data["ltv_percentage"] = round(ltv_percentage,1)
            data["ltv"].update(old_ltv_values)
            data["ltv"]["ltvStatus"] = ltv_status
            return True
        else:
            return False
    
    
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
    
    def car_cutter_extra_margin(self,data):
            
        if data["sourcePrice"] > 10000:
            if data["registrationStatus"] == 1:
                mmPrice = data["mmPrice"]
                extra_margin = 200
                data["cc_extra_margin"] = extra_margin
                data["mmPrice"] = mmPrice + extra_margin
                data["margin"] = data["margin"] + extra_margin
            # return
        
        # mmPrice = data["mmPrice"]
        # forecourt = data["dealerForecourtPrice"]
        # percentage = 110
        # extra_margin = 0
        # forecourt_110 = int(float( (percentage/100) * forecourt ),2)
        # maximum_allowed_margin = forecourt_110 - mmPrice
        # data["forecourt_110"] = forecourt_110
        
        # if maximum_allowed_margin < 0:
        #     data["cc_extra_margin"] = extra_margin
        #     data["mmPrice"] = mmPrice + extra_margin
        #     data["margin"] = data["margin"] + extra_margin
        #     return
        
        # if maximum_allowed_margin >= 200:
        #     extra_margin = 200
        #     data["cc_extra_margin"] = extra_margin
        #     data["mmPrice"] = mmPrice + extra_margin
        #     data["margin"] = data["margin"] + extra_margin
        #     return
        # else:
        #     extra_margin = 200 - maximum_allowed_margin
        #     data["cc_extra_margin"] = extra_margin
        #     data["mmPrice"] = mmPrice + extra_margin
        #     data["margin"] = data["margin"] + extra_margin
        #     return