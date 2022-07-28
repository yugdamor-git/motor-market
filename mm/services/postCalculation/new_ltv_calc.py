import json
import sys


sys.path.append("/libs")

from helper import get_current_datetime

from Database import Database

from dealerForecourt import dealerForecourt

from ltvCalculationRules import ltvCalculationRules

# from mongo_database import MongoDatabase

class MarketCheckLtvCalculationRules:
    
    def __init__(self) -> None:
        self.mysql_db = Database()
        self.dealer_forecourt = dealerForecourt(self.mysql_db)
        self.old_ltv = ltvCalculationRules()
        # self.old_margin_calc = OldMarginCalculation()
        # self.mongodb = MongoDatabase()
    
    
    def calculate_old_margin(self,make,model,engine_cc):
        
        margin = self.old_margin_calc.calculateMargin(make,model,engine_cc)
        
        return margin
    
    def calculate(self,source_price,registration,mileage,website_id):
        
        if source_price < 11999:
            
            forecourt_value,forecourt_response = self.dealer_forecourt.get_dealerforecourt_price(registration,mileage,website_id)
            
            print(f'forecourt_value -> {forecourt_value} , source_price -> {source_price}')
    
            if forecourt_value == None:
                return {
                    "status":False,
                    "forecourt_call":True,
                    "mm_price":None,
                    "margin":None,
                    "ltv":self.old_ltv.getNullValues(),
                    "response":json.dumps(forecourt_response),
                    "ltv_status":0
                }
            
            percentage = 12
            
            percent_source_price = (percentage/100) * source_price
            
            provisional_mm_price =int(source_price + percent_source_price)
            
            ltv_percentage = (provisional_mm_price / forecourt_value) * 100
            
            if ltv_percentage >= 120:
                
                return {
                    "status":False,
                    "forecourt_call":True,
                    "mm_price":None,
                    "margin":None,
                    "forecourt_price":forecourt_value,
                    "ltv":{},
                    "response": json.dumps(forecourt_response),
                    "ltv_status":0
                }
                
            
            if ltv_percentage < 110:
                max_margin = float(forecourt_value) * 1.10
                margin = max_margin - provisional_mm_price
            else:
                margin = 0
                
            max_margin = 3000
            
            if margin >= max_margin:
                margin = max_margin
            
            mm_price = provisional_mm_price + margin
            
            final_ltv = self.old_ltv.calculate(mm_price,forecourt_value)
            
            return  {
                    "status":True,
                    "forecourt_call":True,
                    "mm_price":int(mm_price),
                    "forecourt_price":forecourt_value,
                    "margin":int(margin),
                    "response": json.dumps(forecourt_response),
                    "ltv":final_ltv,
                    "ltv_percentage":ltv_percentage,
                    "ltv_status":1
                }
        
        else:
            percentage = 11
            
            ltv_percentage = 69
            
            percent_source_price = float(percentage/100) * source_price
            
            max_margin = 3000
            
            if percent_source_price >= max_margin:
                margin = max_margin
            else:
                margin = percent_source_price
            
            mm_price = source_price + margin
            
            return  {
                    "status":True,
                    "forecourt_call":False,
                    "mm_price":int(mm_price),
                    "margin":int(margin),
                    "ltv":self.old_ltv.getDefaultValues(),
                    "ltv_percentage":ltv_percentage,
                    "ltv_status":None
                }