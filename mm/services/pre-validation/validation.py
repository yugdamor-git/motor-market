
from Database import Database

class Validation:
    def __init__(self,logsProducer) -> None:
        print("validation init")
        
        self.logsProducer = logsProducer
        
        self.database = Database()
        
    def validate(self,data):
        
        sourceUrl = data["sourceUrl"]
        
        price = data["sourcePrice"]
        
        cc = data["engineCylindersCC"]
        
        built = data["built"]
        
        mileage = data["mileage"]
        
        dealerId = data["dealerId"]
        
        customPriceEnabled = data.get("customPriceEnabled",0)
        
        scraperName = data.get("scraperName",None)
        
        log = {}
        log["service"] = "motormarket.scraper.autotrader.listing.prevalidation"
        log["sourceUrl"] = sourceUrl
        
        status,message = self.priceValidation(price,scraperName)
        
        if status == False:
            log["errorMessage"] = message
            
            self.logsProducer.produce_message({
                    "eventType":"insertLog",
                    "data":log
                })
            
            return False,log
        
        status,message = self.engineCCValidation(cc,customPriceEnabled)
        if status == False:
            log["errorMessage"] = message
            
            self.logsProducer.produce_message({
                    "eventType":"insertLog",
                    "data":log
                })
            
            return False,log
        
        status,message = self.builtValidation(built,scraperName)
        if status == False:
            log["errorMessage"] = message
            
            self.logsProducer.produce_message({
                    "eventType":"insertLog",
                    "data":log
                })
            
            return False,log
        
        status,message = self.mileageValidation(mileage,scraperName)
        if status == False:
            log["errorMessage"] = message
            
            self.logsProducer.produce_message({
                    "eventType":"insertLog",
                    "data":log
                })
            
            return False,log
        
        status,message = self.isBlackListedDealer(dealerId)
        if status == True:
            log["errorMessage"] = message
            
            self.logsProducer.produce_message({
                    "eventType":"insertLog",
                    "data":log
                })
            
            return False,log
        
        return True,{}
    
    def priceValidation(self,price,scraperName):
        
        if price == None:
            return False,"price is empty."
        
        if scraperName == "url-scraper":
            return True,None
        maxPrice = 25000
        
        if price <= maxPrice:
            return True,None
        
        return False,f'price({price}) is more than max price({maxPrice})'
        
    def engineCCValidation(self,cc,customPriceEnabled):
        
        if cc == None:
            return False,"engine cc is empty."
        
        if customPriceEnabled == True:
            return True,None
        
        maxCC = 3001
        
        if cc <= maxCC:
            return True,None
        else:
            return False,f'engineCC({cc}) is more than maxEngineCC({maxCC}).'
    
    def builtValidation(self,built,scraperName):
        
        if scraperName == "url-scraper":
            return True,None
        
        if built == None:
            return False,"built year is empty."
        
        minBuilt = 2012
        
        if built >= minBuilt:
            return True,None

        return False,f'built year({built}) is less than min built year({minBuilt}).'

    def mileageValidation(self,mileage,scraperName):
        
        if mileage == None:
            return False,'mileage is empty.'
        
        if scraperName == "url-scraper":
            return True,None
        
        maxMileage = 120000
        
        if mileage < maxMileage:
            return True,None
        else:
            return False,f'mileage({mileage}) is more than maxMileage({maxMileage})'
        
    def isBlackListedDealer(self,dealerId):
        
        if dealerId == None:
            return False,f'dealerId is None'
        
        self.database.connect()
        
        try:
            dealers = self.database.recSelect("fl_dealer_blacklist",{
                "dealer_id":str(dealerId)
            })
        except Exception as e:
            dealers = None
        
        self.database.disconnect()
        
        if len(dealers) > 0:
            return True,f'dealerId ({dealerId}) is blacklisted.'
        else:
            return False,None
        