
class Validation:
    def __init__(self,logsProducer) -> None:
        print("validation init")
        
        self.logsProducer = logsProducer
        
    def validate(self,data):
        
        sourceUrl = data["sourceUrl"]
        
        price = data["price"]
        
        cc = data["engineCylindersCC"]
        
        margin = data["margin"]
        
        built = data["built"]
        
        mileage = data["mileage"]
        
        images = data["images"]
        
        log = {}
        log["service"] = "services.validation"
        log["sourceUrl"] = sourceUrl
        
        status,message = self.priceValidation(price)
        if status == False:
            log["errorMessage"] = message
            self.logsProducer.produce(log)
            return False
        
        status,message = self.engineCCValidation(cc)
        if status == False:
            log["errorMessage"] = message
            self.logsProducer.produce(log)
            return False
        
        status,message = self.marginValidation(margin)
        if status == False:
            log["errorMessage"] = message
            self.logsProducer.produce(log)
            return False
        
        status,message = self.builtValidation(built)
        if status == False:
            log["errorMessage"] = message
            self.logsProducer.produce(log)
            return False
        
        status,message = self.mileageValidation(mileage)
        if status == False:
            log["errorMessage"] = message
            self.logsProducer.produce(log)
            return False

        status,message = self.imageValidation(images)
        if status == False:
            log["errorMessage"] = message
            self.logsProducer.produce(log)
            return False
        
        return True
    
    
    def imageValidation(self,images):
        if len(images) == 0:
            return False,"there are no images."
        return True,None
    
    def priceValidation(self,price):
        
        if price == None:
            return False,"price is empty."
        
        maxPrice = 25000
        
        if price <= maxPrice:
            return True,None
        
        return False,f'price({price}) is more than max price({maxPrice})'
        
    def engineCCValidation(self,cc):
        
        if cc == None:
            return False,"engine cc is empty."
        
        maxCC = 3001
        
        if cc <= maxCC:
            return True,None
        else:
            return False,f'engineCC({cc}) is more than maxEngineCC({maxCC}).'
    
    def marginValidation(self,margin):
        
        if margin == None:
            return False,f'margin value is empty.'
        else:
            return True,None
    
    def builtValidation(self,built):
        
        if built == None:
            return False,"built year is empty."
        
        minBuilt = 2012
        
        if built >= minBuilt:
            return True,None

        return False,f'built year({built}) is less than min built year({minBuilt}).'

    def mileageValidation(self,mileage):
        
        if mileage == None:
            return False,'mileage is empty.'
        
        maxMileage = 120000
        
        if mileage < maxMileage:
            return True,None
        else:
            return False,f'mileage({mileage}) is more than maxMileage({maxMileage})'
    
    # def isManuallyExpired(self,status):
    #     pass
    
    # def isSoldOut(self,status):
    #     pass
    
    # def isBlackListedDealer(self,dealerId):
        
    #     if dealerId == None:
    #         return False
        
    #     dealers = list(self.db.blacklistedDealers.find({"dealerId":dealerId}))
        
    #     if len(dealers) > 0:
    #         return True
    #     else:
    #         return False
        