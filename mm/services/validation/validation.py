
class Validation:
    def __init__(self) -> None:
        print("validation init")
        
    def validate(self,data):
        price = data["price"]
        
        cc = data["engineCylindersCC"]
        
        margin = data["margin"]
        
        built = data["built"]
        
        mileage = data["mileage"]
        
        if self.priceValidation(price) == False:
            return False
        
        if self.engineCCValidation(cc) == False:
            return False
        
        if self.marginValidation(margin) == False:
            return False
        
        if self.builtValidation(built) == False:
            return False
        
        if self.mileageValidation(mileage) == False:
            return False
        
        return True        
    
    def priceValidation(self,price):
        
        if price == None:
            return False
        
        if price <= 25000:
            return True
        
        return False
        
    def engineCCValidation(self,cc):
        
        if cc == None:
            return False
        
        if cc <= 3001:
            return True
        else:
            return False
    
    def marginValidation(self,margin):
        
        if margin == None:
            return False
        else:
            return True
    
    def builtValidation(self,built):
        
        if built == None:
            return False
        
        if built >= 2012:
            return True

        return False

    def mileageValidation(self,mileage):
        
        if mileage == None:
            return False
        
        if mileage < 120000:
            return True
        else:
            return False