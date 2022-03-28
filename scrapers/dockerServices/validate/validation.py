
class validation:
    def __init__(self) -> None:
        print("validation init")
    
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