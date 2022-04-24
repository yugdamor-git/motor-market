class ltvCalculationRules:
    def __init__(self):
        print("ltv calculations init")
        
        self.personalized_percentage = [
        {"name":"QCF_Oodle_AB","percentage":1.25 + 0.01},
        {"name":"QCF_Oodle_C","percentage":1.2 + 0.01},
        {"name":"QCF_Billing","percentage":1.02 + 0.01},
        {"name":"GCC","percentage":1.10 + 0.01},
        {"name":"AM_TierIn","percentage":1.1 + 0.01},
        {"name":"AM_TierEx","percentage":1 + 0.01},
        {"name":"QCF_Adv_E","percentage":1.2 + 0.01},
        {"name":"QCF_Adv_D","percentage":1.2 + 0.01},
        {"name":"QCF_Adv_C","percentage":1.2 + 0.01},
        {"name":"QCF_Adv_AB","percentage":1.2 + 0.01},
        {"name":"QCF_SMF","percentage":1.1 + 0.01},
        {"name":"QCF_MB_NT","percentage":1.10 + 0.01},
        {"name":"QCF_MB_T","percentage":1 + 0.01},
        {"name":"BMF","percentage":1.2 + 0.01},
        ]

        self.default_value = 99999
    
    def calculate(self,mmPrice:int,glassPrice:int):
        tmp = {}
        
        for item in self.personalized_percentage:
            max_lend = float(glassPrice) * item["percentage"]
            
            diff = max_lend - mmPrice
            
            tmp[item["name"]] = int(diff)
            
        return tmp
    
    def getDefaultValues(self):
        
        tmp = {}
        
        for item in self.personalized_percentage:
            tmp[item["name"]] = self.default_value
        return tmp
    
    def getNullValues(self):
        
        tmp = {}
        
        for item in self.personalized_percentage:
            tmp[item["name"]] = None
        return tmp
    