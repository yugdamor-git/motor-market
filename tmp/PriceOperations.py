import json
class PriceOperations:
    def __init__(self):
        print("price operations")
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
        
    def to_int(self,value):
        temp = None
        try:
            temp = int(str(value).strip())
        except Exception as e:
            print(f'PriceOperations : {str(e)}')
        return temp
    
    def apply_difference_check_operation(self,listing,glass_price):
        if listing["admin_fees"] != None:
            price = self.to_int(listing["cal_price_from_file"] + listing["admin_fees"])
        else:
            price = self.to_int(listing["cal_price_from_file"])
        glass_price = self.to_int(glass_price)
        
        if price != None and glass_price != None and listing["price"] != None:
            if price < 10000:
                for item in self.personalized_percentage:
                    max_lend = glass_price * item["percentage"]
                    diff = max_lend - self.to_int(listing["price"])
                    listing[item["name"]] = int(diff)
            else:
                self.apply_default_values(listing)
        else:
            self.apply_default_values(listing)
            print(f'error : apply_difference_check_operation : price/glass price is not valid in listing')

        return listing
    
    def apply_default_values(self,listing):
        for item in self.personalized_percentage:
            listing[item["name"]] = self.default_value
        return listing