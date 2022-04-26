
from carData import carTypeDict


class marginCalculation:
    def __init__(self) -> None:
        self.make4x4 = [
            "cadillac",
            "jeep"
        ]
        
        self.makeLuxury = [
            "caterham", 
            "hummer", 
            "infiniti", 
            "jaguar", 
            "morgan",
            "lotus"
        ]
        
        self.marginBasedOnCC = {

            "4x4": {
                0: {"margin": 1370, "min": 0, "max": 1001},
                1: {"margin": 1370, "min": 1001, "max": 2001},
                2: {"margin": 1420, "min": 2001, "max": 2501},
                3: {"margin": 1545, "min": 2501, "max": 3001}
            },
            "luxury": {
                0: {"margin": 1425, "min": 0, "max": 1001},
                1: {"margin": 1425, "min": 1001, "max": 2001},
                2: {"margin": 1499, "min": 2001, "max": 2501},
                3: {"margin": 1599, "min": 2501, "max": 3001}
            },
            "others": {
                0: {"margin": 1299, "min": 0, "max": 1001},
                1: {"margin": 1299, "min": 1001, "max": 2001},
                2: {"margin": 1355, "min": 2001, "max": 2501},
                3: {"margin": 1415, "min": 2501, "max": 3001}
            }
        }
        
        self.carTypesDict = carTypeDict
        
    
    
    
    def calculateMargin(self,make,model,cc):
        make = make.lower()
        model = model.lower()
        
        if make == None or model == None or cc == None:
            return None
        
        key = f'{make}_{model}'
        
        if make in self.make4x4:
            category = "4x4"
        elif make in self.makeLuxury:
            category = "luxury"
        else:
            out = self.carTypesDict.get(key,None)
            
            if out == None:
                category = "others"
            else:
                category = out["category"]
        
        margin = None
        
        marginDict = self.marginBasedOnCC[category]
        
        for key in marginDict:
            data = marginDict[key]
            
            if cc >= data["min"] and cc < data["max"]:
                margin = data["margin"]
                break

        return margin
    
    


if __name__ == "__main__":
    m = marginCalculation()
    
    print(m.calculateMargin("audi","a6 allroad",2970))