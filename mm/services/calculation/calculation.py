from pcpAprCalculation import pcpAprCalculation

class Calculation:
    def __init__(self) -> None:
        print("apr pcp init")
        
        self.pcpaprCalc = pcpAprCalculation()
        
    def calculatePcpApr(self,data):
        
        price = data["price"]
        
        mileage = data["mileage"]
        
        built = data["built"]
        
        pcpapr = self.pcpaprCalc.calculate_apr_pcp(price,mileage,built)
        
        return pcpapr