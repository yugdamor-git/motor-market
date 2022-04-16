from distutils.command.upload import upload
import os
import requests

class PlateRecognizer:
    
    def __init__(self) -> None:
        PLATE_RECOGNIZER_TOKEN = os.environ.get("PLATE_RECOGNIZER_TOKEN","3a61a44b8e39d27b22e690612093f362cfef1f63")
        
        self.headers = {
            "Authorization":f'Token {PLATE_RECOGNIZER_TOKEN}'
        }
        
        print(self.headers)
        
        self.regions = ["gb","it"]
        
        self.apiRemainingCallCount = 0
        
        self.apiCallCountEndpoint = 'https://api.platerecognizer.com/v1/statistics/'
        
        self.apiPlatReaderEndpoint = 'https://api.platerecognizer.com/v1/plate-reader/'
        
        self.updateApiRemainingCallCount()
        
    
    def updateApiRemainingCallCount(self):
        
        try:
            response = requests.get(
                self.apiCallCountEndpoint,
                headers=self.headers
            )
            
            jsonDict = response.json()
            
            print(jsonDict)
            
            self.apiRemainingCallCount = jsonDict["total_calls"] - jsonDict["usage"]["calls"]
            
            print(f'apiRemainingCallCount : {self.apiRemainingCallCount}')
        except Exception as e:
            print(f'error : {str(e)}')
    
    def fetchRegistrationNumber(self,image):
        
        self.updateApiRemainingCallCount()
        
        if self.apiRemainingCallCount < 10:
            return {
                "registration":None,
                "status":False,
                "message":"apiRemainingCallCount is less than 10. please refill it."
            }
        
        try:
        
            response = requests.post(
                self.apiPlatReaderEndpoint,
                headers=self.headers,
                data=dict(regions=self.regions),
                files=dict(upload=image)
            )
            
            jsonDict = response.json()
            
            plate = jsonDict["results"][0]["plate"]
            
            return {
                "registration":plate,
                "status":True,
                "message":""
            }
        
        except Exception as e:
            print(f'error : {str(e)}')
        
            return {
                    "registration":None,
                    "status":False,
                    "message":str(e)
                }        


if __name__ == "__main__":
    pr = PlateRecognizer()
    with open("numberplate.jpg","rb") as f:
        p = pr.fetchRegistrationNumber(f)
        
        print(p)