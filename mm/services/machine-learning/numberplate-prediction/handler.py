from pathlib import Path
from predictor import Predictor
from PlateRecognizer import PlateRecognizer


class Handler:
    def __init__(self) -> None:
        self.predictor = Predictor()
        self.pathPrefix = Path('/usr/src/app/files')
        self.PlateRecognizer = PlateRecognizer()
    
    def validateRegistration(self,predicted,actual):
        regNo = str(predicted).strip().upper()
        
        print(f'actual : {actual}')
        print(f'predicted : {regNo}')
        print(f'{actual[-2:-1]}')
        
        if len(regNo) != 7:
            return False,regNo
        
        if not regNo.startswith(actual[0]):
            return False,regNo
        
        if not regNo.endswith(actual[len(regNo) - 2 : len(regNo)]):
            return False,regNo
        
        return True,regNo
    
    def getRegistrationFromImages(self,images,rawRegistration):
        
        registration = None
        
        status = False
        
        for image in images:
            
            imgPath = Path(image["path"])
            
            if imgPath.exists() == False:
                continue
            
            isNumberPlateVisible = self.predictor.predict(imgPath)
            
            if isNumberPlateVisible == False:
                continue
            
            
            with open(imgPath,"rb") as f:
                result = self.PlateRecognizer.fetchRegistrationNumber(f)

            if result["status"] == False:
                continue
            
            status,regno = self.validateRegistration(result["registration"],rawRegistration)
            
            if status == True:
                registration = regno
                break
            else:
                registration = regno
                
        return {
            "predictedRegistration":registration,
            "registrationStatus":status
        }
                    
if __name__ == "__main__":
    h = Handler()
    h.main()