import re

class MMUrlGenerator:
    def __init__(self) -> None:
        self.baseUrl = "https://www.motor.market/cars"
        
    def cleanText(self,text):
        temp_title = text.lower()    
        temp_title = re.sub(r'_+', '-', temp_title, flags=re.S | re.M)
        temp_title = re.sub(r'[\s\.\,\(\)\/\\]', '-', temp_title, flags=re.S | re.M)
        temp_title = re.sub(r'-+', '-', temp_title, flags=re.S | re.M)
        temp_title = re.sub(r'[^a-z0-9_-]', '', temp_title, flags=re.S | re.M)
        return temp_title
    
    def generateMMUrl(self,title,make,model,listingId):
        make = self.cleanText(make)
        model = self.cleanText(model)
        title = self.cleanText(title)
        
        tmpUrl = f'{make}/{model}/{title}-{listingId}.html'
        
        tmpUrl = re.sub(r'-+', '-', tmpUrl, flags=re.S | re.M)
        
        mmUrl = f'{self.baseUrl}/{tmpUrl}'
        
        return mmUrl
