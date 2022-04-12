import base64
from fastai.vision import *
from pathlib import Path
import os

class Predictor:
    def __init__(self):
        print("image predictor init")

        self.model = load_learner('model','plate_or_not_8.pkl')
        
        self.cwd = Path.cwd()
        
    def predict(self,image):
            
        tmp = image.copy()
        
        img = open_image(image["filePath"])
        
        pred_class,pred_idx,outputs = self.model.predict(img)
        
        if not str(pred_class) in ["plate_number"]:
            tmp["imageClass"] = None
            tmp["status"] = False
        else:
            tmp["imageClass"] = str(pred_class)
            tmp["status"] = True
            
        return tmp
    
    
    def encode_image_base64(self,imagePath):
        
        base64str = base64.b64encode(imagePath.read_bytes()).decode("utf-8")
        
        return base64str
    
    def prepare_images(self,urls):
        
        downloadedImages = self.downloader.download_multiple_images(urls)
        
        return downloadedImages