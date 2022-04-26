import base64
from fastai.vision import *
from pathlib import Path
from imageDownloader import ImageDownloader
import os

class Predictor:
    def __init__(self):
        print("image predictor init")

        self.model = load_learner('model')
        
        self.cwd = Path.cwd()
        
        self.downloader = ImageDownloader()
        
    def predict(self,urls,websiteId,sourceId):
        
        predicted = []
        
        downloadedImagesTemp = self.prepare_images(urls,websiteId,sourceId)
        
        downloadedImages = sorted(downloadedImagesTemp, key = lambda i: i['position'])
        
        for image in downloadedImages:
            
            if len(predicted) >= 10:
                break
            
            if image["status"] == False:
                # log image failed to download
                continue
            
            tmp = image.copy()
            
            del tmp["status"]
            
            img = open_image(tmp["path"])
            
            pred_class,pred_idx,outputs = self.model.predict(img)
            
            if not str(pred_class) in ["cars"]:
                print(f'this is not car image : {tmp["url"]}')
                continue
            
            tmp["path"] = str(tmp["path"])
            
            predicted.append(tmp)
            
        return predicted
    
    def encode_image_base64(self,imagePath):
        
        base64str = base64.b64encode(imagePath.read_bytes()).decode("utf-8")
        
        return base64str
    
    def prepare_images(self,urls,websiteId,sourceId):
        downloadedImages = self.downloader.download_multiple_images(urls,websiteId,sourceId)
        return downloadedImages