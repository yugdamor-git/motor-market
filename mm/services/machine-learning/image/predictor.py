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
        
    def predict(self,urls):
        
        predicted = []
        
        downloadedImages = self.prepare_images(urls)
        
        for image in downloadedImages:
            
            tmp = image.copy()
            
            img = open_image(image["filePath"])
            
            os.remove(image["filePath"])
            
            pred_class,pred_idx,outputs = self.model.predict(img)
            
            if not str(pred_class) in ["cars"]:
                print(f'this is not car image : {image["url"]}')
                continue
            
            tmp["imageClass"] = str(pred_class)
            
            predicted.append(image["url"])
            
        return predicted
    
    def encode_image_base64(self,imagePath):
        
        base64str = base64.b64encode(imagePath.read_bytes()).decode("utf-8")
        
        return base64str
    
    def prepare_images(self,urls):
        downloadedImages = self.downloader.download_multiple_images(urls)
        return downloadedImages