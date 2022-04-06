from fastai.vision import *
from pathlib import Path
import requests


class Predictor:
    def __init__(self):
        print("seat predictor init")

        self.model = load_learner('model')
        
        self.cwd = Path.cwd()
        
        self.images = self.cwd.joinpath("images")
        
        if not self.images.exists():
            self.images.mkdir()
            
        self.proxy = ""
        
        self.headers  = {
        'authority': 'm.atcdn.co.uk',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="99", "Google Chrome";v="99"',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36',
        'sec-ch-ua-platform': '"Windows"',
        'accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
        'sec-fetch-site': 'cross-site',
        'sec-fetch-mode': 'no-cors',
        'sec-fetch-dest': 'image',
        'referer': 'https://www.autotrader.co.uk/',
        'accept-language': 'en-US,en;q=0.9,ca;q=0.8,cs;q=0.7,fr;q=0.6,hi;q=0.5'
        }
        
    def predict(self,url):
        
        image = self.prepare_image(url)
        
        pred_class,pred_idx,outputs = self.model.predict(image)
        
        return pred_class
    
    def prepare_image(self,url):
        imagePath = self.downloadImage(url)
    
    def download_image(self,url):
        
        response = requests.get(
            url = url,
            headers= self.headers
        )
        
        content = response.content
        
        
        
        