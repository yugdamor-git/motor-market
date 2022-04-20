import base64
from fastai.vision import *
from pathlib import Path
from imageDownloader import ImageDownloader


class Predictor:
    def __init__(self):
        print("seat predictor init")

        self.model = load_learner('model')
        
        self.downloader = ImageDownloader()
        
    def predict(self,urls):
        
        predicted = []
        
        downloadedImages = self.prepare_images(urls)
        
        for image in downloadedImages:
            
            tmp = image.copy()
            
            img = open_image(image["filePath"])
            
            pred_class,pred_idx,outputs = self.model.predict(img)
            
            print(pred_class)
            
            if not str(pred_class) in ["cars"]:
                print(f'this is not car image : {image["url"]}')
                continue
            
            tmp["imageClass"] = str(pred_class)
            
            tmp["base64"] = self.encode_image_base64(image["filePath"])
            
            predicted.append(tmp)
            
        return predicted
    
    def encode_image_base64(self,imagePath):
        
        base64str = base64.b64encode(imagePath.read_bytes()).decode("utf-8")
        
        return base64str
    
    def prepare_images(self,urls):
        downloadedImages = self.downloader.download_multiple_images(urls)
        return downloadedImages


if __name__ == "__main__":
    p = Predictor()
    
    images = ["https://m.atcdn.co.uk/a/media/{resize}/afe49aa5f1264a7aa09370f3627c941b.jpg", "https://m.atcdn.co.uk/a/media/{resize}/e99f4adc2a1e4592be299887ccb6ec0c.jpg", "https://m.atcdn.co.uk/a/media/{resize}/c05ce15fcf5a4b8492f494a20d47b3f1.jpg", "https://m.atcdn.co.uk/a/media/{resize}/141afafe70864a9aafe0a245ced2de84.jpg", "https://m.atcdn.co.uk/a/media/{resize}/9f8024c5d72549dda2c5d0745b741a13.jpg", "https://m.atcdn.co.uk/a/media/{resize}/6bb5da849b264e3f8dca83305a9b3d74.jpg", "https://m.atcdn.co.uk/a/media/{resize}/25c0f2d80a6d43cdb30fc8eff2257186.jpg", "https://m.atcdn.co.uk/a/media/{resize}/5eab31df67194588871d0940523a9abe.jpg", "https://m.atcdn.co.uk/a/media/{resize}/ed5a385e4a564dadbffa398072542eb3.jpg", "https://m.atcdn.co.uk/a/media/{resize}/306d017e1d144d6682ce7a19df2b2f7c.jpg", "https://m.atcdn.co.uk/a/media/{resize}/417154844dfa43dfb467ca2695a39cce.jpg", "https://m.atcdn.co.uk/a/media/{resize}/bb406a889beb4a839b4d5ea5bb695437.jpg", "https://m.atcdn.co.uk/a/media/{resize}/764a18c45e2549caaae4ed302ec5fc15.jpg", "https://m.atcdn.co.uk/a/media/{resize}/d0a42f27fa1a4da4b700ad6a37f38e6e.jpg", "https://m.atcdn.co.uk/a/media/{resize}/e761e61276d94764bc141b0057cd45bb.jpg", "https://m.atcdn.co.uk/a/media/{resize}/af9319a8d2a54826847b10827547e8c0.jpg", "https://m.atcdn.co.uk/a/media/{resize}/d75331e35a774c32bfb8570e961263fc.jpg", "https://m.atcdn.co.uk/a/media/{resize}/e3e82173f8724591aebdfaf42538fe0a.jpg", "https://m.atcdn.co.uk/a/media/{resize}/084b7f4e6d9644aa97a63fd60420c268.jpg", "https://m.atcdn.co.uk/a/media/{resize}/ac01f43abd49479ea0fd9908e891ddb7.jpg"]

    pred = p.predict(images)
    
    # for pr in pred:
    #     print(pr)
    #     print('----------------------------------------------------------------------------------------------------')
        