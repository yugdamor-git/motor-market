from io import BytesIO
from pathlib import Path
from turtle import position
from PIL import Image
from concurrent.futures import ThreadPoolExecutor,as_completed

class imageGenerator:
    def __init__(self) -> None:
        self.size = {
                "name":"thumb",
                "h":270 * 2,
                "w":180 * 2
            }
    
    def convert_image(self, image,):
        size = self.size
        if image.format == 'PNG' and image.mode == 'RGBA':
            background = self._Image.new('RGBA', image.size, (255, 255, 255))
            background.paste(image, image)
            image = background.convert('RGB')
        elif image.mode == 'P':
            image = image.convert("RGBA")
            background = self._Image.new('RGBA', image.size, (255, 255, 255))
            background.paste(image, image)
            image = background.convert('RGB')
        elif image.mode != 'RGB':
            image = image.convert('RGB')

        if size:
            image = image.copy()
            image.thumbnail((size["h"],size["w"]), Image.Resampling.LANCZOS)

        buf = BytesIO()
        
        image.save(buf, 'JPEG')
        
        return image, buf

    def read_image(self,imagePath):
        img = None
        
        with open(imagePath,"rb") as f:
            img = Image.open(BytesIO(f.read()))  
        return img
    
    def generatePredictionImage(self,filePath,predictionImagePath):
        if not predictionImagePath.exists():
            downloadedImage = self.read_image(filePath)
                
            newImage,_ = self.convert_image(downloadedImage)
            
            newImage.save(predictionImagePath)
        return predictionImagePath

if __name__ == "__main__":
    ig = imageGenerator()
    
    filePath = Path.cwd().joinpath("test.png")
    
    filePath2 = Path.cwd().joinpath("prediction_test.png")
    
    ig.generatePredictionImage(filePath,filePath2)