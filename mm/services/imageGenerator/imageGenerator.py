from io import BytesIO
from pathlib import Path
from PIL import Image
from concurrent.futures import ThreadPoolExecutor,as_completed
from ftpHandler import ftpHandler

class imageGenerator:
    def __init__(self) -> None:
        self.sizes = [
            {
                "name":"thumb",
                "h":270,
                "w":180
            },
            {
                "name":"large",
                "h":900,
                "w":600
            },
        ]
    
    def convert_image(self, image, size=None):
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
    
    def generateImages(self,websiteId,listingId,imageId,imagePath,imageUrl,position):
        processedImages = {}
        processedImages["id"] = imageId
        processedImages["url"] = imageUrl
        processedImages["position"] = position
        processedImages["status"] = False
        
        ftp = ftpHandler()
        
        try:
            rawImage = self.read_image(imagePath)
            
            orgImagePath = f'S{websiteId}/ad{listingId}/org_{imageId}.jpg'
            tmp = {}
            tmp["path"] = orgImagePath
            tmp["type"] = "org"
            tmp["size"] = None
            
            processedImages["org"] = tmp
            
            for size in self.sizes:
                tmp = {}
                imagePathTmp = f'S{websiteId}/ad{listingId}/{size["name"]}_{imageId}.jpg'
                tmp['path'] = imagePathTmp
                tmp['type'] = size["name"]
                tmp["size"] = size
                processedImages[size["name"]] = tmp
            
            imageStats = ftp.getFileStats(orgImagePath)
            
            if imageStats["exists"] == True:
                
                processedImages["status"] = True
                
                processedImages["exists"] = True
                
                return processedImages
            
            
            _,orgImage = self.convert_image(rawImage)
            # upload image in server through ftp
            ftp.uploadFile(orgImagePath,orgImage)
            
            for size in self.sizes:
                _,buff = self.convert_image(rawImage,size)
                imagePathTmp = f'S{websiteId}/ad{listingId}/{size["name"]}_{imageId}.jpg'
                # upload image in server through ftp
                ftp.uploadFile(imagePathTmp,buff)
            
            processedImages["status"] = True
            processedImages["exists"] = False
            
            try:
                deletePath = Path(imagePath)
                deletePath.unlink()
                print(f'file deleted : {deletePath}')
            except:
                pass
            
            return processedImages
        
        except Exception as e:
            print(f'error : {str(e)}')
        
        ftp.disconnect()
        
        return processedImages
        
    def processListing(self,images,websiteId,listingId):
        processedImages = []
        
        threads = []
        
        ftp = ftpHandler()
        
        dirname = f'S{websiteId}/ad{listingId}'

        ftp.createDirectory(f'{ftp.imageDir}/{dirname}')
        
        with ThreadPoolExecutor(max_workers=15) as executor:
            for item in images:
                
                imageId = item["id"]
                
                imagePath = item["path"]
                
                imageUrl = item["url"]
                
                position = item["position"]
                
                threads.append(executor.submit(self.generateImages,websiteId,listingId,imageId,imagePath,imageUrl,position))
        
            for task in as_completed(threads):
                data = task.result()
                
                processedImages.append(data)
        
        ftp.disconnect()
        
        return processedImages



if __name__ == "__main__":
    ig = imageGenerator()
    
    cwd = Path.cwd()
    
    imageDir = cwd.joinpath("images")
    
    
    images = [
        {
            "id":"hjshja",
            "path":f'{imageDir.joinpath("test.png")}',
            "url":"xyz"
        }
    ]
    websiteId = "s56"
    listingId = "a12345"
    
    
    t = ig.processListing(images,websiteId,listingId)
    print(t)
    # cwd = Path.cwd()
    
    # imagePath = cwd.joinpath("test.png")
    
    # img = ig.read_image(imagePath)
    
    # for size in ig.sizes:
    #     print(f'size :{size}')
    #     im,buff = ig.convert_image(img,size)
    #     with open(f'{size["name"]}.jpg',"wb") as f:
    #         f.write(buff.getvalue())
    
    