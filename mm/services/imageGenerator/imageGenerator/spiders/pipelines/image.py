import hashlib
from io import BytesIO
from scrapy.utils.python import to_bytes
import os
from scrapy.pipelines.images import ImageException, ImagesPipeline

from ..topic import producer

class ImageSpiderPipeline:
    
    def __init__(self):  
        self.topic = 'motormarket.scraper.autotrader.listing.database.local'
        
        self.producer = producer.Producer(self.topic)
        
        logsTopic = "motormarket.scraper.logs"
    
        self.logsProducer = producer.Producer(logsTopic)
        
        self.imagePathPrefix = os.environ.get("IMAGE_PATH_PREFIX","/var/www/html/files")
        
        self.imageTypes = [
            "org",
            "thumb",
            "large"
        ]
        
    def process_item(self, item, spider):
        
        print(item)
        
        data = item["data"]
        
        meta = data["meta"]
        
        try:
        
            images = item["images"]
            
            registration = item["registration"]
            
            websiteId = item["websiteId"]
            
            imagesPath = []
            
            for image in images:
                tmp = {}
                guid = hashlib.sha1(to_bytes(image["url"])).hexdigest()
                
                for imgType in self.imageTypes:
                    
                    path = f'{self.imagePathPrefix}/{websiteId}/{registration}/{imgType}_{guid}.jpg'
                    
                    tmp[imgType] = path
                
                tmp["url"] = image["url"]
                
                imagesPath.append(tmp)
            
            thumbImage = imagesPath[0]["thumb"]
            
            data["data"]["images"] = imagesPath
            
            data["data"]["thumbImage"] = thumbImage
            
            self.producer.produce(data)
        except Exception as e:
            print(f'invalid message : {str(e)}')
            log = {}
            log["errorMessage"] = str(e)
            log["service"] = "services.image.generator.pipeline"
            log["sourceUrl"] = meta["sourceUrl"]
            self.logsProducer.produce(log)
        
        return item


class ImageGeneratorPipeline(ImagesPipeline):
    
    def file_path(self, request, response=None, info=None, *, item=None):
        
        guid = hashlib.sha1(to_bytes(request.url)).hexdigest()
        
        return f'{item["websiteId"]}/ad{item["uniqueId"]}/org_{guid}.jpg'
    
    def get_images(self, response, request, info, *, item=None):
        path = self.file_path(request, response=response, info=info, item=item)
        
        orig_image = self._Image.open(BytesIO(response.body))

        width, height = orig_image.size

        image, buf = self.convert_image(orig_image)
        
        for thumb_id, size in self.thumbs.items():
            thumb_path = self.thumb_path(request, thumb_id, response=response, info=info,item=item)
            thumb_image, thumb_buf = self.convert_image(image, size)
            yield thumb_path, thumb_image, thumb_buf
        
        yield path, image, buf

    def thumb_path(self, request, thumb_id, response=None, info=None,item=None):
        
        guid = hashlib.sha1(to_bytes(request.url)).hexdigest()
        
        return f'{item["websiteId"]}/ad{item["uniqueId"]}/{thumb_id}_{guid}.jpg'