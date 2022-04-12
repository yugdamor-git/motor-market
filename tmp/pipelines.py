# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import hashlib
from io import BytesIO
import mimetypes
import os
from fastcore.foundation import L
from itemadapter import ItemAdapter
from scrapy.pipelines.images import ImageException, ImagesPipeline
from scrapy.utils.misc import md5sum
from scrapy.utils.python import to_bytes
from fastai.vision import *
from fastai.metrics import error_rate
import sys
import json
sys.path.append("/var/www/html/car_scrapers/modules")
from Master import Master
import numpy as np
from scrapy.http import Request

CUR_DIR = os.getcwd()

TEMP_IMG_DIR = os.path.join(CUR_DIR,"temp")

try:
    os.mkdir(TEMP_IMG_DIR)
except:
    pass

SCRAPER_NAME = "dealer_scraper"

IMAGE_DIR_PATH = "/var/www/html/files"

WEBSITE_ID = 'S17'

REDIS_KEY = f'{WEBSITE_ID}_{SCRAPER_NAME}'

WEBSITE_IMAGE_DIR =  os.path.join(IMAGE_DIR_PATH,WEBSITE_ID)

PROXY = "http://sp638d4858_2:mysecret007@gate.dc.smartproxy.com:20000"


class CustomImagePipeLine(ImagesPipeline):
    
    def is_car_image(self,img,path):
        img_name = path.split("/")[-1]
        temp_img_path = os.path.join(TEMP_IMG_DIR,img_name)
        img.save(temp_img_path)
        read_image = open_image(temp_img_path)
        learn = load_learner('model')
        pred_class,pred_idx,outputs = learn.predict(read_image)
        os.remove(temp_img_path)
        #collection ids valid and invalid cards seprately
        if str(pred_class) == 'cars':
            return 1
        return 0
    
    def get_media_requests(self, item, info):
        all_req = []
        urls = ItemAdapter(item).get(self.images_urls_field, [])
        for u in urls:
            req = Request(u,meta={"proxy":PROXY})
            all_req.append(req)
        return all_req
    
    def get_images(self, response, request, info, *, item=None):
        path = self.file_path(request, response=response, info=info, item=item)
        orig_image = self._Image.open(BytesIO(response.body))

        width, height = orig_image.size
        # if width < self.min_width or height < self.min_height:
        #     raise ImageException("Image too small "
        #                          f"({width}x{height} < "
        #                          f"{self.min_width}x{self.min_height})")

        image, buf = self.convert_image(orig_image)
        if self.is_car_image(orig_image,path) == 0:
            raise ImageException("this is not car image")
        
        yield path, image, buf

        for thumb_id, size in self.thumbs.items():
            thumb_path = self.thumb_path(request, thumb_id, response=response, info=info,item=item)
            thumb_image, thumb_buf = self.convert_image(image, size)
            yield thumb_path, thumb_image, thumb_buf
            
    def file_path(self, request, response=None, info=None, *, item=None):
        media_guid = hashlib.sha1(to_bytes(request.url)).hexdigest()
        media_ext = os.path.splitext(request.url)[1]
        # Handles empty and wild extensions by trying to guess the
        # mime type then extension or default to empty string otherwise
        if media_ext not in mimetypes.types_map:
            media_ext = ''
            media_type = mimetypes.guess_type(request.url)[0]
            if media_type:
                media_ext = mimetypes.guess_extension(media_type)
        
        return f'ad{item["ID"]}/org_{media_guid}{media_ext}'
    
    def thumb_path(self, request, thumb_id, response=None, info=None,item=None):
        thumb_guid = hashlib.sha1(to_bytes(request.url)).hexdigest()
        return f'ad{item["ID"]}/{thumb_id}_{thumb_guid}.jpg'

    
class DealerScraperPipeline:
    def __init__(self):
        master = Master()
        self.obj_master = master
    
    def push_redis(self,data):
        self.obj_master.obj_redis_cache.rPush(REDIS_KEY,json.dumps(data))
     
    def get_media_type_and_guid(self,url):
        media_guid = hashlib.sha1(to_bytes(url)).hexdigest()
        media_ext = os.path.splitext(url)[1]
        # Handles empty and wild extensions by trying to guess the
        # mime type then extension or default to empty string otherwise
        if media_ext not in mimetypes.types_map:
            media_ext = ''
            media_type = mimetypes.guess_type(url)[0]
            if media_type:
                media_ext = mimetypes.guess_extension(media_type)
        return {"guid":media_guid,"ext":media_ext}
    
    def process_images(self,item):
        print("############################################################################")
        print(item)
        print("############################################################################")
        all_images = []
        car_img_count = 1
        for img in item["images"]:
            if "org_" in img["path"]:
                img_path = os.path.join("/root/ftptest/",img["path"])
                guid_ext = self.get_media_type_and_guid(img["url"])
                temp = {}
                temp["Listing_ID"] = item["ID"]
                temp["Photo"] = f'{WEBSITE_ID}/ad{item["ID"]}/large_{guid_ext["guid"]}{guid_ext["ext"]}'
                temp["Thumbnail"] = f'{WEBSITE_ID}/ad{item["ID"]}/thumb_{guid_ext["guid"]}{guid_ext["ext"]}'
                temp["Original"] = f'{WEBSITE_ID}/ad{item["ID"]}/org_{guid_ext["guid"]}{guid_ext["ext"]}'
                temp["Type"] = "picture"
                temp["create_ts"] = {"func":"now()"}
                temp["plate_updated_flag"] = 0
                temp["approved_from_dashboard"] = 101
                # if self.is_car_image(img_path)==1:
                img["car_image"] = True
                temp["status"] = "active"
                temp["delete_banner_flag"] = 0
                temp["Position"] = car_img_count
                all_images.append(temp)
                car_img_count += 1
                # else:
                #     temp["status"] = "approval"
                #     temp["Position"] = 100
                #     temp["delete_banner_flag"] = 1
                #     all_images.append(temp)
        self.push_redis({"type":"insert_images","data":all_images})
        return car_img_count,all_images
    
    def process_listing(self,item,car_img_count,all_images):
        status = None
        json_listing = item
        json_listing["Photos_count"] = len(all_images)
        if car_img_count == 1:
            status = "expired"
            why = "zero car images found"
        else:
            status = "active"
            json_listing["Main_photo"] = all_images[0]["Thumbnail"]
            json_listing["images_removed"] = 0
            
        json_listing["status"] = status
        
         
        self.push_redis({"type":"insert_listing","data":json_listing,"ID":item["ID"]})

    def process_item(self, item, spider):
        print("################################## IMAGE #####################################################")
        car_img_count,all_images = self.process_images(item)
        self.process_listing(item,car_img_count,all_images)
        return item
