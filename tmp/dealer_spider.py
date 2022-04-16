import scrapy
import sys
sys.path.append("/var/www/html/car_scrapers/modules")
from Master import Master
import json


class DealerSpiderSpider(scrapy.Spider):
    name = 'dealer_spider'
    
    root_url = 'https://m.atcdn.co.uk/a/media/67716aca08c0482f9d466749ceb1775d.jpg'
    
    obj_master = Master()
    
    max_images = 25
    
    instance_id = 1
    
    
    def start_requests(self):
        yield scrapy.Request(self.root_url,callback=self.parse_images)
    
    
    
    def parse_images(self, response):
        self.obj_master.obj_db.connect()
        batch_to_parse = self.obj_master.obj_db.recSelect("fl_listings",{"status":"to_parse","Website_ID":17,"scrapy_instance_id":self.instance_id},limit=300)
        self.obj_master.obj_db.disconnect()
        for listing in batch_to_parse:
            json_str = listing["listing_json"]
            
            json_listing = json.loads(json_str)
            
            json_listing["ID"] = listing["ID"]
            
            json_listing["image_urls"]  = json_listing["images"][0:self.max_images]
            
            yield json_listing