import scrapy


class DealerlistingSpiderPipeline:
    
    def process_item(self, item, spider):
        return item


class DealerlistingSpider(scrapy.Spider):
    name = 'dealerListing'
    allowed_domains = ['https']
    start_urls = ['http://https/']

    def parse(self, response):
        pass
