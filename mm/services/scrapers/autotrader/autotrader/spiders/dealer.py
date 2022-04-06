import scrapy


class DealerSpider(scrapy.Spider):
    name = 'dealer'
    allowed_domains = ['']
    start_urls = ['http:///']

    def parse(self, response):
        pass
