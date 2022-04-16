import scrapy

from .pipelines import image

from .topic import consumer,producer

import os


class ImageSpider(scrapy.Spider):
    name = 'image'
    
    topic = "motormarket.scraper.autotrader.listing.image.generator"
    
    consumer = consumer.Consumer(topic)
    
    logsTopic = "motormarket.scraper.logs"
    
    logsProducer = producer.Producer(logsTopic)
    
    headers  = {
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
    
    ip = os.environ.get("PRODUCTION_SERVER_IP_ADDRESS")
    port = os.environ.get("PRODUCTION_SERVER_FTP_PORT")
    imageDir = os.environ.get("PRODUCTION_SERVER_IMAGE_DIR_PREFIX")
    
    
    IMAGES_STORE = f'ftp://{ip}:{port}{imageDir}'
    
    custom_settings = {
         "ROBOTSTXT_OBEY":False,
         "RETRY_ENABLED":True,
         "CONCURRENT_REQUESTS":32,
         "COOKIES_ENABLED":False,
         "RETRY_TIMES":3,
         "LOG_LEVEL":"DEBUG",
         "ITEM_PIPELINES" : {
            image.ImageGeneratorPipeline: 1,
            image.ImageSpiderPipeline: 300,
        },
         "IMAGES_THUMBS":{
                        'thumb': (270, 180),
                        'large': (900, 600),
                            },
         "MEDIA_ALLOW_REDIRECTS":True,
         "FTP_USER":os.environ.get("PRODUCTION_SERVER_FTP_USERNAME"),
         "FTP_PASSWORD":os.environ.get("PRODUCTION_SERVER_FTP_PASSWORD"),
         "IMAGES_STORE":IMAGES_STORE,
         "DEFAULT_REQUEST_HEADERS":headers,
         "DOWNLOADER_MIDDLEWARES":{
             'imageGenerator.middlewares.ImagegeneratorDownloaderMiddleware': 543
         }
    }
    
    def start_requests(self):
        
        yield scrapy.Request(
            "https://motor.market",
            dont_filter=True,
            callback=self.process_images)
    
    def process_images(self,response):
        
        while True:
            
#             data = {'data': {'dealerName': 'mountbatten car sales', 'dealerNumber': '(01454) 279142', 'dealerLocation': 'GL128BD', 'location': 'GL128BD', 'dealerId': '490215', 'wheelBase': None, 'cabType': 'unlisted', 'make': 'bmw', 'model': '6 series', 'engineCylinders': 2996, 'built': 2015, 'seats': None, 'mileage': 89700, 'fuel': 'petrol', 'registration': 'V****06', 'writeOffCategory': None, 'doors': 2, 'bodyStyle': 'coupe', 'price': 3500, 'priceIndicator': '', 'adminFee': None, 'trim': None, 'vehicleType': 'car', 'emissionScheme': False, 'transmission': 'unlisted', 'id': '202203253932273', 'images': ['https://m.atcdn.co.uk/a/media/{resize}/3f166a2cd72c4630b326f09634647912.jpg', 'https://m.atcdn.co.uk/a/media/{resize}/cbb9975c54014d93944fb2364a98fe85.jpg', 'https://m.atcdn.co.uk/a/media/{resize}/b904ce49b3e84e258358c065a623f81e.jpg', 'https://m.atcdn.co.uk/a/media/{resize}/aedc7572b13f4ea980429437de772e1f.jpg', 'https://m.atcdn.co.uk/a/media/{resize}/acf84040ee7243299aa19b113711fc8b.jpg', 'https://m.atcdn.co.uk/a/media/{resize}/0430e5afe4b34af69756ed03ace2ecf8.jpg', 'https://m.atcdn.co.uk/a/media/{resize}/cc8d52a97bfd42dbb911f5e25feb47f4.jpg', 'https://m.atcdn.co.uk/a/media/{resize}/3e1aa4885c3e43d9a8e1a13a078c2e7e.jpg', 'https://m.atcdn.co.uk/a/media/{resize}/8e3951b87c1140069482f6e8b6ecbe3a.jpg', 'https://m.atcdn.co.uk/a/media/{resize}/75cb9eaad69e47dd9b1cd7ccb0c9306e.jpg', 'https://m.atcdn.co.uk/a/media/{resize}/7b988e59950d43118676f6260426efd5.jpg', 'https://m.atcdn.co.uk/a/media/{resize}/42e957bf9ae3478394a0bba9c20309f4.jpg', 'https://m.atcdn.co.uk/a/media/{resize}/2c7340a57e7b47db87ec11ec78cbe6d7.jpg', 'https://m.atcdn.co.uk/a/media/{resize}/c0feb035fc624f74a146758e7727f03a.jpg', 'https://m.atcdn.co.uk/a/media/{resize}/6696f3a949c6450d8e373cebf01ea436.jpg', 'https://m.atcdn.co.uk/a/media/{resize}/bed3348acb644f2fab1eb25f9cc71f68.jpg', 'https://m.atcdn.co.uk/a/media/{resize}/d8ddb0c4a85145fc9b1e5b82f61e8374.jpg', 'https://m.atcdn.co.uk/a/media/{resize}/b086904763a5479b85bffa8375752673.jpg', 'https://m.atcdn.co.uk/a/media/{resize}/c7369a7d629243a6948c731df3d92ce2.jpg'], 'url': 'https://www.autotrader.co.uk/Car-details/202203253932273', 'engineCylindersCC': 2996, 'engineCylindersLitre': 3.0, 'bodyStylePredicted': 'Coupe', 'title': 'Bmw 6 Series ', 'transmissionCode': 4, 'fuelCode': 1, 'margin': 1415, 'predictedMake': 'BMW', 'predictedModel': '6-Series', 'makeModelPredictionScore': 0.7745966692414836, 'predictedSeats': '5.0', 'seatsPredictionScore': 0.7706637045651281, 'pcpapr': {'0.069': 68, '0.079': 70, '0.089': 71, '0.099': 73, '0.109': 75, '0.119': 76, '0.129': 78, '0.139': 79, '0.149': 81, '0.159': 82, '0.169': 84, '0.179': 86, '0.189': 87, '0.199': 89, '0.209': 91, '0.219': 92, '0.229': 94, '0.239': 95, 
# '0.249': 97, '0.259': 99, '0.269': 100, '0.279': 102, '0.289': 104, '0.299': 105, '0.309': 107, '0.319': 108, '0.329': 110, '0.339': 112, '0.349': 113, '0.359': 115, '0.369': 117, '0.379': 118, '0.389': 120, '0.399': 122, '0.409': 123, '0.419': 125, '0.429': 126, '0.439': 128, '0.449': 130, '0.459': 131, '0.469': 133, '0.479': 135, '0.489': 136, '0.499': 138, '0.399_48': 134, '0.499_48': 149, '0.299_48': 118}}, 'meta': {'uniqueId': '202203253932273', 'sourceUrl': 'https://www.autotrader.co.uk/car-details/202203253932273', 'websiteId': 's21'}}
            data = self.consumer.consume()
            
            meta = data["meta"]
            
            print(data)
            
            try:
                
                images = data["data"]["images"]
                
                uniqueId = meta["uniqueId"]
                
                websiteId = meta["websiteId"]
                
                yield {
                    "image_urls":images,
                    "uniqueId":uniqueId,
                    "data":data,
                    "websiteId":websiteId
                }
                
                break
                
                
            except Exception as e:
                print(f'error : {str(e)}')
                log = {}
                log["errorMessage"] = str(e)
                log["service"] = "services.image.generator.spider"
                log["sourceUrl"] = meta["sourceUrl"]
            