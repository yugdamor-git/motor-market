import scrapy
import sys
sys.path.append("...")
from scrapy.crawler import CrawlerProcess
from topic import producer,consumer
from Database import Database

import os

class Helper:
    def __init__(self) -> None:
        
        self.db = Database()
        
        self.proxy = os.environ.get("RESIDENTIAL_PROXY")
        
        self.maxRetry = 3
        
        self.accountId = 24898
        
        self.invalid_category = ["S", "C", "D", "N"]

        self.headers = {
            'authority': 'www.autotrader.co.uk',
            'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
            'device_used': 'desktop',
            'sec-ch-ua-mobile': '?0',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
            'accept': '*/*',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://www.autotrader.co.uk/',
            'accept-language': 'en-US,en;q=0.9'
            } 
        
    
    def getPageCountRequest(self,dealerId,meta,callback_func):
        url = f'https://www.autotrader.co.uk/json/dealers/stock?advertising-location=at_cars&advertising-location=at_profile_cars&dealer={dealerId}'
        meta["proxy"] = self.proxy
        return scrapy.Request(url,method="GET",headers=self.headers,meta=meta,callback=callback_func)
    
    def getDealerListingsIdByPageRequest(self,dealerId,page,meta,callback_func):
            
        url = f'https://www.autotrader.co.uk/json/dealers/stock?advertising-location=at_cars&advertising-location=at_profile_cars&dealer={dealerId}&page={page}'
        meta["proxy"] = self.proxy
        return scrapy.Request(url,method="GET",headers=self.headers,meta=meta,callback=callback_func)
    
    def extractDealerListingsIdByPage(self,response):
        
        listings = []
        
        jsonResponse = response.json()
                
        stockResponse = jsonResponse["stockResponse"]
        
        results = stockResponse["results"]
        
        for item in results:
            writeOffCategory = item['vehicle']['writeOffCategory']
            if not writeOffCategory in self.invalid_category:
                listings.append(item["id"])
        
        return listings
    
    def extractPageCount(self,response):
        
        jsonResponse = response.json()
        
        totalPages = int(jsonResponse["stockResponse"]["totalPages"])
        
        return totalPages
    
    
    def get_all_db_listing(self):
        
        retry = 5
        listings = []
        self.db.connect()
        for i in range(0,retry):
            try:
                listings = self.db.recCustomQuery("SELECT sourceId,dealer_id FROM fl_listings WHERE Status IN('active','pending')")
                break
            except:
                pass
        self.db.disconnect()
        
        return listings
    
    def group_by_dealer_id(self,listings):
        groupByDealerId = {}

        for listing in listings:
            if not listing["dealer_id"] in groupByDealerId:
                groupByDealerId[listing["dealer_id"]] = []
            groupByDealerId[listing["dealer_id"]].append(listing["sourceId"])
        
        return groupByDealerId
    
    

class DealerListingValidatorPipeline:
    def __init__(self) -> None:
        self.data = {}
        finderTopic = 'motormarket.scraper.autotrader.listing.database.finder'
        self.finderProducer = producer.Producer(finderTopic)
        
        self.fl_listings = 'motormarket.scraper.autotrader.listing.database.production'
        
        self.fl_listings_producer = producer.Producer(self.fl_listings)
        
        
        
    def open_spider(self,spider):
        pass
    
    def scrapeNewlisting(self,sourceId):
        print(f'new listing : {sourceId}')
        data =  {
            "data":{
            "sourceId":f'{sourceId}',
            "sourceUrl":f'https://www.autotrader.co.uk/car-details/{sourceId}',
            "websiteId":"17",
            "scraperName":"dealer-scraper"
            }
        }
        
        self.finderProducer.produce(data)
    
    def updateExpiredListing(self,sourceId):
        print(f'expired listing : {sourceId}')
        event = "update"
        what = {
            "Status":"expired",
            "why":"no longer active on auto trader",
            
        }
            
        where = {
            "sourceId":sourceId
        }
        
        data = {
            "event":event,
            "eventData":{
                "what":what,
                "where":where
            }
        }
        
        self.fl_listings_producer.produce(data)
        
    
    def close_spider(self,spider):
        for dealerId in self.data:
            oldListingIds = {str(id) for id in self.data[dealerId]["old"]}
            newlistingIds = {str(id) for id in self.data[dealerId]["new"]}
            
            if len(newlistingIds.keys()) == 0:
                continue
            
            for old_id in oldListingIds:
                if not old_id in newlistingIds:
                    self.updateExpiredListing(old_id)
            
            for new_id in newlistingIds:
                if not new_id in oldListingIds:
                    self.scrapeNewlisting(new_id)
    
    def process_item(self,item,spider):
        
        dealerId = item["dealerId"]
        
        if not dealerId in self.data:
            self.data[dealerId] = {
                "old":item["oldListingIds"],
                "new":{}
            }
        
        for id in item["scrapedListingIds"]:
            self.data[dealerId]["new"][id] = 1
            
        return item
    
    
    

class DealerListingValidator(scrapy.Spider):
    name = 'dealer-listing-validator'
    
    # logsTopic = "motormarket.scraper.logs"
            
    # logsProducer = producer.Producer(logsTopic)
    
    helper = Helper()
    
    MAX_RETRY_COUNT = 6
    
    def start_requests(self):
        
        listings = self.helper.get_all_db_listing()
        
        groupByDealerId = self.helper.group_by_dealer_id(listings)
        
        index = 0
        for dealerId in groupByDealerId:
            oldListingIds = {str(id) for id in groupByDealerId[dealerId]}
            
            meta = {
                "dealerId":dealerId,
                "oldListingIds":oldListingIds,
                "retryCount":0
            }
            
            yield self.helper.getPageCountRequest(dealerId,meta,self.getListingIds)
            
            if index > 20:
                break
            
            index += 1
        
    def getListingIds(self,response):
        
        meta = response.meta
        
        if response.status == 403 or "cloudflare" in response.text.lower():
            response.meta["retryCount"] += 1
            new_request = response.request.replace(dont_filter=True)
            
            if response.meta["retryCount"] < self.MAX_RETRY_COUNT:
                yield new_request
            
        dealerId = meta.get("dealerId")
        try:
            
            totalPages = self.helper.extractPageCount(response)
            print(f'total pages : {totalPages}')
            for page in range(1,totalPages+1):
                yield self.helper.getDealerListingsIdByPageRequest(dealerId,page,meta,self.extractListingIds)
            
        except Exception as e:
            print(f'error : {str(e)}')
            response.meta["retryCount"] += 1
            new_request = response.request.replace(dont_filter=True)
            
            if response.meta["retryCount"] < self.MAX_RETRY_COUNT:
                yield new_request
    
    def extractListingIds(self,response):
        
        meta = response.meta
        
        if response.status == 403 or "cloudflare" in response.text.lower():
            response.meta["retryCount"] += 1
            new_request = response.request.replace(dont_filter=True)
            
            if response.meta["retryCount"] < self.MAX_RETRY_COUNT:
                yield new_request
        
        dealerId = meta.get("dealerId")
        
        oldListingIds = meta.get("oldListingIds")
        try:
            scrapedListingIds = self.helper.extractDealerListingsIdByPage(response)
        except:
            response.meta["retryCount"] += 1
            new_request = response.request.replace(dont_filter=True)
            
            if response.meta["retryCount"] < self.MAX_RETRY_COUNT:
                yield new_request
        data = {
            "dealerId":dealerId,
            "oldListingIds":oldListingIds,
            "scrapedListingIds":scrapedListingIds
        }
        
        yield data
        
        
if __name__ == "__main__":
    
    settings = {
        "ROBOTSTXT_OBEY":False,
        "HTTPERROR_ALLOWED_CODES":[403],
        "ITEM_PIPELINES":{
            DealerListingValidatorPipeline:300
        }
    }
    
    c = CrawlerProcess(settings)
    
    c.crawl(DealerListingValidator)
    
    c.start()
    