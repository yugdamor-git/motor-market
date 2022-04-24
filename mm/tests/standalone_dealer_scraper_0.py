
# from requests.api import request
from multiprocessing import Process, Queue
import os
import time
import uuid
import scrapy.crawler as crawler
import json
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
import scrapy

import sys
sys.path.append("/var/www/html/car_scrapers/modules")
from Master import Master
from messenger import messenger
mm_bot = messenger()



class GraphQlScrapy:
    def __init__(self):
        self.residential_proxy = "http://sp638d4858_r_2:ac9edf76a737f@gate.smartproxy.com:7000"

    def get_all_data_by_id(self, car_meta, query_type, callback_func):
        url = "https://www.autotrader.co.uk/at-graphql?opname=FPADataQuery"

        query = None

        price_data_query = """
        query FPADataQuery($advertId:String!,$searchOptions:SearchOptions){
            search{
                advert(advertId:$advertId,searchOptions:$searchOptions){
                id
                price
                }
            }
        }
        
        """
        all_data_query = """
        query FPADataQuery($advertId:String!,$searchOptions:SearchOptions){
            search{
                advert(advertId:$advertId,searchOptions:$searchOptions){
                    id
                    colour
                    adminFee
                    imageList{
                        images{
                           url 
                        }
                    }
                    registration
                    year
                    price
                    title
                    priceIndicatorRatingLabel
                    mileage{
                        mileage
                    }
                    sellerContact{
                        phoneNumberOne
                    }
                    vehicleCheckSummary{
                        writeOffCategory
                    }
                    specification{
                        doors
                        wheelbase
                        cabType
                        seats
                        make
                        model
                        trim
                        vehicleCategory
                        ulezCompliant
                        transmission
                        rawBodyType
                        engine{
                            sizeCC
                        }
                        fuel
                    }
                    dealer{
                        dealerId
                        name
                        location{
                            postcode
                        }                
                    }
                }
            }
        }
        """

        if query_type == "all_data":
            query = all_data_query
        else:
            query = price_data_query

        headers = {
            'authority': 'www.autotrader.co.uk',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'x-sauron-app-name': 'sauron-adverts-app',
            'x-sauron-app-version': '1',
            'sec-ch-ua-mobile': '?0',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36',
            'content-type': 'application/json',
            'accept': '*/*',
            'sec-ch-ua-platform': '"Windows"',
            'sec-ch-ua': '"Google Chrome";v="95", "Chromium";v="95", ";Not A Brand";v="99"',
            'origin': 'https://www.autotrader.co.uk',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://www.autotrader.co.uk/',
            'accept-language': 'en-US,en;q=0.9',

        }
        
        payload = [
        {
            "operationName": "FPADataQuery",
            "variables": {
            "advertId": f'{car_meta["id"]}',
            "numberOfImages": 100,
            "searchOptions": {
                "advertisingLocations": [
                "at_cars"
                ],
                "postcode": "wf160pr",
                "collectionLocationOptions": {
                "searchPostcode": "wf160pr"
                },
                "channel": "cars"
            },
            "postcode": "wf160pr"
            },
            "query": query 
        }
        ]

        req_meta = {
            "proxy": self.residential_proxy,
            "vehicle_type":car_meta["vehicle_type"]
        }

        scrapy_req = scrapy.Request(url, headers=headers, method="POST", body=json.dumps(
            payload), callback=callback_func, meta=req_meta)

        return scrapy_req


class DealerScraper_Pipeline(object):
    def process_item(self, item, spider):
        print("INSIDE PIPE LINE", item)
        return item


class DealerScraperSpider(scrapy.Spider):
    name = 'dealer_scraper'
    graphql = GraphQlScrapy()
    all_dealer_ids = ["10028518"]
    # proxy = {"proxy":"http://sp638d4858_2:mysecret007@gate.dc.smartproxy.com:20000"}
    # http://sp638d4858_r_2:ac9edf76a737f@gate.smartproxy.com:7000
    proxy = {"proxy": "http://sp638d4858_r_2:ac9edf76a737f@gate.smartproxy.com:7000"}

    home_url = "https://www.autotrader.co.uk/json/dealers/stock?advertising-location=at_cars&advertising-location=at_profile_cars&dealer={}"
    
    page_url = "https://www.autotrader.co.uk/json/dealers/stock?advertising-location=at_cars&advertising-location=at_profile_cars&dealer={}&page={}"
    
    car_details_url = "https://www.autotrader.co.uk/json/fpa/initial/{}"
    
    obj_master = Master()

    headers = {
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

    def get_all_dealer_ids(self):
        self.obj_master.obj_db.connect()
        query_result = self.obj_master.obj_db.recSelect(
            "fl_dealer_scraper", {"status": "active"})
        for row in query_result:
            self.all_dealer_ids.append(str(row['dealer_id']))
            # if not str(row['dealer_id']) in self.all_dealer_ids:
        self.obj_master.obj_db.disconnect()

    def start_requests(self):
        self.get_all_dealer_ids()
        for dealer_id in self.all_dealer_ids:
            url = self.home_url.format(dealer_id)
            print(dealer_id)
            yield scrapy.Request(url, headers=self.headers, meta=self.proxy, callback=self.extract_total_pages_and_cars)

    def extract_total_pages_and_cars(self, response):
        json_response = response.json()
        total_pages = json_response['stockResponse']['totalPages']
        dealer_id = json_response['pageData']['tracking']['dealer_id']
        # yield {
        #     "total_pages":json_response['stockResponse']['totalPages'],
        #     "total_listings":json_response['stockResponse']['totalResults'],
        #     "dealer_id":json_response['pageData']['tracking']['dealer_id']
        # }

        for page in range(1, total_pages + 1):
            url = self.page_url.format(dealer_id, page)
            yield scrapy.Request(url, headers=self.headers, meta=self.proxy, callback=self.get_page_car_ids)

    def get_page_car_ids(self, response):
        page_json = response.json()
        page_car_ids = self.get_ids_from_json(page_json)
        print(len(page_car_ids))
        for car_id in page_car_ids:
            yield self.graphql.get_all_data_by_id(car_id, "all_data", self.extract_required_data_from_json_2)

    def get_ids_from_json(self, json_data):
        invalid_category = ["S", "C", "D", "N"]
        ids = []
        try:
            stockResponse = json_data['stockResponse']
            for i in stockResponse['results']:
                try:
                    cat = i['vehicle']['writeOffCategory']
                    if not cat in invalid_category:
                        vehicle_type = None
                        if "car-details" in i["targetUrl"]:
                            vehicle_type = "car"
                        else:
                            vehicle_type = "van"
                        ids.append({"id":i['id'],"vehicle_type":vehicle_type})
                except Exception as e:
                    print(str(e))
        except Exception as e:
            pass
        return ids

    def extract_required_data_from_json_2(self, response):
        vehicle_type = response.meta["vehicle_type"]
        json_data = response.json()[0]["data"]["search"]["advert"]
        temp_car_data = {}
        tmp_imgs = []
        dealer = json_data["dealer"]
        try:
            temp_car_data["dealer_name"] = dealer['name']
        except:
            temp_car_data["dealer_name"] = ""
        try:
            temp_car_data["dealer_number"] = json_data["sellerContact"]['phoneNumberOne']
        except:
            temp_car_data["dealer_number"] = ""
        try:
            temp_car_data["dealer_location"] = dealer['location']['postcode'].replace(
                " ", "").strip().upper()
            temp_car_data["location"] = temp_car_data["dealer_location"]
        except:
            temp_car_data["dealer_location"] = ""
            temp_car_data["location"] = ""
        try:
            temp_car_data["dealer_id"] = dealer['dealerId']
        except:
            temp_car_data["dealer_id"] = ""
        ##########################################################################################
        specification = json_data["specification"]
        
        try:
            temp_car_data["wheelbase"] = specification["wheelbase"]
        except:
            temp_car_data["wheelbase"] =  None
        
        try:
            temp_car_data["cabtype"] = specification["cabType"]
        except:
            temp_car_data["cabtype"] = None
        
        try:
            temp_car_data['make'] = specification['make']
        except:
            temp_car_data['make'] = None

        try:
            temp_car_data['model'] = specification['model']
        except:
            temp_car_data['model'] = None

        try:
            temp_car_data['engine_cylinders'] = specification["engine"]["sizeCC"]
        except:
            temp_car_data['engine_cylinders'] = None

        try:
            temp_car_data['year'] = json_data["year"]
        except:
            temp_car_data['year'] = None

        try:
            temp_car_data['seats'] = specification["seats"]
        except:
            temp_car_data['seats'] = None

        try:
            temp_car_data['mileage'] = json_data["mileage"]["mileage"]
        except:
            temp_car_data['mileage'] = None

        try:
            temp_car_data['fuel'] = specification["fuel"]
        except:
            temp_car_data['fuel'] = None

        try:
            temp_car_data['registration'] = json_data["registration"]
        except:
            temp_car_data['registration'] = None

        try:
            temp_car_data['writeOffCategory'] = json_data["vehicleCheckSummary"]["writeOffCategory"]
        except:
            temp_car_data['writeOffCategory'] = None

        try:
            temp_car_data['doors'] = specification['doors']
        except:
            temp_car_data['doors'] = None

        try:
            temp_car_data['body_style'] = specification['rawBodyType']
        except:
            temp_car_data['body_style'] = None

        try:
            temp_car_data['price'] = int(json_data['price'])
        except:
            temp_car_data['price'] = None

        try:
            temp_car_data["price_indicator"] = json_data["priceIndicatorRatingLabel"]
        except:
            temp_car_data["price_indicator"] = None

        try:
            if json_data["adminFee"] != None:
                temp_car_data['admin_fees'] = self.extract_admin_fees(
                    json_data["adminFee"])
            else:
                temp_car_data['admin_fees'] = 0
        except:
            temp_car_data['admin_fees'] = 0

        temp_car_data["price"] = temp_car_data["price"] + temp_car_data["admin_fees"]

        try:
            temp_car_data['trim'] = specification['trim']
        except:
            temp_car_data['trim'] = None
        
        try:
            temp_car_data["vehicle_type"] = specification["vehicleCategory"].lower().strip()
        except:
            temp_car_data["vehicle_type"] = None
        
        try:
            temp_car_data["emission_scheme"] = specification["ulezCompliant"]
        except:
            temp_car_data["emission_scheme"] = 0

        try:
            temp_car_data['transmission'] = specification['transmission']
        except:
            temp_car_data['transmission'] = None

        try:
            temp_car_data['product_url'] = "https://www.autotrader.co.uk/{}-details/{}".format( temp_car_data["vehicle_type"],json_data['id'].strip())
        except:
            temp_car_data['product_url'] = None

        ##########################################################################################
        for img in json_data["imageList"]["images"]:
            tmp_imgs.append(img["url"])

        try:
            if str(temp_car_data["dealer_id"]) in ["10017779", "27396"]:
                tmp_imgs.pop(0)
        except:
            pass

        temp_car_data["images"] = tmp_imgs

        yield temp_car_data



    def extract_admin_fees(self, admin_f):
        try:
            temp = admin_f.strip()
            temp = temp.replace("£", "")
            temp_list = []
            for i in temp:
                try:
                    int(i)
                    temp_list.append(i)
                except:
                    pass
            temp = "".join(temp_list)
            return int(temp)
        except:
            return 0


def f(q, spider):
    try:
        runner = crawler.CrawlerRunner(settings={
            'FEED_URI': f'scrapy_data/{str(uuid.uuid4())}.json',
            'FEED_FORMAT': 'json',
            'CONCURRENT_REQUESTS_PER_DOMAIN': 500,
            'CONCURRENT_REQUESTS': 500,
            'CONCURRENT_ITEMS': 500,
            'LOG_LEVEL': 'DEBUG',
        })
        deferred = runner.crawl(spider)
        deferred.addBoth(lambda _: reactor.stop())
        reactor.run()
        q.put(None)
    except Exception as e:
        q.put(e)


def run_spider(spider):
    q = Queue()
    p = Process(target=f, args=(q, spider,))
    p.start()
    result = q.get()
    p.join()

    if result is not None:
        raise result


def run_main():
    messages = []
    from datetime import datetime
    t1 = datetime.now()
    messages.append(f'*AT Dealer Scraper*')
    messages.append(f'> started : *{t1.strftime("%d-%m-%Y, %H:%M")}*')
    run_spider(DealerScraperSpider)
    t2 = datetime.now()
    messages.append(f'> completed : *{t2.strftime("%d-%m-%Y, %H:%M")}*')
    messages.append(f'> total time : *{(t2 - t1).seconds}* sec')
    print("total time : ", (t2 - t1).seconds)
    mm_bot.send_message("\n".join(messages))
    total_time = f' {t1} - {t2} : total time taken : {(t2 - t1).seconds}' + "\n"


if __name__ == "__main__":
    run_main()
    print("sleeping for 10  seconds...")
    time.sleep(20)
    cmd = "python3 /var/www/html/car_scrapers/auto_trader_dealer_scraper/standalone_at_dealer_parser_1.py"
    os.system(cmd)
