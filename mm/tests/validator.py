

class validation_limit_datausage:
    def __init__(self):
        self.max_usage_per_execution = 100 # in mb (you can modify it)
        self.usage_per_request = 1 # usage per request (do not change)
        self.recent_listing_days = 3 # here you can set days for most recent lisitngs
    
    def calculate_max_listings(self):
        return int((1024 * self.max_usage_per_execution)/self.usage_per_request)
    
    def get_validation_query(self):
        # total_listings = self.calculate_max_listings()
        
        #query = f'SELECT * FROM `fl_listings` WHERE Website_ID=17 AND Status="active"'
        query = "SELECT * FROM `fl_listings` WHERE Status='active'"
        #query =  'SELECT * FROM `fl_listings` WHERE registration_date is null AND Status IN("active","to_parse")'
        # AND create_ts > DATE_ADD(CURDATE(), INTERVAL - {self.recent_listing_days} DAY) ORDER BY create_ts DESC LIMIT {total_listings}
        return query
    

import scrapy
import dateutil
import sys
sys.path.append("/var/www/html/car_scrapers/modules")
from Master import Master
from price_conditions import price_conditions

import json

class GraphQlScrapy:
    def __init__(self):
        self.residential_proxy = "http://sp638d4858_r_2:ac9edf76a737f@gate.smartproxy.com:7000"
    
    def get_all_data_by_id(self,id,query_type,callback_func,main_meta):
        url = "https://www.autotrader.co.uk/at-graphql?opname=UsedFPADataQuery"
        
        query = None
        
        price_data_query = """
        query UsedFPADataQuery($advertId:String!){
            search{
                advert(advertId:$advertId){
                id
                price
                priceIndicatorRatingLabel
                adminFee
                dateOfRegistration
                }
            }
        }
        
        """
        all_data_query = """
        query UsedFPADataQuery($advertId:String!){
            search{
                advert(advertId:$advertId){
                    id
                    colour
                    adminFee
                    dateOfRegistration
                    imageList{
                        images{
                           url 
                        }
                    }
                    registration
                    year
                    price
                    priceIndicatorRatingLabel
                    title
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
                        seats
                        make
                        model
                        trim
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
        payload= {"query":query,"variables":{"advertId":str(id)}}
        
        req_meta = {
            "proxy":self.residential_proxy
        }
        
        req_meta.update(main_meta)
        
        scrapy_req = scrapy.Request(url,headers=headers,method="POST",body=json.dumps(payload),callback=callback_func,meta=req_meta)
        
        return scrapy_req


class ValidatorSpider(scrapy.Spider):
    name = 'validator'
    obj_master = Master()
    data_url = 'https://www.autotrader.co.uk/json/fpa/initial/{}'
    proxy = "http://sp638d4858_r_2:ac9edf76a737f@gate.smartproxy.com:7000"
    pc = price_conditions()
    vld = validation_limit_datausage()
    graphql = GraphQlScrapy()
    headers = headers = {
    'authority': 'www.autotrader.co.uk',
    'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
    'device_used': 'desktop',
    'sec-ch-ua-mobile': '?0',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
    'accept': '*/*',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    'referer': 'https://www.autotrader.co.uk/',
    'accept-language': 'en-US,en;q=0.9',}
    
    def start_requests(self):
        self.obj_master.obj_db.connect()
        
        # all_active = self.obj_master.obj_db.recSelect("fl_listings",{"status":"active","website_id":17})
        all_active = self.obj_master.obj_db.recCustomQuery(self.vld.get_validation_query())
        # all_active = self.obj_master.obj_db.recSelect("fl_listings",{"status":"to_parse","website_id":17})
        for listing in all_active:
            product_id = listing["product_url"].split("/")[-1].strip()
            yield self.graphql.get_all_data_by_id(product_id,"price",self.validate_response,{"item":listing})
            # url = self.data_url.format(product_id)
            # yield scrapy.Request(url,headers=self.headers,callback=self.validate_response,meta={"proxy":self.proxy,"item":listing})
        
        # all_to_parse = self.obj_master.obj_db.recSelect("fl_listings",{"status":"to_parse","website_id":17})
        # self.obj_master.obj_db.disconnect()
        # for listing in all_to_parse:
        #     product_id = listing["product_url"].split("/")[-1].strip()
        #     url = self.data_url.format(product_id)
        #     yield scrapy.Request(url,headers=self.headers,callback=self.validate_response,meta={"proxy":self.proxy,"item":listing})
    
    def extract_admin_fees(self,admin_f):
        try:
            temp = admin_f.strip()
            temp = temp.replace("£","")
            temp_list = []
            for i in temp:
                try:
                    int(i)
                    temp_list.append(i)
                except:
                    pass
            temp =  "".join(temp_list)
            return int(temp)
        except:
            return 0
    
    def get_car_price(self,response):
        
        json_data = response.json()
        
        listing = json_data["data"]["search"]["advert"]
        
        admin_fee = None
        
        registration_date = listing["dateOfRegistration"]
        
        if listing["adminFee"] != None:
            admin_fee = self.extract_admin_fees(listing["adminFee"])
        else:
            admin_fee  = 0
        
        price  = int(listing["price"])
        
        return price,admin_fee,registration_date
    
    def get_price_indicator(self,response):
        try:
            json_data = response.json()
            price_indicator = json_data["data"]["search"]["advert"]["priceIndicatorRatingLabel"]
            return price_indicator
        except:
            return None
        
    def validate_response(self, response):
        
        if "cloudflare" in str(response.body).lower():
            print("cloudflare : retrying")
            new_request = response.request.replace(dont_filter=True)
            yield new_request
        else:
            json_resp = response.json()
            temp = {}
            listing = response.meta["item"]
            temp["ID"] = listing["ID"]
            temp["product_url"]  = listing["product_url"]
            temp["mm_url"] = listing["mm_product_url"]
            temp["mileage"] = listing["mileage"]
            temp["built"] = listing["built"]
            temp["mm_url"] = listing["mm_product_url"]
            temp["at_url"] = listing["product_url"]
            temp["registration"] = listing["registration"]
            if "errors" in json_resp:
                temp["status"] = "expired"
                temp["price_changed"] = 0
                yield temp
            else:
                temp["price_changed"] = 0
                # car_data = self.extract_required_data_from_json(json_resp)
                old_price = listing["cal_price_from_file"]
                new_price,admin_fees,registration_date = self.get_car_price(response)
                print(new_price)
                print(admin_fees)
                temp["cal_price_from_file"] = new_price
                temp["admin_fees"] = admin_fees
                if listing["price_indicator"] != None:
                    temp["price_indicator_present"] = True
                else:
                    temp["price_indicator_present"] = False
                temp["price_indicator"] = self.get_price_indicator(response)
                current_price = int(listing["price"])
                temp["new_price"] = new_price + admin_fees
                make = listing["make"]
                model = listing["model"]
                engine_cylinders = listing["engine_cylinders"]
                resp = self.pc.get_margin_value(make,model,engine_cylinders,temp["new_price"])
                if temp["cal_price_from_file"] >= 7000 and temp["cal_price_from_file"] <=9000:
                    resp["new_price"] = resp["new_price"] - 150
                elif temp["cal_price_from_file"] < 7000:
                    resp["new_price"] = resp["new_price"] - 250
                    
                print(f'margin price : {resp}')
                if resp["status"] == False:
                    temp["status"] = "expired"
                    temp["price_changed"] = 0
                else:  
                    if old_price == new_price:
                        temp["price_changed"] = 0
                    elif new_price > old_price:
                        updated_margin = new_price - old_price
                        new_current_price = current_price + updated_margin
                        temp["price"] = resp["new_price"]
                        temp["price_changed"] = 1
                        temp["loss"] = updated_margin

                    elif old_price > new_price:
                        updated_margin = old_price - new_price
                        new_current_price = current_price - updated_margin
                        temp["price"] = resp["new_price"]
                        temp["profit"] = updated_margin
                        temp["price_changed"] = 2
                    temp["registration_date"] = registration_date
                    temp["status"] =  "active"
                yield temp
