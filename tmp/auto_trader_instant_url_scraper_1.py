# this is data extraction script.

############### CONFIG AREA ###################################################


import random
import requests
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium import webdriver
import datetime
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import traceback
import os
import logging
import time
import pickle
import uuid
from tqdm import tqdm
import json
import sys
import subprocess
HIDDEN_MODE = True



MAX_MILEAGE = 120 * 1000

search_url = "https://www.autotrader.co.uk/car-search?sort=relevance&postcode=wf160pr&radius=1500&include-delivery-option=on&year-from=2014&maximum-mileage="+str(MAX_MILEAGE)+"&exclude-writeoff-categories=on&ma=Y&page={}"

ACCOUNT_ID = 24898
WEBSITE_ID = 17
PRIORITY = 109
SCRAPER_NAME = "auto_trader_instant_url"

CURRENT_SCRIPT_STATUS_KEY = "{}_{}_scraper".format(SCRAPER_NAME, WEBSITE_ID)
sys.path.append("../modules")
from Master import Master
obj_master = Master()

from GraphqlApi import GraphqlApi

graphql = GraphqlApi()


############### CONFIG AREA ###################################################


def already_running(script_name):
    import psutil
    num_instance = 0
    for q in psutil.process_iter():
        if 'python' in q.name():
            if len(q.cmdline()) > 1 and script_name in q.cmdline()[1]:
                num_instance += 1
    return num_instance > 1


####################################### logs #########################################
cur_script = os.path.basename(__file__)
cur_dir = os.getcwd()
log_dir = os.path.join(cur_dir, "logs")
log_file_path = os.path.join(log_dir, cur_script + '.log')
logging.basicConfig(filename=log_file_path, filemode='a', level=logging.INFO,
                    format='%(process)d - %(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
####################################### logs #########################################

####################################### checking if script already running or not #########################################
script_status = already_running(cur_script)
if script_status == True:
    logging.info("The other instance of this script is already running....")
    logging.info("Exiting...")
    exit(0)
else:
    logging.info(
        "No other instance is running...Executing this instance of script")
    # time.sleep(100)
####################################### checking if script already running or not #########################################


def setup_firebase():
    import firebase_admin
    from firebase_admin import credentials
    from firebase_admin import firestore
    import os
    cur_dir = os.getcwd()
    firebase_dir = os.path.join(cur_dir, "firebase")

    cred = credentials.Certificate(os.path.join(
        firebase_dir, "firebase_motor_market.json"))
    firebase_admin.initialize_app(cred)
    firebase_db = firestore.client()
    return firebase_db


firebase_db = setup_firebase()
scraped_files_dir = os.path.join(cur_dir, "scraped_files")
try:
    os.mkdir(scraped_files_dir)
except:
    pass


def set_cron_status(msg):
    try:
        import datetime
        ct = datetime.datetime.now()

        global firebase_db
        website_id = str(WEBSITE_ID)
        script_no = "10"
        script_name = os.path.basename(__file__)
        doc_ref = firebase_db.collection("cron_management").document(
            website_id).collection("CRON").document(script_no)
        doc_ref.set({
            "name": script_name,
            "start": ct.timestamp(),
            "end": 0,
            "message": "running...",
            "running": True,
            "error": ""

        })
    except:
        pass


def update_cron_status(msg, error):
    try:
        global firebase_db
        import datetime
        ct = datetime.datetime.now()

        website_id = str(WEBSITE_ID)
        script_no = "10"

        script_name = os.path.basename(__file__)
        doc_ref = firebase_db.collection("cron_management").document(
            website_id).collection("CRON").document(script_no)
        doc_ref.update({
            "name": script_name,
            "end": ct.timestamp(),
            "message": msg,
            "running": False,
            "error": error

        })
    except:
        pass


def create_webdriver():
    options = webdriver.ChromeOptions()
    if HIDDEN_MODE == True:
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-geolocation")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36")
    wd = webdriver.Chrome("chromedriver", options=options)
    wd.fullscreen_window()
    wd.set_window_size(1920, 1080)
    return wd


def get_car_json_from_id(id):
    import json
    import requests
 

    residential_proxy = {
        "http": "http://sp638d4858_r_2:ac9edf76a737f@gate.smartproxy.com:7000",
        "https": "http://sp638d4858_r_2:ac9edf76a737f@gate.smartproxy.com:7000"
    }

    headers = {
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
        'accept-language': 'en-US,en;q=0.9'
    }
    CAR_DETAILS_URL = f'https://www.autotrader.co.uk/json/fpa/initial/{id}'
    for i in range(0, 10):
        resp = requests.get(CAR_DETAILS_URL, headers=headers,
                            proxies=residential_proxy)
        if resp.status_code == 200:
            break
    json_data = resp.json()
    
    return json_data


def extract_admin_fees(admin_f):
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


def extract_required_data_from_json(car_json):
    temp_car_data = {}
    try:
        seller = car_json['seller']
    except:
        seller = None

    try:
        temp_car_data['location'] = seller['location']
    except:
        temp_car_data['location'] = None

    try:
        vehicle = car_json['vehicle']
    except:
        vehicle = None

    try:
        tracking = car_json['pageData']['tracking']
    except:
        tracking = None

    try:
        advert = car_json['advert']
    except:
        advert = None
    admin_fees = 0

    try:
        if advert['noAdminFees'] == False:
            admin_fees = extract_admin_fees(advert['adminFee'])
    except Exception as e:
        print(str(e))

    try:
        key_facts = vehicle['keyFacts']
    except:
        key_facts = None
    ####################### DEALER ############################################
    try:
        temp_car_data["dealer_name"] = seller['name']
    except:
        temp_car_data["dealer_name"] = ""
    try:
        temp_car_data["dealer_number"] = seller['primaryContactNumber']
    except:
        temp_car_data["dealer_number"] = ""
    try:
        temp_car_data["dealer_location"] = seller['location']['postcode'].replace(
            " ", "").strip().upper()
    except:
        temp_car_data["dealer_location"] = ""
    try:
        temp_car_data["dealer_id"] = seller['id']
    except:
        temp_car_data["dealer_id"] = ""
    ####################### DEALER ############################################

    try:
        temp_car_data['make'] = vehicle['make']
    except:
        temp_car_data['make'] = None

    try:
        temp_car_data['model'] = vehicle['model']
    except:
        temp_car_data['model'] = None

    try:
        temp_car_data['engine_cylinders'] = tracking['engine_size']
    except:
        temp_car_data['engine_cylinders'] = None

    try:
        temp_car_data['year'] = tracking['vehicle_year']
    except:
        temp_car_data['year'] = None

    try:
        temp_car_data['seats'] = tracking['number_of_seats']
    except:
        temp_car_data['seats'] = None

    try:
        temp_car_data['mileage'] = tracking['mileage']
    except:
        temp_car_data['mileage'] = None

    try:
        temp_car_data['fuel'] = tracking['fuel_type']
    except:
        temp_car_data['fuel'] = None

    try:
        temp_car_data['registration'] = vehicle['vrm']
    except:
        temp_car_data['registration'] = None

    # temp_car_data["registration"] = "NX64FFD"

    try:
        temp_car_data['writeOffCategory'] = vehicle['writeOffCategory']
    except:
        temp_car_data['writeOffCategory'] = None

    try:
        temp_car_data['doors'] = tracking['number_of_doors']
    except:
        temp_car_data['doors'] = None

    try:
        temp_car_data['body_style'] = tracking['body_type']
    except:
        temp_car_data['body_style'] = None

    try:
        temp_car_data['price'] = tracking['vehicle_price']
    except:
        temp_car_data['price'] = None

    try:
        temp_car_data["price_indicator"] = advert["priceIndicator"]["ratingLabel"]
    except:
        temp_car_data["price_indicator"] = None

    temp_car_data['admin_fees'] = admin_fees

    try:
        temp_car_data['trim'] = vehicle['trim']
    except:
        temp_car_data['trim'] = None

    try:
        temp_car_data['transmission'] = key_facts['transmission']
    except:
        temp_car_data['transmission'] = None

    temp_imgs = []

    try:
        temp_car_data['product_url'] = "https://www.autotrader.co.uk/car-details/{}".format(
            tracking['ad_id'].strip())
    except:
        temp_car_data['product_url'] = None

    try:
        for img_url in advert['imageUrls']:
            temp_imgs.append(img_url.replace("/{resize}", ""))
            # if len(temp_imgs) <= 10:
    except:
        pass

    temp_car_data['images'] = temp_imgs
    return temp_car_data


def get_all_listing_ids_for_dealer(dealer_id):
    temp_ids = []
    error_count = 0
    index = 0
    while True:
        index = index + 1
        if error_count > 100:
            break
        page_ids = get_listings_from_dealer_page(dealer_id, index)
        if len(page_ids) == 0:
            error_count = error_count + 1
        for id in page_ids:
            if not id in temp_ids:
                temp_ids.append(id)
            else:
                error_count = error_count + 1
    return temp_ids


def get_stock_count_from_dealer_id(dealer_id):
    import requests
    url = "https://www.autotrader.co.uk/json/dealers/stock?advertising-location=at_cars&advertising-location=at_profile_cars&dealer={}&onesearchad=Used&onesearchad=Nearly%20New&onesearchad=New&page=1&sort=price-asc".format(
        dealer_id)

    payload = {}
    headers = {
        'authority': 'www.autotrader.co.uk',
        'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
        'device_used': 'desktop',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'accept': '*/*',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://www.autotrader.co.uk/',
        'accept-language': 'en-US,en;q=0.9,de;q=0.8,hu;q=0.7,ku;q=0.6'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    json_data = response.json()
    stockResponse = json_data['stockResponse']
    totalResults = stockResponse['totalResults']
    return totalResults


def get_listings_from_dealer_page(dealer_id, page_no):
    import requests

    url = "https://www.autotrader.co.uk/json/dealers/stock?advertising-location=at_cars&advertising-location=at_profile_cars&dealer={}&onesearchad=Used&onesearchad=Nearly%20New&onesearchad=New&page={}&sort=price-asc".format(
        dealer_id, page_no)

    payload = {}
    headers = {
        'authority': 'www.autotrader.co.uk',
        'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
        'device_used': 'desktop',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36',
        'accept': '*/*',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://www.autotrader.co.uk/',
        'accept-language': 'en-US,en;q=0.9,de;q=0.8,hu;q=0.7,ku;q=0.6'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    data_json = response.json()
    temp_page_ids = get_ids_from_json(data_json)
    return temp_page_ids


def get_ids_from_json(json_data):
    invalid_category = ["S", "C", "D", "N"]
    ids = []
    try:
        stockResponse = json_data['stockResponse']
        for i in stockResponse['results']:
            try:
                cat = i['vehicle']['writeOffCategory']
                if not cat in invalid_category:
                    ids.append(i['id'])
            except Exception as e:
                print(str(e))
    except Exception as e:
        print("get_ids_from_json -> {}".format(str(e)))
    return ids


def extract_data_for_all_ids(all_dealer_ids):
    print("creating chrome instance")
    # wd = create_webdriver()
    final_data = []
    for id in all_dealer_ids:
        # car_j = get_car_json_from_id(id['car_id'])
        # print("car_j",car_j)
        try:
            car_d = graphql.fetch(id['car_id'])
            car_d['custom_price_value'] = id['custom_price_value']
            car_d['custom_price_enable'] = id['custom_price_enable']
            print("car_d", car_d)
            final_data.append({"db_id": id['db_id'], "data": car_d})
        except:
            pass
    print("destroying chrome instance")
    # wd.close()
    return final_data


def get_black_listed_registration():
    all_reg = []
    # obj_master.obj_db.connect()
    all_registration = obj_master.obj_db.recSelect("fl_blacklist")
    # obj_master.obj_db.disconnect()
    for reg in all_registration:
        if not reg['registration'].upper() in all_reg:
            all_reg.append(reg['registration'])
    return all_reg


def get_black_listed_dealers():
    all_blacklist_dealer_ids = []
    # obj_master.obj_db.connect()
    all_dealers = obj_master.obj_db.recSelect("fl_dealer_blacklist")
    # obj_master.obj_db.disconnect()
    for dealer in all_dealers:
        if not str(dealer['dealer_id']).upper() in all_blacklist_dealer_ids:
            all_blacklist_dealer_ids.append(str(dealer['dealer_id']))
    return all_blacklist_dealer_ids


def get_active_dealer_ids_from_database():
    all_scraper_dealer_ids = []
    # obj_master.obj_db.connect()
    all_dealers = obj_master.obj_db.recSelect(
        "fl_dealer_scraper", {"status": "active"})
    # obj_master.obj_db.disconnect()
    for dealer in all_dealers:
        if not str(dealer['dealer_id']).upper() in all_scraper_dealer_ids:
            all_scraper_dealer_ids.append(str(dealer['dealer_id']))
    return all_scraper_dealer_ids


def update_stock_value_in_db(dealer_id, stock_count):
    obj_master.obj_db.connect()
    obj_master.obj_db.recUpdate(
        "fl_dealer_scraper", {"stock_count": stock_count}, {"dealer_id": dealer_id})
    obj_master.obj_db.disconnect()


def get_dealer_admin_fee(dealer_id):
    import requests
    car_ids = []

    url = "https://www.autotrader.co.uk/json/dealers/stock?advertising-location=at_cars&advertising-location=at_profile_cars&dealer={}&onesearchad=Used&onesearchad=Nearly%20New&onesearchad=New&page=1&sort=price-asc".format(
        dealer_id)

    payload = {}
    headers = {
        'authority': 'www.autotrader.co.uk',
        'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
        'device_used': 'desktop',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'accept': '*/*',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://www.autotrader.co.uk/',
        'accept-language': 'en-US,en;q=0.9,de;q=0.8,hu;q=0.7,ku;q=0.6'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    json_data = response.json()

    stockResponse = json_data['stockResponse']
    results = stockResponse['results']
    for car in results:
        car_ids.append(car['id'])
    if len(car_ids) > 0:
        admin_fee = get_car_data_from_id(car_ids[0])
        return admin_fee
    else:
        return None


def get_car_data_from_id(car_id):
    import requests

    url = "https://www.autotrader.co.uk/json/fpa/initial/"+str(car_id)
#   proxies = { "http": "http://sp638d4858_2:mysecret007@gate.dc.smartproxy.com:20000",
#            "https": "http://sp638d4858_2:mysecret007@gate.dc.smartproxy.com:20000"
#     }
    residential_proxy = {
        "http": "http://sp638d4858_r_2:ac9edf76a737f@gate.smartproxy.com:7000",
        "https": "http://sp638d4858_r_2:ac9edf76a737f@gate.smartproxy.com:7000"
    }
    payload = {}
    headers = {
        'authority': 'www.autotrader.co.uk',
        'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
        'device_used': 'desktop',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'accept': '*/*',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'accept-language': 'en-US,en;q=0.9,de;q=0.8,hu;q=0.7,ku;q=0.6'
    }
    response = requests.request(
        "GET", url, headers=headers, data=payload, proxies=residential_proxy)

    json_data = response.json()
    advert = json_data['advert']
    noAdminFees = advert['noAdminFees']
    if noAdminFees == True:
        return None
    else:
        try:
            adminFee = extract_admin_fees(advert['adminFee'])
        except:
            print(advert)
            adminFee = 0
        return adminFee


def update_admin_fee_in_db():
    obj_master.obj_db.connect()
    all_dealers = obj_master.obj_db.recSelect(
        "fl_dealer_scraper", {"status": "active"})
    for dealer in all_dealers:
        admin_fee = get_dealer_admin_fee(dealer['dealer_id'])
        if admin_fee == None or admin_fee == 0:
            print("admin fee is null or zero. no need to update in db.")
        else:
            obj_master.obj_db.recUpdate("fl_dealer_scraper", {"admin_fee": int(admin_fee)}, {
                                        "dealer_id": dealer['dealer_id']})

    obj_master.obj_db.disconnect()


def get_admin_fee_of_all_dealers():
    temp = {}
    # obj_master.obj_db.connect()
    all_dealers = obj_master.obj_db.recSelect(
        "fl_dealer_scraper", {"status": "active"})
    # obj_master.obj_db.disconnect()
    for dealer in all_dealers:
        temp[str(dealer['dealer_id'])] = dealer['admin_fee']
    return temp


def extract_car_ids_from_urls(urls):
    all_car_ids_temp = []
    for url in urls:
        try:
            print(url)
            if "car-details" in url["url"]:
                temp = url['url'].split("car-details/")[1].split("?")[0]
            elif "van-details" in url["url"]:
                temp = url['url'].split("van-details/")[1].split("?")[0]
            elif "motorhome-details" in url:
                temp = url['url'].split("motorhome-details/")[1].split("?")[0]
            else:
                print("not able to extract id from url")
                exit(0)
            try:
                custom_price = float(url['custom_price_value'])
            except Exception as e:
                print(str(e))
                custom_price = 0
            all_car_ids_temp.append(
                {"db_id": url['id'], "car_id": temp, "custom_price_enable": url['custom_price_enable'], "custom_price_value": custom_price})
        except Exception as ee:
            print(str(ee))
    print(all_car_ids_temp)
    return all_car_ids_temp


def load_new_urls():
    # obj_master.obj_db.connect()
    pending_urls = obj_master.obj_db.recSelect(
        "AT_urls", {"status": "active", "scraped": 0})
    obj_master.obj_db.recUpdate("AT_urls", {"scraped": 2, "updated_at": {
                                'func': "now()"}}, {"status": "active", "scraped": 0})
    # obj_master.obj_db.disconnect()
    return pending_urls


def update_db_scraped_status(db_id, status_code):
    obj_master.obj_db.recUpdate("AT_urls", {"scraped": status_code, "updated_at": {
                                'func': "now()"}}, {"id": db_id})


def remove_que_data():
    key_redis_data = "{}_scraper_data_{}".format(SCRAPER_NAME, WEBSITE_ID)
    for i in range(1, 20):
        print(obj_master.obj_redis_cache.lPop(key_redis_data))


def save_scraped_file(scraped_data):
    file_name = str(uuid.uuid4())
    file_path = os.path.join(scraped_files_dir, file_name)
    with open(file_path, "wb") as f:
        pickle.dump(scraped_data, f)
    print("scraped file saved -> ", file_name)


def insert_into_fl_listings_logs(car_data_20, message):
    try:
        import json
        rows = obj_master.obj_db.recSelect(
            "fl_listings_logs", {"registration": car_data_20['registration']})
        if len(rows) > 0:
            temp = {}
            temp['json_data'] = json.dumps(car_data_20)
            temp['Website_ID'] = 17
            temp['make'] = car_data_20['make']
            temp['model'] = car_data_20['model']
            temp['price'] = car_data_20['price']
            temp['error_log_message'] = message
            temp['registration'] = car_data_20['registration']
            temp['product_url'] = car_data_20['product_url']
            temp['mileage'] = car_data_20['mileage']
            if "built" in car_data_20:
                temp['built'] = car_data_20['built']
            if "year" in car_data_20:
                temp['built'] = car_data_20['year']
            temp['updated_at'] = {'func': "now()"}
            obj_master.obj_db.recUpdate(
                "fl_listings_logs", temp, {"ID": rows[0]['ID']})
        else:
            temp = {}
            temp['json_data'] = json.dumps(car_data_20)
            temp['Website_ID'] = 17
            temp['make'] = car_data_20['make']
            temp['model'] = car_data_20['model']
            temp['price'] = car_data_20['price']
            temp['error_log_message'] = message
            temp['registration'] = car_data_20['registration']
            # temp["registration"] = "VNI4ZFA"
            temp['product_url'] = car_data_20['product_url']
            temp['mileage'] = car_data_20['mileage']
            if "built" in car_data_20:
                temp['built'] = car_data_20['built']
            if "year" in car_data_20:
                temp['built'] = car_data_20['year']
            temp['updated_at'] = {'func': "now()"}
            temp['create_ts'] = {'func': "now()"}
            obj_master.obj_db.recInsert("fl_listings_logs", temp)
    except Exception as e:
        print(str(e))


if __name__ == "__main__":
    invalid_category = ["S", "C", "D", "N"]
    # remove_que_data()
    obj_master.obj_db.connect()
    all_car_urls = load_new_urls()
    if len(all_car_urls) > 0:
        set_cron_status("running...")
        total_scraped = 0
        # update_admin_fee_in_db()
        all_dealers_admin_fee = get_admin_fee_of_all_dealers()
        # black_listed_registration = get_black_listed_registration()
        black_listed_registration = []
        print("blacklisted registration -> {}".format(str(black_listed_registration)))
        # black_listed_dealers = get_black_listed_dealers()
        black_listed_dealers = []
        print("black_listed_dealers -> {}".format(str(black_listed_dealers)))
        obj_master.obj_redis_cache.setKeyValue(
            CURRENT_SCRIPT_STATUS_KEY, "FALSE")
        key_redis_data = "{}_scraper_data_{}".format(SCRAPER_NAME, WEBSITE_ID)
        temp_all_dealer_ids = get_active_dealer_ids_from_database()
        all_dealer_ids = list(set(temp_all_dealer_ids))
        ############# working code ##########################################################
        all_car_ids = extract_car_ids_from_urls(all_car_urls)
        all_car_data = extract_data_for_all_ids(all_car_ids)
        for car_data_ in all_car_data:
            car_data = car_data_['data']
            if not car_data['writeOffCategory'] in invalid_category:
                del car_data['writeOffCategory']
                if car_data['make'] != None and car_data['model'] != None:
                    if not str(car_data['registration']).strip().upper() in black_listed_registration:
                        if not str(car_data['dealer_id']).upper() in black_listed_dealers:
                            if car_data['registration'] != None:
                                total_scraped = total_scraped + 1
                                temp_car_data = car_data.copy()
                                print("before", temp_car_data['admin_fees'])
                                if temp_car_data['admin_fees'] == 0:
                                    try:
                                        print("admin fee on website is zero")
                                        print(
                                            "Database admin fee -> ", all_dealers_admin_fee[str(car_data['dealer_id'])])
                                        temp_car_data['admin_fees'] = all_dealers_admin_fee[str(
                                            car_data['dealer_id'])]
                                    except:
                                        pass
                                print("after", temp_car_data['admin_fees'])
                                print(temp_car_data)
                                save_scraped_file(temp_car_data)
                                # obj_master.obj_redis_cache.rPush(key_redis_data,json.dumps(temp_car_data))
                                update_db_scraped_status(car_data_["db_id"], 1)
            else:
                try:
                    insert_into_fl_listings_logs(
                        car_data, f'writeOffCategory -> {car_data["writeOffCategory"]}')
                except Exception as e:
                    with open("car_data writeOffCategory.txt", "w") as f:
                        f.write(str(e))
        obj_master.obj_redis_cache.setKeyValue(
            CURRENT_SCRIPT_STATUS_KEY, "TRUE")
        update_cron_status("Total Listing Scraped : {}".format(
            total_scraped), "No Error")
        os.system(
            "cd /var/www/html/car_scrapers/auto_trader_url_scraper;/usr/bin/python3 auto_trader_instant_url_crawler_2.py")
    else:
        print("there are zero new urls.exiting...!")
    obj_master.obj_db.disconnect()
    ############# working code ##########################################################

    # all_car_urls = get_all_car_urls(search_url)
    # all_car_data = extract_car_data_from_url(all_car_urls)
    # for car_data in all_car_data:
    #     if car_data['make'] != None and car_data['model'] != None:
    #         if not str(car_data['registration']).strip().upper() in black_listed_registration:
    #             if not str(car_data['dealer_id']).upper() in black_listed_dealers:
    #                 print(car_data)
    #                 obj_master.obj_redis_cache.rPush(key_redis_data,json.dumps(car_data))
    # obj_master.obj_redis_cache.setKeyValue(CURRENT_SCRIPT_STATUS_KEY,"TRUE")
