## this script will take data from scraper and it will perform neccesary operations...
from itsdangerous import exc
import pymongo

MARGIN = 1219
API_PERCENTAGE = 1.10

ACCOUNT_ID = 24898
WEBSITE_ID = 17
PRIORITY = 109
SCRAPER_NAME = "auto_trader_dealer"

CURRENT_SCRIPT_STATUS_KEY = "{}_{}_crawler".format(SCRAPER_NAME,WEBSITE_ID)
PREVIOUS_SCRIPT_STATUS_KEY = "{}_{}_scraper".format(SCRAPER_NAME,WEBSITE_ID)

DB_NAME = "motormarket"
USERNAME = "mm_scraper"
PWD = "mm_password"
PORT = 27017
SCRAPED_DATA_1 = "scraped_data_1"
PREPROCESSED_DATA_2 = "preprocessed_data_2"
PREPROCESSED_DATA_3 = "preprocessed_data_3"


REDIS_KEY = "S17_dealer_scraper"

# myclient = pymongo.MongoClient("mongodb://scraper_local:motor_market_local@127.0.0.1:27017/mydb")
myclient = pymongo.MongoClient(f'mongodb://{USERNAME}:{PWD}@127.0.0.1:{PORT}/{DB_NAME}')

db = myclient[DB_NAME]

import traceback
import sys
import os
import re
import json
import time
import logging
import os
import pickle
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
import unidecode
from bson.decimal128 import Decimal128
current_dir  = os.getcwd()
cur_script = os.path.basename(__file__)



def already_running(script_name):
    import psutil
    num_instance = 0
    for q in psutil.process_iter():
        if 'python' in q.name():
            if len(q.cmdline()) > 1 and script_name in q.cmdline()[1]:
                num_instance += 1
    return num_instance > 1

####################################### checking if script already running or not #########################################
cur_script = os.path.basename(__file__)
script_status = already_running(cur_script)
if script_status == True:
    logging.info("The other instance of this script is already running....")
    logging.info("Exiting...")
    exit(0)
else:
    logging.info("No other instance is running...Executing this instance of script")
    # time.sleep(100)
####################################### checking if script already running or not #########################################

sys.path.append("../modules")

from common_config import margin_cc_dict
from pcp_apr_calc import pcp_apr_calc
pa_calculator = pcp_apr_calc()

from PriceOperations import PriceOperations

po = PriceOperations()

from messenger import messenger

mm_bot = messenger()


from body_style_mapping import body_style_mapping
bsm = body_style_mapping()

from price_api import price_api






############### ML - SEATS #############################################
ALL_SEAT_DATA = bsm.load_make_model_seat()
VECTORIZER = bsm.get_vectorizer(ALL_SEAT_DATA)
ALL_TFIDF = bsm.get_tfidf(ALL_SEAT_DATA,VECTORIZER)
############### ML - SEATS #############################################

from Master import Master

obj_master = Master()

from price_api import price_api
price_ap = price_api(obj_master)


from cron_stat_manager import cron_stat_manager

csm = cron_stat_manager()

if obj_master.obj_config.debug:
    logging.basicConfig(level=logging.DEBUG, filename= obj_master.obj_config.logs_dir_path + cur_script + '.log', filemode='w', format='%(process)d - %(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
else:
    logging.basicConfig(filename= obj_master.obj_config.logs_dir_path + cur_script + '.log', filemode='a', format='%(process)d - %(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

def remove_accents(a):
  en = a.encode("utf-8")
  return unidecode.unidecode(en.decode('utf-8'))
def setup_firebase():
  import firebase_admin
  from firebase_admin import credentials
  from firebase_admin import firestore
  import os
  cur_dir = os.getcwd()
  firebase_dir = os.path.join(cur_dir,"firebase")

  cred = credentials.Certificate(os.path.join(firebase_dir,"firebase_motor_market.json"))
  firebase_admin.initialize_app(cred)
  firebase_db = firestore.client()
  return firebase_db
firebase_db = setup_firebase()

def get_price(reg_no,mileage):
    try:
        crp_price = price_ap.check_database_DealerForecourt_response(reg_no,mileage,WEBSITE_ID)
    except:
        traceback.print_exc()
        crp_price = 0
    price = round(float(crp_price))
    return price

def apply_price_condtions(price_from_api,price_on_site_):
  print("INPUT : api price -> {} and price on site -> {}".format(price_from_api,price_on_site_))
  temp_api = price_from_api * 1.10
  print("api * 1.10 -> {}".format(temp_api))
  print("price on AT site -> {}".format(price_on_site_))
  margin = temp_api - int(price_on_site_)
  print("margin -> {}".format(margin))
  max_margin = 1500
  if temp_api <= 8500:
    if margin >= 749:
      if margin > max_margin:
          return True,int(temp_api),int(margin),"",margin - 749
      else:
          return True,int(temp_api),int(margin),"",0
    else:
      return False,None,int(margin),"api_calc_price < 8500 and margin < 749.\nDealer Forcourt={}\nAT price -> {}\nDealer Forcourt * 1.10 -> {}\nmargin-> {}".format(price_from_api,price_on_site_,temp_api,margin),None

  elif temp_api >= 8501 and temp_api <= 10000:
    if margin >= 849:
      if margin > max_margin:
          return True,int(temp_api),int(margin),"",margin - 849
      else:
          return True,int(temp_api),int(margin),"",0
    else:
      return False,None,int(margin),"8501 < api_calc_price < 10000 and margin < 849.\nDealer Forcourt={}\nAT price -> {}\nDealer Forcourt * 1.10 -> {}\nmargin-> {}".format(price_from_api,price_on_site_,temp_api,margin),None

  elif temp_api >= 10001:
    if margin >= 900:
      if margin > max_margin:
          return True,int(temp_api),int(margin),"",margin - 900
      else:
          return True,int(temp_api),int(margin),"",0
    else:
      return False,None,int(margin),"10001 < api_calc_price and margin < 900\nDealer Forcourt={}\nAT price -> {}\nDealer Forcourt * 1.10 -> {}\nmargin-> {}".format(price_from_api,price_on_site_,temp_api,margin),None
  else:
    return False,None,int(margin),"none of api_price condtion is true",None


def remove_duplicate(h5_dict):
  all_reg = {}
  final_list =[]
  for item in h5_dict:
    all_reg[item['registration']] = item
  copy_dict = all_reg.copy()
  for item in copy_dict:
    final_list.append(all_reg[item])
  return final_list

def set_cron_status(msg):
  try:
      import datetime
      ct = datetime.datetime.now()

      global firebase_db
      website_id = str(WEBSITE_ID)
      script_no = "7"
      script_name = os.path.basename(__file__)
      doc_ref = firebase_db.collection("cron_management").document(website_id).collection("CRON").document(script_no)
      doc_ref.set({
          "name":script_name,
          "start":ct.timestamp(),
          "end":0,
          "message":"running...",
          "running":True,
          "error":""

      })
  except:
      pass

def update_cron_status(msg,error):
  try:
      global firebase_db
      import datetime
      ct = datetime.datetime.now()

      website_id = str(WEBSITE_ID)
      script_no = "7"
      script_name = os.path.basename(__file__)
      doc_ref = firebase_db.collection("cron_management").document(website_id).collection("CRON").document(script_no)
      doc_ref.update({
          "name":script_name,
          "end":ct.timestamp(),
          "message":msg,
          "running":False,
          "error":error

      })
  except:
      pass


def custom_tokenizer(title):
  import re
  title = str(title).replace("-"," ")
  stop_words = ['cdti','5dr','limited edition','limited' ,'edition','tfsi','thp','gdi','cvt','tip','td','aircross','dci','sline','tgi','premium','quattro','sport','coupe','amg','motor uk']
  title = title.lower().replace("-","").strip()
  re_exps = ["\(.*?\)","\d+\.\d+","\[.*?\]"]
  for exp in re_exps:
    bracket = re.findall(exp,title)
    if len(bracket) >= 1:
      for brac in bracket:
        title = title.replace(brac,"").strip()
  replace_punc = ['[',']','(',')','/','&','+','!']
  for punc in replace_punc:
    title = title.replace(punc," ")
  title_words = title.split(" ")

  for index,word in enumerate(title_words):
    if len(word) == 1:
      if index == 0:
        title_words[index] = word + title_words[index+1]
      else:
        title_words.append(title_words[index-1])
        title_words[index - 1] = title_words[index-1] + word
        if index != len(title_words) -1:
          title_words.pop(index)
  unique_words = []
  for word in title_words:
    if word not in unique_words:
      if len(word) > 1:
        unique_words.append(word.strip())
  # final_out = " ".join(unique_words).strip()
  for word in stop_words:
    if len(word) > 1:
      if word.strip() in unique_words:
        unique_words.pop(unique_words.index(word.strip()))
  last_out =[]
  for word in unique_words:
    if len(word) > 1:
      last_out.append(word)
  return last_out

def load_make_model():
  import os
  import pickle
  
  quick_fixes_labels = [
    "citroen;berlingo",
    "citroen;c1",
    "citroen;c3",
    "citroen;c4",
    "citroen;c5",
    "citroen;dispatch"
    ]

  quick_fixes_strings = [
    "citroen berlingo",
    "citroen c1",
    "citroen c3",
    "citroen c4",
    "citroen c5",
    "citroen dispatch"
    ]
  
  current_dir = os.getcwd()
  files_dir = os.path.join(current_dir,"make_model_predictor")
  vectorizer = None
  labels = None
  all_strings = None
  vectors = None
  with open(os.path.join(files_dir,"all_labels_v8.h5"),"rb") as ff:
    labels = pickle.load(ff)
  
  for l in quick_fixes_labels:
    labels.append(l)
  
  

  with open(os.path.join(files_dir,"all_strings_v8.h5"),"rb") as ff:
    all_strings = pickle.load(ff)

  for s in quick_fixes_strings:
    all_strings.append(s)
  
  vectorizer = TfidfVectorizer(analyzer='word',use_idf=False,tokenizer=custom_tokenizer)
  vectors = vectorizer.fit_transform(all_strings)

  return vectorizer,labels,vectors

vectorizer,labels,vectors = load_make_model()


def predict_car_model_and_make(title,vectorizer,labels,vectors):
  title_vector = vectorizer.transform([title])
  cosim = cosine_similarity(title_vector,vectors)
  if cosim.max() < 0.80:
    return 0,labels[cosim.argmax()],cosim.max()
  else:
    return 1,labels[cosim.argmax()],cosim.max()

def perform_transformation(car_data_4):
    temp = car_data_4.copy()

    try:
        temp['engine_cylinders'] = int(temp['engine_cylinders'])/1000
    except:
        temp['engine_cylinders'] = None
    try:
        temp['price'] = float(temp['price'])
    except:
        temp['price'] = None  
    
    
    try:
        temp['built'] = int(temp['year'])
    except:
        temp['built'] = None

    try:
        temp['seats'] = int(temp['seats'])
    except:
        temp['seats'] = None

    try:
        temp['mileage'] = int(temp['mileage'])
    except:
        temp['mileage'] = None
    try:
        temp['product_url'] = car_data_4['product_url']
    except:
        temp['product_url'] = ""
    try:
        temp['doors'] = int(temp['doors'])
    except:
        temp['doors'] = None

    try:
        temp['title'] = "{} {} {}".format(temp['make'],temp['model'],temp['trim'])
        temp['title'] = temp['title'].replace("None","")
    except:
        temp['title'] = None

    return temp

def apply_transmission_condtions(car_data_3):

    temp_dict = car_data_3.copy()

    temp_dict['transmission_org'] = temp_dict['transmission']
    if temp_dict['transmission'] != None:
        if temp_dict['transmission'].lower() == 'automatic':
          temp_dict['transmission'] = 1
        elif temp_dict['transmission'].lower() == 'manual':
          temp_dict['transmission'] = 2
        elif temp_dict['transmission'].lower() == 'automanual':
          temp_dict['transmission'] = 3
        elif temp_dict['transmission'].lower() == 'auto':
          temp_dict['transmission'] = 1
        elif temp_dict['transmission'].lower() == 'auto clutch':
          temp_dict['transmission'] = 1
        elif temp_dict['transmission'].lower() == 'auto/manual mode':
          temp_dict['transmission'] = 1
        elif temp_dict['transmission'].lower() == 'cvt':
          temp_dict['transmission'] = 1
        elif temp_dict['transmission'].lower() == 'cvt/manual mode':
          temp_dict['transmission'] = 1
        elif temp_dict['transmission'].lower() == 'manual transmission':
          temp_dict['transmission'] = 2
        elif temp_dict['transmission'].lower() == 'semi-automatic':
          temp_dict['transmission'] = 1
        elif temp_dict['transmission'].lower() == 'sequential':
          temp_dict['transmission'] = 1
        else:
          temp_dict['transmission'] = 4

    return temp_dict

def apply_fuel_conditions(car_data_2):
    temp_dict = car_data_2.copy()

    temp_dict['fuel'] = temp_dict['fuel'].lower()

    if temp_dict['fuel'] == 'petrol':
      temp_dict['fuel'] = 1
    elif temp_dict['fuel'] == 'diesel':
      temp_dict['fuel'] = 2
    elif temp_dict['fuel'] == 'gas':
      temp_dict['fuel'] = 3
    elif temp_dict['fuel'] == 'hybrid':
      temp_dict['fuel'] = 5
    elif temp_dict['fuel'] == 'electric':
      temp_dict['fuel'] = 6
    #Hybrid – Petrol/Electric Plug-in
    elif temp_dict['fuel'] == 'hybrid – petrol/elec' or 'hybrid –' in temp_dict['fuel']:
      temp_dict['fuel'] = 7
    else:
      temp_dict['fuel'] = 4
    return temp_dict

def apply_location_condition(car_data_1):
    import json
    temp = car_data_1.copy()
    temp["location"] = temp["dealer_location"]
    sale_location = None
    try:
        sale_location = temp['location']
        sale_location = sale_location.replace(" ","").strip().upper()
    except:
        sale_location = None
    temp['sale_location'] = sale_location
    temp_1 = {"dealer_name":temp['dealer_name'],
            "dealer_location":temp['location'],
            "dealer_id":temp['dealer_id'],
            "dealer_number":temp['dealer_number']}
    temp['location_json'] = json.dumps(temp_1)
    try:
        del temp['location']
    except:
        pass
    return temp

def check_if_registration_exists_in_db(account_id,registration_num):
    status = False
    row = None

    dict_where = {
        'account_id': account_id,
        'product_website_id': registration_num
    }
    fl_listings_rows = []
    fl_listings_rows_temp = db[PREPROCESSED_DATA_3].find({"product_website_id":registration_num})
    for rw in fl_listings_rows_temp:
        fl_listings_rows.append(rw)

    if len(fl_listings_rows) > 0:
        status = True
        row = fl_listings_rows[0]
    return {"status":status,"row":row}

def handle_previous_active_listings(all_car_data):
  print(f'total records after filter {len(all_car_data)}')
  
  temp = []
  
  all_cur_reg = obj_master.obj_db.recSelect("fl_listings")
  all_car_reg = []
  for car in all_cur_reg:
    all_car_reg.append(car["registration"])

  all_reg_num = []
  
  for index,car in enumerate(all_car_data):
    all_reg_num.append(car["registration"])
    if car["registration"] in all_car_reg:
      temp.insert(-1,car)
    else:
      temp.insert(0,car)
  
  sql_queries = []
  all_reg_num_str = ""
  query_index = 0
  for reg in all_reg_num:
    all_reg_num_str  = all_reg_num_str + "'" + reg.replace('"',"").replace("'","").strip() + "',"
    query_index += 1
    if query_index > 1000:
      query_index = 0
      query = "UPDATE `fl_listings` SET Status='active' WHERE `registration` IN("+ all_reg_num_str.strip(",")+") AND Status='expired' AND mannual_expire=0 AND images_removed=0"
      sql_queries.append(query)
      all_reg_num_str = ""
  if query_index < 1000:
    query = "UPDATE `fl_listings` SET Status='active' WHERE `registration` IN("+ all_reg_num_str.strip(",")+") AND Status='expired' AND mannual_expire=0 AND images_removed=0"
    sql_queries.append(query)
    
  # obj_master.obj_db.connect()
  return sql_queries,temp

def calc_pcp_yield(ID,car_data,obj_master):
    yield pa_calculator.insert_pcp_apr_data_into_db(ID,car_data,obj_master)

def push_redis(data):
  obj_master.obj_redis_cache.rPush(REDIS_KEY,json.dumps(data))

def process_upsert_data_into_db(all_car_data):
    all_pcp_calc_yields = []
    set_cron_status("STARTED")
    script_name = os.path.basename(__file__)
    obj_master.obj_db.connect()
    try:
      dealer_scraper_table,is_featured = load_dealer_scraper_auto_approve()
      is_featured_str = ""
      for is_f in is_featured:
        is_featured_str = is_featured_str + ',"'+ str(is_f) + '"'

      is_featured_query = f'UPDATE `fl_listings` SET Priority=109 WHERE dealer_id IN({is_featured_str.strip().strip(",")});'
    except Exception as e:
      mm_bot.send_message(str(e))
    
    csm.set_cron_status(WEBSITE_ID,2,script_name,obj_master)


    #### was active -> 0 then where status -> active -> was active == 1
    #UPDATE `fl_listings` SET was_active = 1 WHERE  Status="active"
    #UPDATE `fl_listings` SET was_active = 0 WHERE  1

    handle_previous_active_query,all_new_data = handle_previous_active_listings(all_car_data)
    # all_car_data = all_new_data

    obj_master.obj_db.recCustomQuery("UPDATE `fl_listings` SET was_active = 0 WHERE Website_ID= 17")
    obj_master.obj_db.recCustomQuery('UPDATE `fl_listings` SET was_active = 1 WHERE  Status="active"')


    # obj_master.obj_db.recCustomQuery('UPDATE `fl_listings` SET approved_from_dashboard = -1 WHERE Website_ID = 17 AND dealer_scraper_listing = 1 AND Status = "expired" AND Category_ID IS NOT NULL')
    obj_master.obj_db.recCustomQuery("UPDATE fl_listings SET Status='expired',why='expired because listing was not found in scraped data' where Website_ID="+ str(WEBSITE_ID)+ " AND Status='active' AND dealer_scraper_listing=1")
    obj_master.obj_db.recCustomQuery("UPDATE fl_listings SET is_parsed=0 where account_id="+str(ACCOUNT_ID)+" AND status='active' AND updated_at<DATE(NOW())")
    obj_master.obj_db.recCustomQuery("UPDATE fl_listings SET expired_flag=1 where Website_ID=17 AND dealer_scraper_listing=1")
    time.sleep(10)
    for sql_query in handle_previous_active_query:
      obj_master.obj_db.recCustomQuery(sql_query)
    time.sleep(10)
    sync_mysql_to_mongo()
    time.sleep(120)
    updated = 0
    inserted = 0
    for car_data in all_new_data:
        try:
          temp_single = car_data.copy()
          try:
              product_url = temp_single['product_url']
          except:
              product_url = ""
          dict_product = {'product_url':product_url}
          dict_product['product_website_id'] = car_data['registration']
          dict_product['account_id'] = ACCOUNT_ID
          dict_product['Loc_latitude'] = ""
          dict_product['Loc_longitude'] = ""
          dict_product['Loc_address'] = ""
          dict_product['b_country'] = ""
          dict_product['b_states'] = ""
          dict_product['b_zip'] = ""
          dict_product['postcode'] = ""
          dict_product['website_id'] = WEBSITE_ID
          dict_product['expired_flag'] = 0
          dict_product["Category_ID"] = 0
          dict_product["approved_from_dashboard"] = 0
          # dict_product["custom_price_enable"] =  0

          output = check_if_registration_exists_in_db(ACCOUNT_ID,temp_single['registration'])
          fl_listing_row = output['row']
          if output['status'] == False:
              inserted = inserted + 1
              dict_product['Status'] = 'to_parse'
              dict_product['why'] = "to_parse because i am new listing.recently scraped from website."
              dict_product['engine_cylinders'] = temp_single['engine_cylinders']
              dict_product['reference_number'] = temp_single['registration']
              # title = str(temp_single['title']).replace("-"," ")
              # ttt = " ".join(custom_tokenizer(title))
              # sig,lab,acc = predict_car_model_and_make(ttt,vectorizer,labels,vectors)

              # temp_lab = lab.lower().split(";")

              make_ = temp_single["make"].title()
              model_ = temp_single["model"].title()

              temp_single['make'] = remove_accents(make_)

              temp_single['title'] = temp_single['title']

              temp_single['model'] = model_

              dict_product['Priority'] = PRIORITY

              org_body_style = temp_single['body_style']

              pred_body_style = bsm.predict_bodystyle(org_body_style)

              temp_single['body_style'] = pred_body_style

              dict_product['body_style'] = pred_body_style

              dict_product['org_bodystyle'] = org_body_style

              dict_product['custom_price_enable'] = 0

              dict_product['cal_price_from_file'] = temp_single['price']

              temp_single['org_bodystyle'] = org_body_style

              dict_product['sale_location'] = temp_single['sale_location']

              dict_product['price'] = temp_single['price']
              
              
              
              try:
                dict_product["price_indicator"] = temp_single["price_indicator"]
              except:
                dict_product["price_indicator"] = None
              
              
              if not  str(dict_product["price_indicator"]).strip().lower() in ["fair price","higher price"]:
                
                if "predicted_seats" in temp_single.keys():
                    dict_product["predicted_seats"] = temp_single["predicted_seats"]

                if "body_style" in temp_single.keys():
                    dict_product["body_style"] = temp_single["body_style"]

                dict_product['location_json'] = temp_single['location_json']
                dict_product['listing_json'] = json.dumps(temp_single)
                dict_product['cal_price_from_api'] = temp_single['cal_price_from_api']
                dict_product['cal_price_from_file'] = temp_single['cal_price_from_file']
                dict_product['dealer_scraper_listing'] = 1
                dict_product['admin_fees'] =  temp_single['admin_fees']
                dict_product['Photos_count'] = len(temp_single['images'])
                dict_product['margin'] = temp_single['margin']
                dict_product['discount'] = temp_single['discount']
                dict_product["mannual_expire"] = 0
                dict_product["wheelbase"] = temp_single["wheelbase"]
                dict_product["cabtype"] = temp_single["cabtype"]
                dict_product["trim"] = temp_single["trim"]
                dict_product["vehicle_type"] = temp_single["vehicle_type"]
                dict_product["images_removed"] = 0
                dict_product["emission_scheme"] = temp_single["emission_scheme"]
                # dict_product["price"] = dict_product["price"] + 200
                # dict_product["temp_200"] = 1

                pa_price = int(temp_single["price"])
                
                
                pa_mileage = int(temp_single["mileage"])
                pa_built = int(temp_single["built"])
                temp_pa = {}
                
                # apply price operations
                # po.apply_difference_check_operation(dict_product,glass_price,price_ap)
                # glass_price = price_ap.check_database_DealerForecourt_response(temp_single["registration"],temp_single["mileage"],17)
               
                try:
                  at_price = po.to_int(temp_single["cal_price_from_file"] + temp_single["admin_fees"])
                  
                  if at_price != None:
                    if at_price < 10000:
                      glass_price = price_ap.check_database_DealerForecourt_response(temp_single["registration"],temp_single["mileage"],17)
                      po.apply_difference_check_operation(dict_product,glass_price)
                    else:
                      po.apply_default_values(dict_product)
                except Exception as e:
                  print(f'Error : apply_difference_check_operation : {str(e)} ')
                
                ID =obj_master.obj_db.recInsert('fl_listings', dict_product)
                temp_pa["id"] = ID
                temp_pa["price"] = pa_price
                temp_pa["mileage"] =  pa_mileage
                temp_pa["built"] = pa_built
                temp_pa["type"] = "calc_pa"
                push_redis(temp_pa)
                # all_pcp_calc_yields.append(calc_pcp_yield(ID,car_data,obj_master))
                print("ID of listing row : {}".format(str(ID)))
                dict_product["ID"] = ID
                db[PREPROCESSED_DATA_3].insert_one(dict_product)
          else:
              updated = updated + 1
              insert_dict_temp = temp_single.copy()
              insert_actual = {}
              make_ = temp_single["make"].title()
              model_ = temp_single["model"].title()
              insert_actual['engine_cylinders'] = insert_dict_temp['engine_cylinders']

              insert_actual['make'] = remove_accents(make_)
              insert_actual['model'] = model_
              insert_actual['mileage'] = insert_dict_temp['mileage']
              insert_actual['transmission'] = insert_dict_temp['transmission']
              insert_actual['fuel'] = insert_dict_temp['fuel']
              insert_actual['doors'] = insert_dict_temp['doors']
              insert_actual['body_style'] = insert_dict_temp['body_style']
              insert_actual['built'] = insert_dict_temp['built']
              insert_actual['Priority'] = PRIORITY
              insert_actual['sale_location'] = insert_dict_temp['sale_location']
              insert_actual['reference_number'] = insert_dict_temp['registration']
              
              insert_actual["wheelbase"] = temp_single["wheelbase"]
              insert_actual["cabtype"] = temp_single["cabtype"]
              insert_actual['cal_price_from_file'] = temp_single['cal_price_from_file']
              insert_actual['admin_fees'] =  temp_single['admin_fees']
              insert_actual["trim"] = temp_single["trim"]
              insert_actual["vehicle_type"] = temp_single["vehicle_type"]

              if fl_listing_row['custom_price_enable'] == 2:
                insert_actual['price'] = fl_listing_row['price']
              else:
                insert_actual['price'] = insert_dict_temp['price']
              
              try:
                insert_actual["price_indicator"] = insert_dict_temp["price_indicator"]
              except:
                insert_actual["price_indicator"] = None

              insert_actual['expired_flag'] = 0
              insert_actual['cal_price_from_api'] = insert_dict_temp['cal_price_from_api']
              insert_actual['cal_price_from_file'] = insert_dict_temp['cal_price_from_file']

              insert_actual['admin_fees'] = insert_dict_temp['admin_fees']

              org_body_style = insert_dict_temp['body_style']
              pred_body_style = bsm.predict_bodystyle(org_body_style)
              insert_actual['body_style'] = pred_body_style
              insert_actual['org_bodystyle'] = org_body_style

              insert_actual['status'] = 'active'
              insert_actual['is_parsed'] = 1
              if int(fl_listing_row['mannual_expire']) == 0 and fl_listing_row['approved_from_dashboard'] == 1:
                  insert_actual['status'] = 'active'
                  insert_actual['why'] = "active because approved from dashboard(was active and again found in scraped data.)"
              temp_category_id = fl_listing_row['Category_ID']
              temp_photo_count = fl_listing_row['Photos_count']
              if fl_listing_row['approved_from_dashboard'] == -1:
                  insert_actual['status'] = 'expired'
                  insert_actual['why'] = "expired because it was rejected from dashboard"
              if temp_category_id == 0 or temp_photo_count == 0:
                  insert_actual['Status'] = 'to_parse'
                  insert_actual['why'] = "because temp_category_id == 0 or temp_photo_count == 0"
              if fl_listing_row['Status'] == "image_parsed":
                  insert_actual['status'] = 'image_parsed'
                  insert_actual['why'] = "image_parsed because yesterday they did not approved me from dashboard"
              if fl_listing_row['Status'] == "to_prase":
                  insert_actual['status'] = 'to_parse'
                  insert_actual['why'] = "to_parse because maybe i am new listing."

              if "predicted_seats" in temp_single.keys():
                  insert_actual["predicted_seats"] = temp_single["predicted_seats"]

              if "body_style" in temp_single.keys():
                  insert_actual["body_style"] = temp_single["body_style"]

              insert_actual['Pay_date'] = {'func': "now()"}
              insert_actual['error_count'] = 0
              insert_actual['updated_at'] = {'func': "now()"}
              insert_actual['img_process_flag'] = {'func': "NULL"}
              insert_actual['dealer_scraper_listing'] = 1
              insert_actual['location_json'] = temp_single['location_json']
              insert_actual['listing_json'] = json.dumps(temp_single)
              insert_actual['Photos_count'] = len(temp_single['images'])
              insert_actual["emission_scheme"] = temp_single["emission_scheme"]

              if fl_listing_row['custom_price_enable'] == 2:
                insert_actual['margin'] = fl_listing_row['margin']
                insert_actual['discount'] = fl_listing_row['discount']
              else:
                insert_actual['margin'] = temp_single['margin']
                insert_actual['discount'] = temp_single['discount']

              # if fl_listing_row["temp_200"] == 0:
              #   insert_actual["price"] = insert_actual["price"] + 200
              #   insert_actual["temp_200"] = 1

              modified_data = insert_actual
              
              if fl_listing_row["images_removed"] == 1:
                modified_data["status"] = "to_parse"
                
              
              try:
                  at_price = po.to_int(temp_single["cal_price_from_file"] + temp_single["admin_fees"])
                  if at_price != None:
                    if at_price < 10000:
                      glass_price = price_ap.check_database_DealerForecourt_response(temp_single["registration"],temp_single["mileage"],17)
                      po.apply_difference_check_operation(modified_data,glass_price)
                    else:
                      po.apply_default_values(modified_data)
              except Exception as e:
                print(f'Error : apply_difference_check_operation : {str(e)} ')
                
              
              # try:

              #     if modified_data["price"] < 10000:
              #       glass_price = price_ap.check_database_DealerForecourt_response(temp_single["registration"],temp_single["mileage"],17)
              #       apply_difference_check_operation(modified_data,glass_price)
              #     else:
              #       apply_default_values(modified_data)
              # except Exception as e:
              #     print(f'Error : apply_difference_check_operation : {str(e)} ')
              
              obj_master.obj_db.recUpdate('fl_listings', modified_data,{'id': fl_listing_row['ID']})
              db[PREPROCESSED_DATA_3].update_one({"ID":fl_listing_row["ID"]},{"$set":modified_data})
              try:
                if int(fl_listing_row["price"]) == int(temp_single["price"]):
                  print("price is same as before no need to calc pcp")
                else:
                  print("price changed, need to calc pcp")
                  pa_price = int(temp_single["price"])
                  pa_mileage = int(temp_single["mileage"])
                  pa_built = int(temp_single["built"])
                  temp_pa["id"] = fl_listing_row["ID"]
                  temp_pa["price"] = pa_price
                  temp_pa["mileage"] =  pa_mileage
                  temp_pa["built"] = pa_built
                  temp_pa["type"] = "calc_pa"
                  push_redis(temp_pa)
              except:
                pa_calculator.insert_pcp_apr_data_into_db(fl_listing_row["ID"],car_data,obj_master)
              # all_pcp_calc_yields.append(calc_pcp_yield(ID,car_data,obj_master))
              # if fl_listing_row['custom_price_enable'] == 2:
              #   print("skipping pa calc. custom price enable == 2")
              # else:
              #   try:
              #     if fl_listing_row["price"] != insert_actual["price"]:
              #       pa_calculator.insert_pcp_apr_data_into_db(fl_listing_row['ID'],car_data,obj_master)
              #   except:
              #       pa_calculator.insert_pcp_apr_data_into_db(fl_listing_row['ID'],car_data,obj_master)

              print('Updating record id : {}'.format(fl_listing_row['ID']))
        except Exception as e:
          mm_bot.send_message(f'*AT Dealer Scraper*\nError {str(e)}:\n*{fl_listing_row}*')
          break
    messages = []
    messages.append("*AT Dealer Scraper*")
    messages.append(f'> updated : *{updated}*')
    messages.append(f'> inserted : *{inserted}*')
    try:
      obj_master.obj_db.recCustomQuery(is_featured_query)
    except Exception as e:
      messages.append(f'> there was some issue in is_featured query please update it : {str(e)}')
    mm_bot.send_message("\n".join(messages))
    update_cron_status("updated : {} inserted : {}".format(updated,inserted),"No Error")
    csm.update_cron_status(WEBSITE_ID,2,"updated : {} inserted : {}".format(updated,inserted),"No Error",obj_master)
    #obj_master.obj_db.recCustomQuery("UPDATE fl_listings SET Status='expired',why='expired due to expired flag=1' #where Website_ID=17 and expired_flag=1 and dealer_scraper_listing=1")

    # for pcp_yield in all_pcp_calc_yields:
    #     next(pcp_yield)

    obj_master.obj_db.disconnect()

car_types_dict = {'alfa romeo_stelvio quadrifoglio': {'make': 'alfa romeo',
  'model': 'stelvio quadrifoglio',
  'category': '4x4'},
 'alfa romeo_stelvio': {'make': 'alfa romeo',
  'model': 'stelvio',
  'category': '4x4'},
 'audi_q2': {'make': 'audi', 'model': 'q2', 'category': '4x4'},
 'audi_q3': {'make': 'audi', 'model': 'q3', 'category': '4x4'},
 'bmw_x1': {'make': 'bmw', 'model': 'x1', 'category': '4x4'},
 'bmw_x2': {'make': 'bmw', 'model': 'x2', 'category': '4x4'},
 'bmw_x3': {'make': 'bmw', 'model': 'x3', 'category': '4x4'},
 'cadillac_bls': {'make': 'cadillac', 'model': 'bls', 'category': '4x4'},
 'cadillac_escalade': {'make': 'cadillac',
  'model': 'escalade',
  'category': '4x4'},
 'chevrolet_captiva': {'make': 'chevrolet',
  'model': 'captiva',
  'category': '4x4'},
 'dodge_avenger': {'make': 'dodge', 'model': 'avenger', 'category': '4x4'},
 'dodge_caliber': {'make': 'dodge', 'model': 'caliber', 'category': '4x4'},
 'dodge_charger': {'make': 'dodge', 'model': 'charger', 'category': '4x4'},
 'dodge_nitro': {'make': 'dodge', 'model': 'nitro', 'category': '4x4'},
 'dodge_ram': {'make': 'dodge', 'model': 'ram', 'category': '4x4'},
 'ford_edge': {'make': 'ford', 'model': 'edge', 'category': '4x4'},
 'ford_ranger pickup': {'make': 'ford',
  'model': 'ranger pickup',
  'category': '4x4'},
 'jaguar_e-pace': {'make': 'jaguar', 'model': 'e-pace', 'category': 'luxury'},
 'jaguar_f-pace': {'make': 'jaguar', 'model': 'f-pace', 'category': 'luxury'},
 'jaguar_i-pace': {'make': 'jaguar', 'model': 'i-pace', 'category': 'luxury'},
 'jeep_cherokee': {'make': 'jeep', 'model': 'cherokee', 'category': '4x4'},
 'jeep_commander': {'make': 'jeep', 'model': 'commander', 'category': '4x4'},
 'jeep_compass': {'make': 'jeep', 'model': 'compass', 'category': '4x4'},
 'jeep_grand cherokee': {'make': 'jeep',
  'model': 'grand cherokee',
  'category': '4x4'},
 'jeep_patriot': {'make': 'jeep', 'model': 'patriot', 'category': '4x4'},
 'jeep_renegade': {'make': 'jeep', 'model': 'renegade', 'category': '4x4'},
 'jeep_wrangler': {'make': 'jeep', 'model': 'wrangler', 'category': '4x4'},
 'land rover_range rover evoque': {'make': 'land rover',
  'model': 'range rover evoque',
  'category': '4x4'},
 'mercedes_gla': {'make': 'mercedes', 'model': 'gla', 'category': '4x4'},
 'mercedes_glb': {'make': 'mercedes', 'model': 'glb', 'category': '4x4'},
 'mercedes_glc': {'make': 'mercedes', 'model': 'glc', 'category': '4x4'},
 'mitsubishi_l200 pickup': {'make': 'mitsubishi',
  'model': 'l200 pickup',
  'category': '4x4'},
 'nissan_navara pickup': {'make': 'nissan',
  'model': 'navara pickup',
  'category': '4x4'},
 'nissan_x-trail': {'make': 'nissan', 'model': 'x-trail', 'category': '4x4'},
 'volkswagen_amarok': {'make': 'volkswagen',
  'model': 'amarok',
  'category': '4x4'},
 'alfa romeo_4c': {'make': 'alfa romeo', 'model': '4c', 'category': 'luxury'},
 'alfa romeo_8c': {'make': 'alfa romeo', 'model': '8c', 'category': 'luxury'},
 'audi_q5': {'make': 'audi', 'model': 'q5', 'category': 'luxury'},
 'audi_q7': {'make': 'audi', 'model': 'q7', 'category': 'luxury'},
 'audi_q8': {'make': 'audi', 'model': 'q8', 'category': 'luxury'},
 'audi_rs q3': {'make': 'audi', 'model': 'rs q3', 'category': 'luxury'},
 'audi_rs3': {'make': 'audi', 'model': 'rs3', 'category': 'luxury'},
 'audi_rs4': {'make': 'audi', 'model': 'rs4', 'category': 'luxury'},
 'audi_rs5': {'make': 'audi', 'model': 'rs5', 'category': 'luxury'},
 'audi_rs6': {'make': 'audi', 'model': 'rs6', 'category': 'luxury'},
 'audi_rs7': {'make': 'audi', 'model': 'rs7', 'category': 'luxury'},
 'audi_s1 quattro': {'make': 'audi',
  'model': 's1 quattro',
  'category': 'luxury'},
 'audi_s3 quattro': {'make': 'audi',
  'model': 's3 quattro',
  'category': 'luxury'},
 'audi_s4 quattro': {'make': 'audi',
  'model': 's4 quattro',
  'category': 'luxury'},
 'audi_s4': {'make': 'audi', 'model': 's4', 'category': 'luxury'},
 'audi_s5 quattro': {'make': 'audi',
  'model': 's5 quattro',
  'category': 'luxury'},
 'audi_s5': {'make': 'audi', 'model': 's5', 'category': 'luxury'},
 'audi_s6 quattro': {'make': 'audi',
  'model': 's6 quattro',
  'category': 'luxury'},
 'audi_s6': {'make': 'audi', 'model': 's6', 'category': 'luxury'},
 'audi_s7 quattro': {'make': 'audi',
  'model': 's7 quattro',
  'category': 'luxury'},
 'audi_s8 quattro': {'make': 'audi',
  'model': 's8 quattro',
  'category': 'luxury'},
 'audi_sq': {'make': 'audi', 'model': 'sq', 'category': 'luxury'},
 'audi_sq5': {'make': 'audi', 'model': 'sq5', 'category': 'luxury'},
 'bmw_alpina': {'make': 'bmw', 'model': 'alpina', 'category': 'luxury'},
 'bmw_i3': {'make': 'bmw', 'model': 'i3', 'category': 'luxury'},
 'bmw_m1': {'make': 'bmw', 'model': 'm1', 'category': 'luxury'},
 'bmw_m135i': {'make': 'bmw', 'model': 'm135i', 'category': 'luxury'},
 'bmw_m140i': {'make': 'bmw', 'model': 'm140i', 'category': 'luxury'},
 'bmw_m2 competition': {'make': 'bmw',
  'model': 'm2 competition',
  'category': 'luxury'},
 'bmw_m2': {'make': 'bmw', 'model': 'm2', 'category': 'luxury'},
 'bmw_m235i': {'make': 'bmw', 'model': 'm235i', 'category': 'luxury'},
 'bmw_m3 competition': {'make': 'bmw',
  'model': 'm3 competition',
  'category': 'luxury'},
 'bmw_m3': {'make': 'bmw', 'model': 'm3', 'category': 'luxury'},
 'bmw_m4 competition': {'make': 'bmw',
  'model': 'm4 competition',
  'category': 'luxury'},
 'bmw_m4': {'make': 'bmw', 'model': 'm4', 'category': 'luxury'},
 'bmw_m5 competition': {'make': 'bmw',
  'model': 'm5 competition',
  'category': 'luxury'},
 'bmw_m5': {'make': 'bmw', 'model': 'm5', 'category': 'luxury'},
 'bmw_m6 competition': {'make': 'bmw',
  'model': 'm6 competition',
  'category': 'luxury'},
 'bmw_m6': {'make': 'bmw', 'model': 'm6', 'category': 'luxury'},
 'bmw_x4 m': {'make': 'bmw', 'model': 'x4 m', 'category': 'luxury'},
 'bmw_x4': {'make': 'bmw', 'model': 'x4', 'category': 'luxury'},
 'bmw_x5 m': {'make': 'bmw', 'model': 'x5 m', 'category': 'luxury'},
 'bmw_x5': {'make': 'bmw', 'model': 'x5', 'category': 'luxury'},
 'bmw_x6 m': {'make': 'bmw', 'model': 'x6 m', 'category': 'luxury'},
 'bmw_x6': {'make': 'bmw', 'model': 'x6', 'category': 'luxury'},
 'bmw_x7': {'make': 'bmw', 'model': 'x7', 'category': 'luxury'},
 'bmw_z4': {'make': 'bmw', 'model': 'z4', 'category': 'luxury'},
 'caterham_7 supersport': {'make': 'caterham',
  'model': '7 supersport',
  'category': 'luxury'},
 'caterham_7.0': {'make': 'caterham', 'model': '7.0', 'category': 'luxury'},
 'chevrolet_corvette': {'make': 'chevrolet',
  'model': 'corvette',
  'category': 'luxury'},
 'dodge_challenger': {'make': 'dodge',
  'model': 'challenger',
  'category': 'luxury'},
 'ford_mustang': {'make': 'ford', 'model': 'mustang', 'category': 'luxury'},
 'ford_rs': {'make': 'ford', 'model': 'rs', 'category': 'luxury'},
 'ford_focus st': {'make': 'ford', 'model': 'focus st', 'category': 'luxury'},
 'hummer_h2': {'make': 'hummer', 'model': 'h2', 'category': 'luxury'},
 'hummer_h3': {'make': 'hummer', 'model': 'h3', 'category': 'luxury'},
 'infiniti_q30': {'make': 'infiniti', 'model': 'q30', 'category': 'luxury'},
 'infiniti_q50': {'make': 'infiniti', 'model': 'q50', 'category': 'luxury'},
 'infiniti_q60': {'make': 'infiniti', 'model': 'q60', 'category': 'luxury'},
 'infiniti_q70': {'make': 'infiniti', 'model': 'q70', 'category': 'luxury'},
 'infiniti_qx50': {'make': 'infiniti', 'model': 'qx50', 'category': 'luxury'},
 'infiniti_qx55': {'make': 'infiniti', 'model': 'qx55', 'category': 'luxury'},
 'infiniti_qx60': {'make': 'infiniti', 'model': 'qx60', 'category': 'luxury'},
 'infiniti_qx70': {'make': 'infiniti', 'model': 'qx70', 'category': 'luxury'},
 'jaguar_s-type': {'make': 'jaguar', 'model': 's-type', 'category': 'luxury'},
 'jaguar_x-type': {'make': 'jaguar', 'model': 'x-type', 'category': 'luxury'},
 'jaguar_xe': {'make': 'jaguar', 'model': 'xe', 'category': 'luxury'},
 'jaguar_xf': {'make': 'jaguar', 'model': 'xf', 'category': 'luxury'},
 'jaguar_xfr': {'make': 'jaguar', 'model': 'xfr', 'category': 'luxury'},
 'jaguar_xj': {'make': 'jaguar', 'model': 'xj', 'category': 'luxury'},
 'jaguar_xjl': {'make': 'jaguar', 'model': 'xjl', 'category': 'luxury'},
 'jaguar_xjr': {'make': 'jaguar', 'model': 'xjr', 'category': 'luxury'},
 'jaguar_xk': {'make': 'jaguar', 'model': 'xk', 'category': 'luxury'},
 'jaguar_xk8': {'make': 'jaguar', 'model': 'xk8', 'category': 'luxury'},
 'jaguar_xkr': {'make': 'jaguar', 'model': 'xkr', 'category': 'luxury'},
 'land rover_autobiography': {'make': 'land rover',
  'model': 'autobiography',
  'category': 'luxury'},
 'land rover_defender 110': {'make': 'land rover',
  'model': 'defender 110',
  'category': 'luxury'},
 'land rover_defender 130': {'make': 'land rover',
  'model': 'defender 130',
  'category': 'luxury'},
 'land rover_defender 90': {'make': 'land rover',
  'model': 'defender 90',
  'category': 'luxury'},
 'land rover_discovery 3': {'make': 'land rover',
  'model': 'discovery 3',
  'category': 'luxury'},
 'land rover_discovery 4': {'make': 'land rover',
  'model': 'discovery 4',
  'category': 'luxury'},
 'land rover_discovery sport': {'make': 'land rover',
  'model': 'discovery sport',
  'category': 'luxury'},
 'land rover_freelander 1': {'make': 'land rover',
  'model': 'freelander 1',
  'category': 'luxury'},
 'land rover_freelander 2': {'make': 'land rover',
  'model': 'freelander 2',
  'category': 'luxury'},
 'land rover_range rover sport': {'make': 'land rover',
  'model': 'range rover sport',
  'category': 'luxury'},
 'land rover_range rover velar': {'make': 'land rover',
  'model': 'range rover velar',
  'category': 'luxury'},
 'land rover_range rover vogue': {'make': 'land rover',
  'model': 'range rover vogue',
  'category': 'luxury'},
 'land rover_range rover': {'make': 'land rover',
  'model': 'range rover',
  'category': 'luxury'},
 'lexus_ls': {'make': 'lexus', 'model': 'ls', 'category': 'luxury'},
 'lotus_elise': {'make': 'lotus', 'model': 'elise', 'category': 'luxury'},
 'lotus_evora': {'make': 'lotus', 'model': 'evora', 'category': 'luxury'},
 'lotus_exige': {'make': 'lotus', 'model': 'exige', 'category': 'luxury'},
 'maserati_ghibli': {'make': 'maserati',
  'model': 'ghibli',
  'category': 'luxury'},
 'maserati_levante': {'make': 'maserati',
  'model': 'levante',
  'category': 'luxury'},
 'maserati_quattroporte': {'make': 'maserati',
  'model': 'quattroporte',
  'category': 'luxury'},
 'mazda_rx-8': {'make': 'mazda', 'model': 'rx-8', 'category': 'luxury'},
 'mercedes_amg gt s': {'make': 'mercedes',
  'model': 'amg gt s',
  'category': 'luxury'},
 'mercedes_amg gt': {'make': 'mercedes',
  'model': 'amg gt',
  'category': 'luxury'},
 'mercedes_c43 amg': {'make': 'mercedes',
  'model': 'c43 amg',
  'category': 'luxury'},
 'mercedes_c63 amg': {'make': 'mercedes',
  'model': 'c63 amg',
  'category': 'luxury'},
 'mercedes_cl63 amg': {'make': 'mercedes',
  'model': 'cl63 amg',
  'category': 'luxury'},
 'mercedes_cl65 amg': {'make': 'mercedes',
  'model': 'cl65 amg',
  'category': 'luxury'},
 'mercedes_cla35 amg': {'make': 'mercedes',
  'model': 'cla35 amg',
  'category': 'luxury'},
 'mercedes_cla45 amg': {'make': 'mercedes',
  'model': 'cla45 amg',
  'category': 'luxury'},
 'mercedes_cls 63 amg s': {'make': 'mercedes',
  'model': 'cls 63 amg s',
  'category': 'luxury'},
 'mercedes_cls53 amg': {'make': 'mercedes',
  'model': 'cls53 amg',
  'category': 'luxury'},
 'mercedes_cls63 amg s': {'make': 'mercedes',
  'model': 'cls63 amg s',
  'category': 'luxury'},
 'mercedes_cls63 amg': {'make': 'mercedes',
  'model': 'cls63 amg',
  'category': 'luxury'},
 'mercedes_e43 amg': {'make': 'mercedes',
  'model': 'e43 amg',
  'category': 'luxury'},
 'mercedes_e53 amg': {'make': 'mercedes',
  'model': 'e53 amg',
  'category': 'luxury'},
 'mercedes_e63 amg': {'make': 'mercedes',
  'model': 'e63 amg',
  'category': 'luxury'},
 'mercedes_g': {'make': 'mercedes', 'model': 'g', 'category': 'luxury'},
 'mercedes_g350 bluetec': {'make': 'mercedes',
  'model': 'g350 bluetec',
  'category': 'luxury'},
 'mercedes_g55 amg': {'make': 'mercedes',
  'model': 'g55 amg',
  'category': 'luxury'},
 'mercedes_g63 amg': {'make': 'mercedes',
  'model': 'g63 amg',
  'category': 'luxury'},
 'mercedes_gl': {'make': 'mercedes', 'model': 'gl', 'category': 'luxury'},
 'mercedes_gl63 amg': {'make': 'mercedes',
  'model': 'gl63 amg',
  'category': 'luxury'},
 'mercedes_gla45 amg': {'make': 'mercedes',
  'model': 'gla45 amg',
  'category': 'luxury'},
 'mercedes_glc43 amg': {'make': 'mercedes',
  'model': 'glc43 amg',
  'category': 'luxury'},
 'mercedes_glc63 amg': {'make': 'mercedes',
  'model': 'glc63 amg',
  'category': 'luxury'},
 'mercedes_gle': {'make': 'mercedes', 'model': 'gle', 'category': 'luxury'},
 'mercedes_gle43 amg': {'make': 'mercedes',
  'model': 'gle43 amg',
  'category': 'luxury'},
 'mercedes_gle63 amg': {'make': 'mercedes',
  'model': 'gle63 amg',
  'category': 'luxury'},
 'mercedes_gls': {'make': 'mercedes', 'model': 'gls', 'category': 'luxury'},
 'mercedes_gls63 amg': {'make': 'mercedes',
  'model': 'gls63 amg',
  'category': 'luxury'},
 'mercedes_ml': {'make': 'mercedes', 'model': 'ml', 'category': 'luxury'},
 'mercedes_ml63 amg': {'make': 'mercedes',
  'model': 'ml63 amg',
  'category': 'luxury'},
 'mercedes_s63 amg': {'make': 'mercedes',
  'model': 's63 amg',
  'category': 'luxury'},
 'mercedes_s65 amg': {'make': 'mercedes',
  'model': 's65 amg',
  'category': 'luxury'},
 'mercedes_sl55 amg': {'make': 'mercedes',
  'model': 'sl55 amg',
  'category': 'luxury'},
 'mercedes_sl63 amg': {'make': 'mercedes',
  'model': 'sl63 amg',
  'category': 'luxury'},
 'mercedes_sl65 amg': {'make': 'mercedes',
  'model': 'sl65 amg',
  'category': 'luxury'},
 'mercedes_slc43 amg': {'make': 'mercedes',
  'model': 'slc43 amg',
  'category': 'luxury'},
 'mercedes_slk55 amg': {'make': 'mercedes',
  'model': 'slk55 amg',
  'category': 'luxury'},
 'mercedes_sls amg': {'make': 'mercedes',
  'model': 'sls amg',
  'category': 'luxury'},
 'mitsubishi_lancer evo': {'make': 'mitsubishi',
  'model': 'lancer evo',
  'category': 'luxury'},
 'mitsubishi_lancer ralliart': {'make': 'mitsubishi',
  'model': 'lancer ralliart',
  'category': 'luxury'},
 'mitsubishi_lancer': {'make': 'mitsubishi',
  'model': 'lancer',
  'category': 'luxury'},
 'morgan_aero': {'make': 'morgan', 'model': 'aero', 'category': 'luxury'},
 'morgan_plus 8': {'make': 'morgan', 'model': 'plus 8', 'category': 'luxury'},
 'nissan_skyline': {'make': 'nissan',
  'model': 'skyline',
  'category': 'luxury'},
 'porsche_cayenne': {'make': 'porsche',
  'model': 'cayenne',
  'category': 'luxury'},
 'porsche_cayman': {'make': 'porsche',
  'model': 'cayman',
  'category': 'luxury'},
 'porsche_macan': {'make': 'porsche', 'model': 'macan', 'category': 'luxury'},
 'subaru_impreza turbo': {'make': 'subaru',
  'model': 'impreza turbo',
  'category': 'luxury'},
 'subaru_impreza wrx': {'make': 'subaru',
  'model': 'impreza wrx',
  'category': 'luxury'},
 'subaru_impreza': {'make': 'subaru',
  'model': 'impreza',
  'category': 'luxury'}}

# margin_cc_dict = {

#     "4x4": {
#         1:{"margin":994,"min":1000,"max":2001},
#         2:{"margin":1045,"min":2001,"max":2501},
#         3:{"margin":1119,"min":2501,"max":3001}
#     },
#     "luxury":{
#         1:{"margin":1046,"min":1000,"max":2001},
#         2:{"margin":1124,"min":2001,"max":2501},
#         3:{"margin":1228,"min":2501,"max":3001}
#     },
#     "others":{
#         1:{"margin":949,"min":1000,"max":2001},
#         2:{"margin":1004,"min":2001,"max":2501},
#         3:{"margin":1064,"min":2501,"max":3001}
#     }
# }

def get_margin_value(make,model,cc,price):
    make_4x4 = ["cadillac","jeep"]
    
    make_luxury = ["caterham","hummer","infiniti","jaguar","morgan","lotus"]
    
    cc = cc * 1000
    
    make = str(make).strip()
    
    model = str(model).strip()
    
    cc = int(cc)
    
    car_key = f'{make};{model}'

    car_cat = None
    if make in make_4x4:
      car_cat = {"category":"4x4"}
    elif make in make_luxury:
      car_cat = {"category":"luxury"}
    else:
      try:
          car_cat = car_types_dict[car_key]
      except:
          car_cat = {"category":"others"}
    final_price = 0
    margin = 0
    margin_cc = margin_cc_dict[car_cat["category"]]
    
    for item in margin_cc.items():
      data_item = item[1]
      if cc >= data_item["min"] and cc < data_item["max"]:
          margin = data_item["margin"]
          final_price = price + margin
          break
    if margin == 0:
        status = False
        message = "we are not taking this car due to cc > 3000"
    else:
        status = True
        message = "we are taking this car.all conditions are true."

    return {
        "make":make,
        "model":model,
        "cc":cc,
        "car_category":car_cat["category"],
        "input_price":price,
        "new_price":final_price,
        "margin":margin,
        "status":status,
        "message":message
    }


def apply_price_condition(car_data_9):
    global vectorizer
    global labels
    global vectors
    #apply any price related conditions over here...
    temp = car_data_9.copy()
    
    print(temp["price"])
    
    if temp['price'] <= 25000:
        admin_fees = temp['admin_fees']
        scraper_price_ = temp['price']
        sig,lab,acc = predict_car_model_and_make(temp["title"],vectorizer,labels,vectors)
        print(lab)
        make = lab.split(";")[0]
        model = str(lab).split(";")[1]
        temp["make"] = make
        temp["model"] = model

        ######################### ML - SEATS PREDICTION ##################################

        seats,cosim = bsm.predict_seats(ALL_TFIDF,VECTORIZER,make,model)

        if seats != None:
            temp['predicted_seats'] = seats

        ######################### ML - SEATS PREDICTION ##################################

        #################suv############################
        suv_pred = bsm.predict_bodystyle_suv(make,model)
        
        if suv_pred != None:
            temp['body_style'] = suv_pred
        #################suv############################


        if temp["engine_cylinders"] == None or temp["engine_cylinders"] == 0:
          return temp,False,"engine_cylinders not found"

        else:
          margin_res = get_margin_value(make,model,temp["engine_cylinders"],scraper_price_)
          if margin_res["status"] == True:

            temp['margin'] = int(margin_res["margin"])
            temp["temp_200"] = 1
            temp['cal_price_from_api'] = 0
            temp['cal_price_from_file'] = temp['price']
            temp['price'] = margin_res["new_price"]
            
            if temp['cal_price_from_file'] <= 9000 and temp['cal_price_from_file'] >= 7000:
              temp["price"] = temp["price"] - 150
              temp["margin"] = temp["margin"] -150
            elif temp["cal_price_from_file"] < 7000:
              temp["price"] = temp["price"] - 250
              temp["margin"] = temp["margin"] - 250
              
            temp['discount'] = 0
            print("this listing will be added.")
            return temp,True,margin_res["message"]
          else:
            return temp,False,"due to margin logic update..."
    else:
        return temp,False,"price > 25000"

def apply_built_condtion(temp_data_4):
    if temp_data_4['built'] != None:
        if temp_data_4['built'] >= 2012:
            return temp_data_4,True,""
        else:
            return temp_data_4,False,"built < 2012"
    else:
        return temp_data_4,False,"built data is not available!"

def apply_mileage_condtion(temp_data_5):
    if temp_data_5['mileage'] != None:
        if temp_data_5['mileage'] < 120000:
            return temp_data_5,True,""
        else:
            return temp_data_5,False,"mileage > 120000"
    else:
        return temp_data_5,False,"mileage is not available!"

def get_digits(reg_no):
  digits = []
  for char in str(reg_no).strip():
    try:
      int(char)
      digits.append(char)
    except:
      pass
  return "".join(digits)

def extract_built_from_registration(reg_no):
  reg_digits = get_digits(reg_no)
  digits_built_pairs = {
    "64":2014,
    "14":2014,
    "65":2015,
    "15":2015,
    "66":2016,
    "16":2016,
    "67":2017,
    "17":2017,
    "68":2018,
    "18":2018,
    "69":2019,
    "19":2019,
    "70":2020,
    "20":2020,
    "71":2021,
    "21":2021
  }
  try:
    built =  digits_built_pairs[reg_digits]
    return built
  except:
    return None
def built_extraction_from_reg(car_data_6):
    temp_data_6 = car_data_6.copy()
    built = extract_built_from_registration(car_data_6['registration'])
    if temp_data_6["year"] == None:
        temp_data_6['year'] = built
    return temp_data_6

def insert_into_fl_listings_logs(car_data_20,message):
    try:
        import json
        rows = obj_master.obj_db.recSelect("fl_listings_logs",{"registration":car_data_20['registration']})
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
            obj_master.obj_db.recUpdate("fl_listings_logs",temp,{"ID":rows[0]['ID']})
        else:
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
            temp['create_ts'] = {'func': "now()"}
            obj_master.obj_db.recInsert("fl_listings_logs",temp)
    except Exception as e:
        print(str(e))


def insert_database_call_and_api_call(price_api,obj_master):
    database_count = price_api.database_call_count
    api_count = price_api.api_call_count
    temp_count_data = {}
    temp_count_data['website_id'] = WEBSITE_ID
    temp_count_data['database_call_count'] = database_count
    temp_count_data['api_call_count'] = api_count
    obj_master.obj_db.recInsert("fl_price_api_usage",temp_count_data)

def get_value_from_redis():
  error_count = 0
  redis_data = {}
  while True:
    try:
      json_str = obj_master.obj_redis_cache.lPop("{}_dealer_scraper_data_{}".format(SCRAPER_NAME,WEBSITE_ID))
      car_data_6 = json.loads(json_str)
      redis_data[car_data_6['registration']] = car_data_6
    except:
      error_count = error_count + 1
    if error_count  > 20:
      break
  final_out_redis = []
  for item in redis_data.items():
    final_out_redis.append(item[1])
  return final_out_redis

def get_parsed_data():
  cur_dir = os.getcwd()
  scrapy_old_data_path = os.path.join(cur_dir,"scrapy_old_data")
  scrapy_parsed_dir = os.path.join(cur_dir,"parsed")
  parsed_file_path = os.path.join(scrapy_parsed_dir,"parsed_data.h5")
  all_parsed_data = []
  try:
    with open(parsed_file_path,"rb") as f:
      all_parsed_data = pickle.load(f)
    os.system(f'mv {parsed_file_path} {scrapy_old_data_path}')
  except:
    pass
  return all_parsed_data

def load_dealer_scraper_auto_approve():
    temp_data = {}
    is_featured = []
    all_rows = obj_master.obj_db.recSelect("fl_dealer_scraper",{"status":"active"})
    for temp_row in all_rows:
        temp_data[str(temp_row["dealer_id"])] = temp_row
        if temp_row["is_featured"] == 1:
          is_featured.append(str(temp_row["dealer_id"]))
    return temp_data,is_featured

def sync_mysql_to_mongo():
    decimal_cols = ['QCF_Oodle_AB',
        'QCF_Oodle_C',
        'QCF_Billing',
        'GCC',
        'AM_TierIn',
        'AM_TierEx',
        'QCF_Adv_E',
        'QCF_Adv_D',
        'QCF_Adv_C',
        'QCF_Adv_AB',
        'QCF_SMF',
        'QCF_MB_NT',
        'QCF_MB_T',
        'BMF']
    
    all_data = obj_master.obj_db.recSelect("fl_listings")
    for listing in all_data:
      for item in listing:
        if item in decimal_cols:
          if listing[item] != None:
            listing[item] = Decimal128(listing[item])
          
    db[PREPROCESSED_DATA_3].insert_many(all_data)
    # obj_master.obj_db.disconnect()
    # for listing in all_data:
    #     temp = listing.copy()
    #     temp["_id"] = listing["registration"]
    #     try:
    #         db[PREPROCESSED_DATA_3].insert_one(temp)
    #     except:
    #         del temp["_id"]
    #         db[PREPROCESSED_DATA_3].update_one({"_id":listing["registration"]},{"$set":temp})

def insert_preprocessed_data():
    data = None
    with open("scrapy_old_data/parsed_data.h5","rb") as f:
        data = pickle.load(f)

    for item in data:
        temp = item.copy()
        temp["_id"] = item["registration"]
        try:
            db[PREPROCESSED_DATA_2].insert_one(item)
        except:
            del temp["_id"]
            db[PREPROCESSED_DATA_2].update_one({"_id":item["registration"]},{"$set":temp})

if __name__ == "__main__":
    try:
      cur_dir = os.getcwd()
      parsed_file_path  = os.path.join(cur_dir,"parsed/parsed_data.h5")

      # insert_preprocessed_data()
      transformed_data = []
      index = 0
      error_count = 0
      obj_master.obj_db.connect()
      # sync_mysql_to_mongo()
      all_sc = []
      data_ = None
      if os.path.exists(parsed_file_path):
        with open(parsed_file_path,"rb") as f:
            data_ = pickle.load(f)
        try:
          db[PREPROCESSED_DATA_3].drop()
        except:
          pass
        time.sleep(10)
      else:
        print("no parsed file found")
        data_ = []
      for car_data_6 in data_:
        index = index + 1
        print(index)
        try:
          temp_data_0 = built_extraction_from_reg(car_data_6)
          temp_data = perform_transformation(temp_data_0)
          temp_data_1 = apply_transmission_condtions(temp_data)
          temp_data_2 = apply_fuel_conditions(temp_data_1)
          if temp_data_2['price'] == None:
              # insert_into_fl_listings_logs(temp_data_2,"price is not available for this listing")
              continue
          if temp_data_2['make'] == None and temp_data_2['model'] == None:
              # insert_into_fl_listings_logs(temp_data_2,"make and model is not available for this listing")
              continue
          if len(temp_data_2['images']) == 0:
              # insert_into_fl_listings_logs(temp_data_2,"image is not available for this listing")
              continue
          temp_data_3 = apply_location_condition(temp_data_2)
          temp_data_5,status_5,message_5 = apply_mileage_condtion(temp_data_3)
          if status_5 == False:
              # insert_into_fl_listings_logs(temp_data_5,message_5)
              continue
          temp_data_6,status_6,message_6 = apply_built_condtion(temp_data_5)
          if status_6 == False:
              # insert_into_fl_listings_logs(temp_data_6,message_6)
              continue
          temp_data_4,status_4,message_4 = apply_price_condition(temp_data_6)
          if status_4 == False:
              print(message_4)
              continue
              # insert_into_fl_listings_logs(temp_data_4,message_4)
              
          print("passed")
          transformed_data.append(temp_data_4)
        except Exception as e:
          error_count = error_count + 1
          mm_bot.send_message(f'*AT Dealer Scraper*\nThere is some issue : *{str(traceback.format_exc())}*')
          break
        
      # update the api call and database call count
      # obj_master.obj_db.recCustomQuery('DELETE FROM fl_rejected_listings where updated < DATE_SUB(DATE(now()), INTERVAL 3 DAY)')
      
      obj_master.obj_db.disconnect()
      if len(transformed_data) > 0:
          process_upsert_data_into_db(transformed_data)
          target_parsed_path = os.path.join(cur_dir,"scrapy_old_data/parsed_data.h5")
          os.system(f'mv {parsed_file_path} {target_parsed_path}')
      
      # obj_master.obj_redis_cache.setKeyValue(PREVIOUS_SCRIPT_STATUS_KEY,"FALSE")
      # obj_master.obj_redis_cache.setKeyValue(CURRENT_SCRIPT_STATUS_KEY,"TRUE")
    except Exception as e:
      mm_bot.send_message(f'*AT Dealer Scraper*\nThere is some issue : *{str(e)}*')
