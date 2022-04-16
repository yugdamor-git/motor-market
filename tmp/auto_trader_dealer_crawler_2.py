## this script will take data from scraper and it will perform neccesary operations...

MARGIN = 1219
API_PERCENTAGE = 1.10

ACCOUNT_ID = 24898
WEBSITE_ID = 17
PRIORITY = 109
SCRAPER_NAME = "auto_trader_dealer"

CURRENT_SCRIPT_STATUS_KEY = "{}_{}_crawler".format(SCRAPER_NAME,WEBSITE_ID)
PREVIOUS_SCRIPT_STATUS_KEY = "{}_{}_scraper".format(SCRAPER_NAME,WEBSITE_ID)

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
from pcp_apr_calc import pcp_apr_calc
pa_calculator = pcp_apr_calc()


from body_style_mapping import body_style_mapping
bsm = body_style_mapping()


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
  current_dir = os.getcwd()
  files_dir = os.path.join(current_dir,"make_model_predictor")
  vectorizer = None
  labels = None
  all_strings = None
  vectors = None
  with open(os.path.join(files_dir,"all_labels_v8.h5"),"rb") as ff:
    labels = pickle.load(ff)

  with open(os.path.join(files_dir,"all_strings_v8.h5"),"rb") as ff:
    all_strings = pickle.load(ff)

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
        temp['engine_cylinders'] = float(temp['engine_cylinders'])
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
    del temp['trim']

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
    sale_location = None
    try:
        sale_location = car_data_1['location']['postcode']
        sale_location = sale_location.replace(" ","").strip().upper()
    except:
        sale_location = None
    temp['sale_location'] = sale_location
    temp_1 = {"dealer_name":car_data_1['dealer_name'],
            "dealer_location":car_data_1['location'],
            "dealer_id":car_data_1['dealer_id'],
            "dealer_number":car_data_1['dealer_number']}
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

    fl_listings_rows = obj_master.obj_db.recSelect('fl_listings',dict_where)

    if len(fl_listings_rows) > 0:
        status = True
        row = fl_listings_rows[0]

    return {"status":status,"row":row}

def process_upsert_data_into_db(all_car_data):
    set_cron_status("STARTED")
    script_name = os.path.basename(__file__)
    obj_master.obj_db.connect()
    csm.set_cron_status(WEBSITE_ID,2,script_name,obj_master)
    # obj_master.obj_db.recCustomQuery('UPDATE `fl_listings` SET approved_from_dashboard = -1 WHERE Website_ID = 17 AND dealer_scraper_listing = 1 AND Status = "expired" AND Category_ID IS NOT NULL')
    obj_master.obj_db.recCustomQuery("UPDATE fl_listings SET Status='expired',why='expired because listing was not found in scraped data' where Website_ID="+ str(WEBSITE_ID)+ " AND Status='active' AND dealer_scraper_listing=1")
    obj_master.obj_db.recCustomQuery("UPDATE fl_listings SET is_parsed=0 where account_id="+str(ACCOUNT_ID)+" AND status='active' AND updated_at<DATE(NOW())")
    obj_master.obj_db.recCustomQuery("UPDATE fl_listings SET expired_flag=1 where Website_ID=17 AND dealer_scraper_listing=1")
    updated = 0
    inserted = 0
    for car_data in all_car_data:
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

        output = check_if_registration_exists_in_db(ACCOUNT_ID,temp_single['registration'])
        fl_listing_row = output['row']
        if output['status'] == False:
            inserted = inserted + 1
            dict_product['Status'] = 'to_parse'
            dict_product['why'] = "to_parse because i am new listing.recently scraped from website."
            dict_product['engine_cylinders'] = temp_single['engine_cylinders']
            dict_product['reference_number'] = temp_single['registration']
            title = temp_single['title']
            ttt = " ".join(custom_tokenizer(title))

            sig,lab,acc = predict_car_model_and_make(ttt,vectorizer,labels,vectors)

            temp_lab = lab.lower().split(";")

            make_ = temp_lab[0].strip().title()

            model_ = temp_lab[1].strip().title()

            temp_single['make'] = remove_accents(make_)

            temp_single['title'] = temp_single['title']

            temp_single['model'] = model_

            dict_product['Priority'] = PRIORITY

            org_body_style = temp_single['body_style']

            pred_body_style = bsm.predict_bodystyle(org_body_style)

            temp_single['body_style'] = pred_body_style

            dict_product['body_style'] = pred_body_style

            dict_product['org_bodystyle'] = org_body_style

            dict_product['cal_price_from_file'] = temp_single['price']

            temp_single['org_bodystyle'] = org_body_style

            dict_product['sale_location'] = temp_single['sale_location']

            dict_product['price'] = temp_single['price']
            #################suv############################
            suv_pred = bsm.predict_bodystyle_suv(temp_single['make'],temp_single['make'])
            if suv_pred != None:
                temp_single['body_style'] = suv_pred
                dict_product['body_style'] = suv_pred
            #################suv############################

            ######################### ML - SEATS PREDICTION ##################################

            seats,cosim = bsm.predict_seats(ALL_TFIDF,VECTORIZER,make_,model_)
            if seats != None:
                dict_product['predicted_seats'] = seats

            ######################### ML - SEATS PREDICTION ##################################

            dict_product['location_json'] = temp_single['location_json']
            dict_product['listing_json'] = json.dumps(temp_single)
            dict_product['cal_price_from_api'] = temp_single['cal_price_from_api']
            dict_product['cal_price_from_file'] = temp_single['cal_price_from_file']
            dict_product['dealer_scraper_listing'] = 1
            dict_product['admin_fees'] =  temp_single['admin_fees']
            dict_product['Photos_count'] = len(temp_single['images'])
            dict_product['margin'] = temp_single['margin']
            dict_product['discount'] = temp_single['discount']
            ID =obj_master.obj_db.recInsert('fl_listings', dict_product)
            pa_calculator.insert_pcp_apr_data_into_db(ID,car_data,obj_master)
            print("ID of listing row : {}".format(str(ID)))
        else:
            updated = updated + 1
            insert_dict_temp = temp_single.copy()
            insert_actual = {}
            title = temp_single['title'].lower()
            ttt = " ".join(custom_tokenizer(title))
            sig,lab,acc = predict_car_model_and_make(ttt,vectorizer,labels,vectors)
            temp_lab = lab.lower().split(";")
            make_ = temp_lab[0].strip().title()
            model_ = temp_lab[1].strip().title()
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
            
            if fl_listing_row['custom_price_enable'] == 2:
              insert_actual['price'] = fl_listing_row['price']
            else:  
              insert_actual['price'] = insert_dict_temp['price']
            
            insert_actual['expired_flag'] = 0
            insert_actual['cal_price_from_api'] = insert_dict_temp['cal_price_from_api']
            insert_actual['cal_price_from_file'] = insert_dict_temp['cal_price_from_file']

            insert_actual['admin_fees'] = insert_dict_temp['admin_fees']

            org_body_style = insert_dict_temp['body_style']
            pred_body_style = bsm.predict_bodystyle(org_body_style)
            insert_actual['body_style'] = pred_body_style
            insert_actual['org_bodystyle'] = org_body_style

            ######################### ML - SEATS PREDICTION ##################################
            seats,cosim = bsm.predict_seats(ALL_TFIDF,VECTORIZER,make_,model_)
            if seats != None:
                insert_actual['predicted_seats'] = seats
            ######################### ML - SEATS PREDICTION ##################################

            #################suv############################
            suv_pred = bsm.predict_bodystyle_suv(insert_dict_temp['make'],insert_dict_temp['make'])
            if suv_pred != None:
                insert_actual['body_style'] = suv_pred
            #################suv############################

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
                insert_actual['Status'] = 'expired'
                insert_actual['why'] = "expired because temp_category_id == 0 or temp_photo_count == 0"
            if fl_listing_row['Status'] == "image_parsed":
                insert_actual['status'] = 'image_parsed'
                insert_actual['why'] = "image_parsed because yesterday they did not approved me from dashboard"
            if fl_listing_row['Status'] == "to_prase":
                insert_actual['status'] = 'to_parse'
                insert_actual['why'] = "to_parse because maybe i am new listing."
            insert_actual['Pay_date'] = {'func': "now()"}
            insert_actual['error_count'] = 0
            insert_actual['updated_at'] = {'func': "now()"}
            insert_actual['img_process_flag'] = {'func': "NULL"}
            insert_actual['dealer_scraper_listing'] = 1
            insert_actual['location_json'] = temp_single['location_json']
            insert_actual['listing_json'] = json.dumps(temp_single)
            insert_actual['Photos_count'] = len(temp_single['images'])
            
            if fl_listing_row['custom_price_enable'] == 2:
              insert_actual['margin'] = fl_listing_row['margin']
              insert_actual['discount'] = fl_listing_row['discount']
            else:
              insert_actual['margin'] = temp_single['margin']
              insert_actual['discount'] = temp_single['discount']
              
            modified_data = insert_actual
            obj_master.obj_db.recUpdate('fl_listings', modified_data,{'id': fl_listing_row['ID']})
            
            if fl_listing_row['custom_price_enable'] == 2:
              print("skipping pa calc. custom price enable == 2")
            else:
              pa_calculator.insert_pcp_apr_data_into_db(fl_listing_row['ID'],car_data,obj_master)
            
            print('Updating record id : {}'.format(fl_listing_row['ID']))
    update_cron_status("updated : {} inserted : {}".format(updated,inserted),"No Error")
    csm.update_cron_status(WEBSITE_ID,2,"updated : {} inserted : {}".format(updated,inserted),"No Error",obj_master)
    obj_master.obj_db.recCustomQuery("UPDATE fl_listings SET Status='expired',why='expired due to expired flag=1' where Website_ID=17 and expired_flag=1 and dealer_scraper_listing=1")
    obj_master.obj_db.disconnect()

def apply_price_condition(car_data_9):
    #apply any price related conditions over here...
    temp = car_data_9.copy()
    if temp['price'] <= 15000:
        admin_fees = temp['admin_fees']
        scraper_price_ = temp['price'] + admin_fees
        print("scraper price -> {}".format(scraper_price_))
        api_price = get_price(temp['registration'],temp['mileage'])
        status,new_price,margin,message,discount = apply_price_condtions(api_price,scraper_price_)
        temp['margin'] = int(margin)
        temp['cal_price_from_api'] = int(api_price * API_PERCENTAGE)
        temp['cal_price_from_file'] = temp['price']
        temp['price'] = new_price
        temp['discount'] = discount
        return temp,status,message
    else:
        return temp,False,"price > 15000"

def apply_built_condtion(temp_data_4):
    if temp_data_4['built'] != None:
        if temp_data_4['built'] >= 2014:
            return temp_data_4,True,""
        else:
            return temp_data_4,False,"built < 2014"
    else:
        return temp_data_4,False,"built data is not available!"

def apply_mileage_condtion(temp_data_5):
    if temp_data_5['mileage'] != None:
        if temp_data_5['mileage'] < 90000:
            return temp_data_5,True,""
        else:
            return temp_data_5,False,"mileage > 90000"
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
    if built != None:
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
      json_str = obj_master.obj_redis_cache.lPop("{}_scraper_data_{}".format(SCRAPER_NAME,WEBSITE_ID))
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

if __name__ == "__main__":
    redis_data = get_value_from_redis()
    # STATUS_VALUE = "TRUE"
    if len(redis_data) > 0:
        transformed_data = []
        index = 0
        error_count = 0
        obj_master.obj_db.connect()
        for car_data_6 in redis_data:
            if error_count >20:
                break
            index = index + 1
            try:
                temp_data_0 = built_extraction_from_reg(car_data_6)
                temp_data = perform_transformation(temp_data_0)
                temp_data_1 = apply_transmission_condtions(temp_data)
                temp_data_2 = apply_fuel_conditions(temp_data_1)
                if temp_data_2['price'] == None:
                    insert_into_fl_listings_logs(temp_data_2,"price is not available for this listing")
                    continue
                if temp_data_2['make'] == None and temp_data_2['model'] == None:
                    insert_into_fl_listings_logs(temp_data_2,"make and model is not available for this listing")
                    continue
                if len(temp_data_2['images']) == 0:
                    insert_into_fl_listings_logs(temp_data_2,"image is not available for this listing")
                    continue
                temp_data_3 = apply_location_condition(temp_data_2)
                temp_data_5,status_5,message_5 = apply_mileage_condtion(temp_data_3)
                if status_5 == False:
                    insert_into_fl_listings_logs(temp_data_5,message_5)
                    continue
                temp_data_6,status_6,message_6 = apply_built_condtion(temp_data_5)
                if status_6 == False:
                    insert_into_fl_listings_logs(temp_data_6,message_6)
                    continue
                temp_data_4,status_4,message_4 = apply_price_condition(temp_data_6)
                if status_4 == False:
                    insert_into_fl_listings_logs(temp_data_4,message_4)
                    continue
                transformed_data.append(temp_data_4)
            except Exception as e:
                 traceback.print_exc()
                 error_count = error_count + 1
        # update the api call and database call count
        try:
            insert_database_call_and_api_call(price_ap,obj_master)
        except:
            pass
        # obj_master.obj_db.recCustomQuery('DELETE FROM fl_rejected_listings where updated < DATE_SUB(DATE(now()), INTERVAL 3 DAY)')
        obj_master.obj_db.disconnect()
        if len(transformed_data) > 0:
            process_upsert_data_into_db(transformed_data)
        obj_master.obj_redis_cache.setKeyValue(PREVIOUS_SCRIPT_STATUS_KEY,"FALSE")
        obj_master.obj_redis_cache.setKeyValue(CURRENT_SCRIPT_STATUS_KEY,"TRUE")
    else:
        print("there is not any data from scraper...exiting.")
