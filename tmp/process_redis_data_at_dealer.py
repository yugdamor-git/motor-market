
import os
import time
import traceback
from dateutil import parser

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
    print("The other instance of this script is already running....")
    print("Exiting...")
    exit(0)
else:
    print("No other instance is running...Executing this instance of script")
    # time.sleep(100)
####################################### checking if script already running or not #########################################




import sys
import json
sys.path.append("/var/www/html/car_scrapers/modules")
from Master import Master
import re

from pcp_apr_calc import pcp_apr_calc


IMAGE_DIR_PATH = "/var/www/html/files"

SCRAPER_NAME = "dealer_scraper"
WEBSITE_ID = 'S17'

REDIS_KEY = f'{WEBSITE_ID}_{SCRAPER_NAME}'

WEBSITE_IMAGE_DIR =  os.path.join(IMAGE_DIR_PATH,WEBSITE_ID)



class redis_processor():
    def __init__(self):
        master = Master()
        self.obj_master = master
        pa_calculator = pcp_apr_calc()
        self.pa_calc = pa_calculator
    
    def handle_category(self,temp_dict):
        self.obj_master.obj_db.connect()
        temp_mk_model = temp_dict['make'].lower()
        if temp_dict['model']:
            temp_mk_model = temp_mk_model + '_' + temp_dict['model'].lower()
        temp_mk_model = temp_mk_model.replace('-', '_')
        temp_mk_model = temp_mk_model.replace(' ', '_')
        category_temp = self.obj_master.obj_redis_cache.getKeyValue('category-'+temp_mk_model)
        if category_temp:
            temp_dict['Category_ID'] = category_temp
        else:
            ###############
            #Double check after cache..
            temp_cat_rows = self.obj_master.obj_db.recSelect('fl_categories',{'Key':temp_mk_model})
            #if record not for for this category..
            if len(temp_cat_rows)==0:
                temp_make_model_path = temp_dict['make']
                if 'model' in temp_dict and temp_dict['model']:
                    if temp_make_model_path:
                        temp_make_model_path = temp_make_model_path + '/' + temp_dict['model']
                    else:
                        temp_make_model_path = temp_dict['model']
                temp_make_model_path = temp_make_model_path.replace(' ', '-')
                make_temp = temp_dict['make']
                make_temp = make_temp.replace('-', '_')
                make_temp = make_temp.replace(' ', '_')
                #check the parent category exists in database
                category_rows = self.obj_master.obj_db.recSelect('fl_categories',{'Key':make_temp})
                if len(category_rows):
                    category_dict = dict()
                    category_dict['Path'] = temp_make_model_path
                    category_dict['Parent_ID'] = category_rows[0]['ID']
                    category_dict['Parent_IDs'] = category_rows[0]['ID']
                    category_dict['Parent_keys'] = category_rows[0]['Key']
                    category_dict['Type'] = category_rows[0]['Type']
                    category_dict['Key'] = temp_mk_model
                    category_dict['self_inserted'] = 1
                    new_category_id = self.obj_master.obj_db.recInsert('fl_categories',category_dict)
                    if new_category_id:
                        ###############
                        #push new key to category cache
                        self.obj_master.obj_redis_cache.setKeyValue('category-'+temp_mk_model,new_category_id)
                        ###############
                        new_category_dict = {}
                        new_category_dict['Tree'] = str(category_rows[0]['ID']) + '.' + str(new_category_id)
                        new_category_dict['Position'] = new_category_id
                        new_category_dict['Level'] = 1
                        new_category_dict['Modified'] = {'func':'now()'}
                        new_category_dict['neglect'] = 0
                        self.obj_master.obj_db.recUpdate('fl_categories',new_category_dict,{'ID':new_category_id})
                        #obj_master.obj_redis_cache.rPush(key_redis_data,json.dumps({"table_name": "fl_categories", "data": new_category_dict, "where": {'ID':new_category_id} }))
                        #######
                        lang_key = 'categories+name+'+temp_mk_model
                        temp_value = ''
                        if 'model' in temp_dict and temp_dict['model']:
                            temp_value = temp_dict['model']
                        else:
                            temp_value = temp_dict['make']
                        self.obj_master.obj_db.recInsert('fl_lang_keys',{'Code':'en','Module':'common','Key':lang_key,'Value':temp_value})
                        #obj_master.obj_redis_cache.rPush(key_redis_data,json.dumps({"table_name": "fl_lang_keys", "data": {'Code':'en','Module':'common','Key':lang_key,'Value':temp_value} }))
                        #######
                        temp_dict['Category_ID'] = new_category_id
                else:
                    temp_cat_dict = {'path':make_temp,
                                'level':0,'Parent_ID':0,'Parent_IDs':'',
                                'Parent_keys':'','Key':make_temp,
                                'Name':temp_dict['make'],
                                'status':'active',
                                'self_inserted':2
                    }
                    new_cate_id = self.obj_master.obj_db.recInsert('fl_categories',temp_cat_dict)
                    if new_cate_id:
                        self.obj_master.obj_redis_cache.setKeyValue('category-'+make_temp,new_cate_id)
                        self.obj_master.obj_db.recUpdate('fl_categories',{'neglect':0,'Position':new_cate_id,'Tree':new_cate_id},{'ID':new_cate_id})
                        #obj_master.obj_redis_cache.rPush(key_redis_data,json.dumps({"table_name": "fl_categories", "data": {'neglect':0,'Position':new_cate_id,'Tree':new_cate_id}, "where": {'ID':new_cate_id} }))
                        self.obj_master.obj_db.recInsert('fl_lang_keys',{'Code':'en','Module':'common','Key':'categories+name+'+make_temp,'Value':make_temp})
                        #obj_master.obj_redis_cache.rPush(key_redis_data,json.dumps({"table_name": "fl_lang_keys", "data": {'Code':'en','Module':'common','Key':'categories+name+'+make_temp,'Value':make_temp} }))
        self.obj_master.obj_db.disconnect()   
    
    def modify_result_data(self,temp_dict):
        if 'built' in temp_dict and temp_dict['built']:
            m = re.search(r'(\d\d\d\d)', str(temp_dict['built']),re.S | re.M | re.I)
            if m:
                temp_dict['built'] = m.group(1)

        if 'mileage' in temp_dict and temp_dict['mileage']:
            temp_dict['mileage'] = re.sub(r'[^\d\.]', '', str(temp_dict['mileage']), flags=re.S | re.M)
        #######################
        if 'transmission' in temp_dict and temp_dict['transmission']:
            temp_dict['transmission'] = str(temp_dict['transmission'])
            if temp_dict['transmission'].lower() == 'automatic':
                temp_dict['transmission'] = 1
            elif temp_dict['transmission'].lower() == 'manual':
                temp_dict['transmission'] = 2
            elif temp_dict['transmission'].lower() == 'automanual':
                temp_dict['transmission'] = 3
            else:
                temp_dict['transmission'] = 4
        if 'fuel' in temp_dict and temp_dict['fuel']:
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
        if 'doors' in temp_dict and temp_dict['doors']:
            #3 doors
            m = re.search(r'(\d+)\s+doors', str(temp_dict['doors']),re.S | re.M | re.I)
            if m:
                door_value = int(m.group(1))
                if door_value == 2:
                    temp_dict['doors'] = 1
                elif door_value == 3:
                    temp_dict['doors'] = 2
                elif door_value == 4:
                    temp_dict['doors'] = 3
                elif door_value == 5:
                    temp_dict['doors'] = 4
                elif door_value == 6:
                    temp_dict['doors'] = 5
                elif door_value == 10:
                    temp_dict['doors'] = 6
                else:
                    temp_dict['doors'] = 7
                    
    def parse_details(self,listing_json):
        result_dict = listing_json.copy()
        result_dict['make'] = listing_json['make']
        result_dict['model'] = listing_json['model']
        result_dict['images'] = listing_json['images']
        result_dict['title'] = listing_json['title']
        result_dict['body_style'] = listing_json['body_style']
        result_dict['price'] = listing_json['price']
        result_dict['exterior_color'] = ""
        result_dict['condition'] = ""
        result_dict['built'] = listing_json['built']
        result_dict['transmission'] = listing_json['transmission']
        result_dict['engine_cylinders'] = listing_json['engine_cylinders']
        result_dict['fuel'] =listing_json['fuel']
        result_dict['mileage'] = listing_json['mileage']
        result_dict['doors'] = listing_json['doors']
        plan_type = 'listing'
        try:
            result_dict["Main_photo"] = listing_json["Main_photo"]
        except:
            pass
        result_dict["Photos_count"] = listing_json["Photos_count"]
        plan_id = 26
        featured_id = 26
        result_dict['Plan_type'] = plan_type
        result_dict['Plan_ID'] = plan_id
        result_dict['Featured_ID'] = featured_id
        result_dict["approved_from_dashboard"] = 101
        try:
           del result_dict["year"]
        except:
            pass
        return result_dict
    
    def update_listing(self,table_name,data,where):
        print(where)
        print(data)
        print(table_name)
        data_temp = data.copy()
        try:
            del data_temp["ID"]
        except:
            pass
        try:
            del data_temp["images"]
        except:
            pass
        try:
            del data_temp["image_urls"]
        except:
            pass
        self.connect_db()
        self.obj_master.obj_db.recUpdate(table_name,data_temp,where)
        print(f'listing updated id -> {where["ID"]}')
        self.disconnect_db()
        
    def insert_listing(self,table_name,data):
        self.connect_db()
        for photo  in data:
            ID = self.obj_master.obj_db.recInsert(table_name,photo)
            print(f'new image inserted on id -> {ID}')
        self.disconnect_db()
        
    def process_listing(self,listing_id,listing_json_temp):
        listing_json = listing_json_temp.copy()
        result_dict = self.parse_details(listing_json)
        result_dict["status"] = listing_json_temp["status"]
        result_dict['why'] = "active beacause my all images are parsed by parser"
        self.handle_category(result_dict)
        result_dict['is_parsed'] = 1
        result_dict['Pay_date'] = {'func': "now()"}
        result_dict['error_count'] = 0
        result_dict['updated_at'] = {'func': "now()"}
        result_dict['img_process_flag'] = {'func': "NULL"}
        
        self.update_listing("fl_listings",result_dict,{"ID":listing_id})
    
    def process_images(self,data):
        self.insert_listing("fl_listing_photos",data)
    
    def connect_db(self):
        self.obj_master.obj_db.connect()
    
    def disconnect_db(self):
        self.obj_master.obj_db.disconnect()
    
    def process_expired_listing(self,id):
        self.connect_db()
        self.obj_master.obj_db.recUpdate("fl_listings",{"status":"expired"},{"ID":id})
        self.disconnect_db()

    def process_price_changed_listing(self,id,data):
        self.connect_db()
        self.pa_calc.insert_pcp_apr_data_into_db(id,data,self.obj_master)
        # self.obj_master.obj_db.recUpdate("fl_listings",new_var_pa,{"ID":id})
        self.disconnect_db()

    def calc_pa(self,id,new_price,mileage,built,cal_price_from_file=None):   
        self.connect_db()
        temp_car = {"price":new_price,"mileage":mileage,"built":built}
        if cal_price_from_file != None:
            temp_car["cal_price_from_file"] = cal_price_from_file
        self.pa_calc.insert_pcp_apr_data_into_db(id,temp_car,self.obj_master)
        self.disconnect_db()

    def main_processor(self,listing):
        print(listing)
        if listing["type"] == "insert_images":
            self.process_images(listing["data"])
        elif listing["type"] == "insert_listing":
            if "writeOffCategory" in listing["data"]:
                del listing["data"]["writeOffCategory"]
            self.process_listing(listing["ID"],listing["data"])
        elif listing["type"] == "expired_listing":
            self.process_expired_listing(listing["id"])
        elif listing["type"] == "price_increased":
            self.process_price_changed_listing(listing["id"],listing["data"])
        elif listing["type"] == "price_decreased":
            self.process_price_changed_listing(listing["id"],listing["data"])
        elif listing["type"] == "calc_pa":
            self.calc_pa(listing["id"],int(listing["price"]),int(listing["mileage"]),int(listing["built"]))
            
        elif listing["type"] == "update":
            data = listing["data"]
            if "price_indicator" in data:
                indicator = str(listing["data"]["price_indicator"]).lower().strip()
                if indicator in ["fair price","higher price"]:
                    data["status"] = "expired"
            for key in data:
                if "date" in key:
                    data[key] =  self.parse_date(data[key])
                    
            self.update_listing(listing["table_name"],data,{"ID":listing["id"]})
            
    def parse_date(self,value):
        try:
            return parser.parse(str(value).strip())
        except Exception as e:
            print(f'error : parse_date : {str(e)}')
            return None    
        
    def pop_redis_que(self):
        json_str = self.obj_master.obj_redis_cache.lPop(REDIS_KEY)
        json_listing = json.loads(json_str)
        return json_listing
    
    def push_redis_que(self,data):
        pass
    

if __name__ == "__main__":
    processor  = redis_processor()
    while True:
        try:
            redis_json = processor.pop_redis_que()
            processor.main_processor(redis_json)
            # time.sleep(0.4)
        except TypeError as e:
            interval = 5
            print(f'No Data Received... sleeping for {interval} seconds')
            time.sleep(interval)
        except Exception as e:
            traceback.print_exc()
        
        
        
        
        