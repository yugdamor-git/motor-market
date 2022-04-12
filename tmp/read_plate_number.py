#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import re
import json
import time
import warnings
import requests

#####################
processoutput = os.popen("ps -A -L -F").read()
cur_script = os.path.basename(__file__)
res = re.findall(cur_script,processoutput)

if len(res)>1:
        print ("EXITING BECAUSE ALREADY RUNNING.\n\n")
        exit(0)
#####################
sys.path.append("../modules")
from Master import Master
obj_master = Master()



def get_plate_number(img_path):        
    img_path = obj_master.obj_config.image_dir_base_path + img_path
    regions = ['gb', 'it'] # Change to your country
    with open(img_path, 'rb') as fp:
        response = requests.post(
            'https://api.platerecognizer.com/v1/plate-reader/',
            data=dict(regions=regions),  # Optional
            files=dict(upload=fp),
            headers={'Authorization': 'Token 3a61a44b8e39d27b22e690612093f362cfef1f63'})
        json_dict = json.loads(response.text)        
        if 'results' in json_dict and json_dict['results']:
            if 'plate' in json_dict['results'][0]:
                return json_dict['results'][0]['plate']
    return None


if __name__ == "__main__":    
    obj_master.obj_db.connect()
    total_call_limit = 0
    ap_res_content = obj_master.obj_req.getPage('GET','https://api.platerecognizer.com/v1/statistics/',{'headers':{"Authorization":"Token 3a61a44b8e39d27b22e690612093f362cfef1f63"}})
    dict_api_data = json.loads(ap_res_content)
    total_call_limit = dict_api_data['total_calls'] - dict_api_data['usage']['calls']
    print('Total call limit left:'+str(total_call_limit))
    #we process 1000 records at a time
    if total_call_limit>0:
        total_call_limit = 1000
    else:
        total_call_limit = 0
    count = 1
    photo_rows = obj_master.obj_db.recSelect('fl_listing_photos',
            {'number_plate_flag':1,'plate_updated_flag':0},limit=total_call_limit,order_by='Listing_ID',order_type='DESC')
    if not total_call_limit:
        photo_rows = []
    for photo_row in photo_rows:
        print('Processing record:'+str(count)+' of total:'+str(len(photo_rows)))
        #if image not exists the processed next.
        if not obj_master.obj_helper.isFileExists(obj_master.obj_config.image_dir_base_path+photo_row['Original']):
            continue
        #print(photo_row['Original'])
        number_plate =  get_plate_number(photo_row['Original'])        
        valid_img_flag = False
        if number_plate:
            print('The number plate is :'+str(number_plate))
            number_plate = number_plate.strip()            
            if len(number_plate) == 7:
                m = re.search(r'^[a-z A-Z]{2}\d\d[a-z A-Z]{3}$', str(number_plate), re.S | re.M | re.I)
                if m:
                    valid_img_flag = True        
        if valid_img_flag == True:
            print('valid...')            
            obj_master.obj_db.recUpdate('fl_listings',
                {'registration':number_plate},{'id':photo_row['Listing_ID']})
            obj_master.obj_db.recUpdate('fl_listing_photos',
                        {'plate_updated_flag':1},{'id':photo_row['ID']})
        else:
            print('invalid..')
            obj_master.obj_db.recUpdate('fl_listing_photos',
                {'plate_updated_flag':2},{'id':photo_row['ID']})
        time.sleep(2)
        count = count + 1
    obj_master.obj_db.disconnect()
