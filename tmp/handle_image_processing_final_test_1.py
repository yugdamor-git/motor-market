#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import re
import json
import uuid
import time
import warnings
warnings.filterwarnings('ignore')
import requests
from fastai.vision import *
from fastai.metrics import error_rate
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



def check_image_status(listing_id):
    dict_arr_photo_id = {'arr_valid_ids':[],'arr_invalid_ids':[]}
    return_result = dict()
    fl_listing_photo_rows = obj_master.obj_db.recCustomQuery(
            "SELECT * FROM fl_listing_photos \
             WHERE delete_banner_flag IS NULL \
             AND listing_id='"+str(listing_id)+"'")
    for fl_listing_photo_row in fl_listing_photo_rows:
        photo_path = obj_master.obj_config.image_dir_base_path + fl_listing_photo_row['Photo']
        #defaulat image not need to check
        if fl_listing_photo_row['Photo'] =='images/motor-market.jpg':
            dict_arr_photo_id['arr_valid_ids'].append(fl_listing_photo_row['ID'])
        else:
            read_image = open_image(photo_path)
            learn = load_learner('model')
            pred_class,pred_idx,outputs = learn.predict(read_image)
            #collection ids valid and invalid cards seprately
            if str(pred_class) == 'cars':
                dict_arr_photo_id['arr_valid_ids'].append(fl_listing_photo_row['ID'])
            else:
                dict_arr_photo_id['arr_invalid_ids'].append(fl_listing_photo_row['ID'])
    #set all images as valid
    if len(dict_arr_photo_id['arr_valid_ids']):
        str_ids = ','.join(map(str,dict_arr_photo_id['arr_valid_ids']))
        qry_temp =  "UPDATE fl_listing_photos SET \
                        delete_banner_flag = 0 WHERE \
                        id IN("+str_ids+")"
        #print(qry_temp)
        obj_master.obj_db.recCustomQuery(qry_temp)
    #set all images as invalid
    if len(dict_arr_photo_id['arr_invalid_ids']):
        str_ids = ','.join(map(str,dict_arr_photo_id['arr_invalid_ids']))
        qry_temp =  "UPDATE fl_listing_photos SET \
                        delete_banner_flag = 1, status = 'approval' WHERE \
                        id IN("+str_ids+")"
        #print(qry_temp)
        obj_master.obj_db.recCustomQuery(qry_temp)

    ##############################
    #calculating the photo_count,main_image for fl_listing table
    where_dict = {'delete_banner_flag':0,'status':'active','listing_id':listing_id}
    temp_rows = obj_master.obj_db.recSelect('fl_listing_photos',where_dict)
    if len(temp_rows):
        return_result['Main_photo'] = temp_rows[0]['Thumbnail']
        return_result['photos_count'] = len(temp_rows)
    else:
        dict_temp = {
            'listing_id':listing_id,
            'Photo':'images/motor-market.jpg',
            'Thumbnail':'images/thumb_motor-market.jpg',
            'Original':'images/motor-market.jpg',
            'Position':1,
            'delete_banner_flag':0}
        print("inserting:"+str(dict_temp))
        obj_master.obj_db.recInsert('fl_listing_photos',dict_temp)
        return_result['Main_photo'] = dict_temp['Photo']
        return_result['photos_count'] = 1
    return return_result


if __name__ == "__main__":
    obj_master.obj_db.connect()
    fl_listing_rows = obj_master.obj_db.recSelect('fl_listings',
        {'listing_check_flag':0,'status':'pending',"Website_ID":17},1000)
    start_time = obj_master.obj_helper.currentTime(1)
    for i in range(0,len(fl_listing_rows)):
        print("Processing record:"+str(i)+" of total:"+str(len(fl_listing_rows)))
        fl_listing_row = fl_listing_rows[i]
        listing_id = fl_listing_row['ID']
        print("Processing :"+str(listing_id))
        try:
            final_result = check_image_status(listing_id)
        except Exception as e:
            print('Error:'+str(e))
            continue
        final_result['listing_check_flag'] = 1
        final_result['status'] = "approval"
        print(final_result)
        obj_master.obj_db.recUpdate('fl_listings',final_result,{'ID':listing_id})
    obj_master.obj_db.disconnect()
    print("Start:"+start_time)
    print("End:"+obj_master.obj_helper.currentTime(1))
