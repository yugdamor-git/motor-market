#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import re
import json
import uuid
import PIL.Image
import numpy as np
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
sys.path.append("modules")
from Master import Master
obj_master = Master()



def check_image_status(listing_id):
    return_result = dict()
    img_list=[]
    plate_number_dict = {'plate_number':[],'not plate_number': []}
    fl_listing_photo_rows = obj_master.obj_db.recCustomQuery(
            "SELECT * FROM fl_listing_photos \
             WHERE delete_banner_flag = 0 \
                AND listing_id='"+str(listing_id)+"'")
    update_car_query =  "UPDATE fl_listings SET \
                         listing_check_number = 1 WHERE \
                         id IN("+str(listing_id)+")"
    obj_master.obj_db.recCustomQuery(update_car_query)

    for fl_listing_photo_row in fl_listing_photo_rows:
        photo_path = obj_master.obj_config.image_dir_base_path + fl_listing_photo_row['Photo']
        #Make predictions
        read_image = open_image(photo_path)
        learn = load_learner('model','plate_or_not_8.pkl')
        pred_class,pred_idx,outputs = learn.predict(read_image)
        #collection ids valid and invalid cards seprately
        if str(pred_class) == "plate_number":
            plate_number_dict['plate_number'].append({str(fl_listing_photo_row['ID']):obj_master.obj_config.image_dir_base_path + fl_listing_photo_row['Photo']})
            print(plate_number_dict)
        else:
            plate_number_dict['not plate_number'].append({str( fl_listing_photo_row['ID']):obj_master.obj_config.image_dir_base_path + fl_listing_photo_row['Photo']})
    #set all images as valid 
    for image in plate_number_dict['plate_number']:
        image = list(image.values())[0]
        print(image)
        im = PIL.Image.open(image).convert('L') # to grayscale
        array = np.asarray(im, dtype=np.int32)

        gy, gx = np.gradient(array)
        gnorm = np.sqrt(gx**2 + gy**2)
        sharpness = np.average(gnorm)
        img_list.append(sharpness)
    sharper_img = max(img_list)
    sharper_img_index = img_list.index(sharper_img)
    result = list(plate_number_dict['plate_number'][sharper_img_index].keys())[0]
    print(result)
    qry_temp =  "UPDATE fl_listing_photos SET \
                    number_plate_flag = 1 WHERE \
                    id IN("+result+")"
    print(qry_temp)
    obj_master.obj_db.recCustomQuery(qry_temp)
    return_result[str(listing_id)] = result
    return return_result
    

if __name__ == "__main__":
    obj_master.obj_db.connect()
    # fl_listing_rows = obj_master.obj_db.recSelect('fl_listings',
    #     {'listing_check_flag':0,'status':'active'},1000)
    # fl_listing_rows = obj_master.obj_db.recSelect('fl_listings',
    start_time = obj_master.obj_helper.currentTime(1)
    # for i in range(0,len(fl_listing_rows)):
    #     print("Processing record:"+str(i)+" of total:"+str(len(fl_listing_rows)))
    #     fl_listing_row = fl_listing_rows[i]
    #     listing_id = fl_listing_row['ID']
    #     print("Processing :"+str(listing_id))
    #     try:
    #         final_result = check_image_status(listing_id)
    #     except Exception as e:
    #         print('Error:'+str(e))
    #         continue
    #     print(final_result)        
    try:
        final_result = check_image_status(368606)
        print(final_result)
    except Exception as e:
        print("Error:"+str(e))
        pass
    print("Start:"+start_time)
    print("End:"+obj_master.obj_helper.currentTime(1))
    obj_master.obj_db.disconnect()
