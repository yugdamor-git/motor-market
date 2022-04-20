#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os, signal
import re
import json
import uuid
import time
import random
from functools import partial
import subprocess
#####################
processoutput = os.popen("ps -A -L -F").read()
cur_script = os.path.basename(__file__)
res = re.findall(cur_script, processoutput)
if len(res) > 1:
    print("EXITING BECAUSE ALREADY RUNNING.\n\n")
    exit(0)
#####################
sys.path.append("../modules")
from Master import Master
obj_master = Master()
max_thread_count = 5
key_redis_data = 'car_project_data-1'

def count_current_thread():
    try:
        script_name = os.path.basename(__file__)
        process = subprocess.Popen("ps -L -F -A|grep " + script_name + "|wc -l", shell=True, stdout=subprocess.PIPE)
        return int(process.communicate()[0].decode("utf-8")) - 2
    except Exception as e:
        print(str(e))
    return 100000

def alarm_handler(pid, signum, frame):
    # print ('Signal handler called with signal '+ str(pid))
    # kill process forcefully if execution time completed
    print("PID: " + str(pid) + " killed forcefully\n")
    os.kill(pid, 1)

def make_url_correct(temp_title):
    temp_title = temp_title.lower()
    #convert all spaces to -    
    temp_title = re.sub(r'_+', '-', temp_title, flags=re.S | re.M)
    temp_title = re.sub(r'[\s\.\,\(\)\/\\]', '-', temp_title, flags=re.S | re.M)
    temp_title = re.sub(r'-+', '-', temp_title, flags=re.S | re.M)
    temp_title = re.sub(r'[^a-z0-9_-]', '', temp_title, flags=re.S | re.M)
    return temp_title

def get_final_url(listing_row):
    listing_id = listing_row['ID']
    if listing_row['title'] and listing_row['make'] and listing_row['model']:
        # Create db connection
        try:
            temp_url = make_url_correct(listing_row['title'])            
            temp_make = make_url_correct(listing_row['make'])
            temp_model = make_url_correct(listing_row['model'])
            temp_url = temp_make+'/'+temp_model+'/'+temp_url+'-'+str(listing_id)+'.html'
            temp_url = re.sub(r'-+', '-', temp_url, flags=re.S | re.M)
            final_url = 'https://www.motor.market/cars/'+ temp_url
            obj_master.obj_db.connect()
            obj_master.obj_db.recUpdate('fl_listings',{"mm_url_date":{'func':'now()'},"mm_product_url":final_url},{"id":listing_id})
            print('record updated..')
            obj_master.obj_db.disconnect()
        except Exception as e:
            error_log = "Error:" + str(e)
            print(error_log)

def handleSIGCHLD(sig, frame):
    os.waitpid(-1, os.WNOHANG)

if __name__ == "__main__":        
    obj_master.obj_db.connect()    
    fl_listing_rows = obj_master.obj_db.recCustomQuery("SELECT * FROM fl_listings WHERE status='active' AND mm_product_url IS NULL AND title!='' ORDER BY id ASC LIMIT 4000")
    obj_master.obj_db.disconnect()
    current_thread_count = 0
    signal.signal(signal.SIGCHLD, handleSIGCHLD)
    for i in range(0, len(fl_listing_rows)):
        print('getting final url:'+str(i)+' of total:'+str(len(fl_listing_rows)))        
        p_res = get_final_url(fl_listing_rows[i])
    #     pid = os.fork()
    #     if pid == 0:
    #         # set max execution time for child thread
    #         signal.signal(signal.SIGALRM, partial(alarm_handler, os.getpid()))
    #         signal.alarm(12)  # max execution time 180 sec
    #         # execute task            
    #         p_res = get_final_url(fl_listing_rows[i])
    #         os._exit(0)
    #     elif pid:
    #         current_thread_count +=1
    #         #wait for every child process
    #         os.waitpid(pid, os.WNOHANG)
    #         print('current running thread: ' + str(current_thread_count) + " out of " + str(i)  + "\n")
    #         while current_thread_count >= max_thread_count:
    #             time.sleep(3)
    #             print('waiting 10 sec...\n')
    #             current_thread_count = count_current_thread()    
    # exit()
