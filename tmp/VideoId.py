import pickle

import sys

sys.path.append("../modules")

from Master import Master

import pandas as pd

class VideoId:
    def __init__(self):
        self.file_path = "/var/www/html/car_scrapers/common_scripts/video_id/video_id.h5"
        self.video_data = None
        self.load_video_data()
        self.obj_master = Master()
        self.db_update = []
        self.csv_data = []

    
    def load_video_data(self):
        with open(self.file_path,"rb") as f:
            self.video_data = pickle.load(f)
        print('video id data loaded')
    
    def get_video_id(self,make,model,built):
        listing_video_id = None
        make_model = f'{make};{model}'.lower()
        if make_model in self.video_data:
            car_data = self.video_data[make_model]
            for vid_data in car_data["cars"]:
                if vid_data["year_condition"] == True:
                    if built != None:
                        # print(vid_data)
                        if built >= vid_data["start_year"] and built <= vid_data["end_year"]:
                            listing_video_id = vid_data["video_id"]
                            print(f'Video Id Found for : {make_model} built : {built}, video id : {listing_video_id}')
                            self.csv_data.append({"make":vid_data["make"],"model":vid_data["model"],"built":built,"video_id":vid_data["video_id"]})
                            break
                        else:
                            print(f'No Data Available for : {make_model} and year : {built}')
                else:
                    listing_video_id = vid_data["video_id"]
                    print(f'Video Id Found for : {make_model} ,built : {built} , video id : {listing_video_id}')
                    self.csv_data.append({"make":vid_data["make"],"model":vid_data["model"],"built":built,"video_id":vid_data["video_id"]})
                    break
                
        
        return listing_video_id
    
    def get_pending_listings(self):
        query = 'SELECT * FROM `fl_listings` WHERE video_id = 0  AND Status IN("active","to_parse")'
        # video_id = 0  AND
        self.obj_master.obj_db.connect()
        all_records = self.obj_master.obj_db.recCustomQuery(query)
        self.obj_master.obj_db.disconnect()
        return all_records
    
    def process_record(self,row):
        make = row["make"]
        model = row["model"]
        built = row["built"]
        try:
            built = int(built)
        except:
            built = None
        
        video_id = self.get_video_id(make,model,built)

        if video_id != None:
            self.db_update.append({"ID":row["ID"],"data":{"video_id":video_id}})
    
    def update_video_id_in_db(self):
        self.obj_master.obj_db.connect()
        for row in self.db_update:
            print(f'updating : {row["ID"]} , video id : {row["data"]["video_id"]}')
            self.obj_master.obj_db.recUpdate("fl_listings",row["data"],{"ID":row["ID"]})
        self.obj_master.obj_db.disconnect()
    




if __name__ == "__main__":
    vd = VideoId()
    pending_listings = vd.get_pending_listings()
    # listings = [
    #     {"make":"alfa romeo","model":"159","built":2007},
    #     {"make":"alfa romeo","model":"159","built":2011}

    # ]
    # for listing in listings:
    #     print(listing)
    #     vd.get_video_id(listing["make"],listing["model"],listing["built"])
    for listing in pending_listings:
        vd.process_record(listing)
    print(f'video id found for : {len(vd.db_update)} records.')
    # df = pd.DataFrame(vd.csv_data)
    # df.to_csv("mm_video_id.csv")
    vd.update_video_id_in_db()
