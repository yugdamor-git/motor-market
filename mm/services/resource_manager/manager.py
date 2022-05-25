import sys

import shutil

sys.path.append("/libs")

from pathlib import Path

from Database import Database

class Manager:
    
    def __init__(self) -> None:
        
        self.db = Database()

        self.image_dir_path = Path("/var/www/html/files")
    
    def execute_sql(self,sql_query):
        self.db.connect()
        result = []
        try:
            result = self.db.recCustomQuery(sql_query)
        except Exception as e:
            print(f'error : {str(e)}')
        
        self.db.disconnect()
        
        return result
    
    def clean_fl_listing_photos(self):
        
        sql_query = "DELETE FROM `fl_listing_photos` where Listing_ID NOT IN(SELECT ID FROM `fl_listings` WHERE 1)"
        # sql_query = "SELECT COUNT(ID) FROM `fl_listing_photos` where Listing_ID NOT IN(SELECT ID FROM `fl_listings` WHERE 1)"

        self.execute_sql(sql_query)
    
    def clean_fl_listings(self):
        
        max_days = 4
        
        sql_query = f'DELETE FROM `fl_listings` WHERE updated_at <= CURRENT_DATE() - {max_days} AND Status="expired"'
        # sql_query = f'SELECT COUNT(ID) FROM `fl_listings` WHERE updated_at <= CURRENT_DATE() - {max_days} AND Status="expired"'
        
        self.execute_sql(sql_query)
        
  
            
    def get_server_directory_dict(self,website_id):
        
        website_dir = self.image_dir_path.joinpath(f'S{website_id}')
        
        directory_server_dict = {}
        
        for dir in website_dir.iterdir():
            if dir.is_dir() == True:
                directory_server_dict[dir.name] = 1
        
        return directory_server_dict
    
    
    def delete_dir(self,path):
        try:
            shutil.rmtree(path)
            print(f'deleted : {path}')
        except Exception as e:
            print(f'error : {str(e)}')
        
    
    def delete_images(self):
        
        distinct_website_id_query = "SELECT DISTINCT(Website_ID) FROM `fl_listings` WHERE Website_ID IS NOT NULL"
        
        website_ids = self.execute_sql(distinct_website_id_query)
        
        print(f'distinct website id found : {website_ids}')
        
        if len(website_ids) == 0:
            print(f'there are zero website ids in db. skipping...')
            return
        
        for item in website_ids:
            website_id = item["Website_ID"]
            
            print(f'processing : {website_id}')
            
            sql_query = f'SELECT ID FROM `fl_listings` WHERE Website_ID={website_id}'
            
            listings = self.execute_sql(sql_query)
            
            if len(listings) == 0:
                print(f'there are zero listings for website id : {website_id}')
                break
            
            print(f'total listings found for website id : {website_id} is {len(listings)}')
            
            directory_db_dict = {}
            
            for listing in listings:
                dir_name = f'ad{listing["ID"]}'
                directory_db_dict[dir_name] = 1

            if len(directory_db_dict) == 0:
                break
            
            directory_server_dict = self.get_server_directory_dict(website_id)
            
            if len(directory_server_dict) == 0:
                print(f'there are zero folders on server image dir. skipping.')
                break
            
            print(f'total image folder found on server for website id : {website_id} is {len(directory_server_dict)}')
            
            for server_dir in directory_server_dict:
                
                print(f'processing : {server_dir}')
                
                if not server_dir in directory_db_dict:
                    listing_path = self.image_dir_path.joinpath(f'S{website_id}/{server_dir}')
                    print(f'deleting : {listing_path}')
                    self.delete_dir(listing_path)
                    # delete this directory
                else:
                    print(f'skipping : {server_dir}')
                    
    def main(self):
        self.clean_fl_listings()
        self.clean_fl_listing_photos()
        self.delete_images()


if __name__ == "__main__":
    m = Manager()
    m.main()