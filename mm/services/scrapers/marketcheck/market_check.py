from pathlib import Path
import pandas as pd

class MarketCheck:
    
    def __init__(self) -> None:
        
        self.cwd = Path.cwd()
        
        self.new_files_dir = self.cwd.joinpath("new_files")
        
        self.processed_files_dir = self.cwd.joinpath("processed_files")
        
        if not self.new_files_dir.exists():
            self.new_files_dir.mkdir()
        
        if not self.processed_files_dir.exists():
            self.processed_files_dir.mkdir()
            
        self.accountId = 24898
    
        self.planId = 26
        
        self.featureId = 26
        
        self.websiteId = "18"
        
        self.priority = 109
        
    def parse_dealers(self,df:pd.DataFrame):
        dealers = []
        columns = ["dealer_id","seller_name"]       
        dealer_df = df[columns]
        dealer_df.drop_duplicates(inplace=True)
        
        for index,row in dealer_df.iterrows():
            row_dict = row.to_dict()
            dealers.append(row_dict)
        return dealers
    
    def parse_engine_size(self,text):
        try:
            tmp = float(text.replace("L","").strip())
            return int(tmp * 1000)
        except:
            return None
    
    def parse_listings(self,df:pd.DataFrame):
        listings = []
        columns = []
        
        listing_df = df[columns]
        listing_df.drop_duplicates(inplace=True)
        
        for index,row in listing_df.iterrows():
            row_dict = row.to_dict()
            tmp = {}
            
            tmp["sourceId"] = row_dict["id"]
            tmp["sourceUrl"] = row_dict["vdp_url"]
            tmp["price"] = row_dict["price"]
            tmp["mileage"] = row_dict["miles"]
            tmp["built"] = row_dict["built"]
            tmp["make"] = row_dict["make"]
            tmp["model"] = row_dict["model"]
            tmp["trim"] = row_dict["variant"]
            tmp["bodyStyle"] = row_dict["body_type"]
            tmp["fuel"] = row_dict["fuel_type"]
            tmp["transmission"] = row_dict["transmission"]
            tmp["doors"] = row_dict["doors"]
            tmp["registration"] = row_dict["vehicle_registration_mark"]
            tmp["registration_date"] = row_dict["vehicle_registration_date"]
            tmp["exterior_color"] = row_dict["exterior_color"]
            tmp["dealer_id"] = row_dict["dealer_id"]
            tmp["dealer_name"] = row_dict["seller_name"]
            tmp["dealer_number"] = row_dict["seller_phone"]
            
            tmp["location_json"] = {
                "street":row_dict["street"],
                "city":row_dict["city"],
                "county":row_dict["county"],
                "postal_code":row_dict["postal_code"],
                "country":row_dict["country"]
            }
            
            tmp["dealer_location"] = row_dict["postal_code"]
            
            tmp["images"] = row_dict["photo_links"].split("|")
            
            tmp["engineCylinders"] = self.parse_engine_size(row_dict["engine_size"])
            
            tmp["accountId"] = self.accountId
            tmp["websiteId"] = self.websiteId
            tmp["featuredId"] = self.featureId
            tmp["planId"] = self.planId
            tmp["priority"] = self.priority
            
            listings.append(tmp)
            
        return listings
    
    def parse_csv(self,filepath):
        df = pd.read_csv(filepath)
        
        dealers = self.parse_dealers(df)
        
        listings = self.parse_listings(df)
        
        
        
    