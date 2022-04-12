import pymongo
import os
from datetime import datetime

class Database:
    def __init__(self):
        
        db_name = "motormarket"
        username = os.environ.get("MONGO_USERNAME")
        password = os.environ.get("MONGO_PASSWORD")
        connection_uri = f'mongodb://{username}:{password}@mongodb:27017/?authSource=admin'
        
        client = pymongo.MongoClient(connection_uri)
        
        db = client[db_name]
        
        self.listings = db["listings"]


class localDbHandler:
    def __init__(self) -> None:
        self.db = Database()
    
    def upsertListing(self,data):
        
        data = data["data"]
        
        meta = data["meta"]
        
        uniqueId = meta["uniqueId"]
        
        listings = list(self.db.listings.find({"_id":uniqueId}))
        
        if len(listings) == 0:
            self.insertListing(data,uniqueId)
            
        if len(listings) > 0:
            self.updateListing(data,uniqueId)
        
        return uniqueId
    
    def find(self,sourceUrl):
        pass
         
    def insert(self,data,uniqueId):
        
        data["_id"] = uniqueId
        
        data["status"] = "updated"
        
        data["createdAt"] = datetime.now()
        
        data["updatedAt"] = datetime.now()
        
        self.db.listings.insert_one(data)
    
    def update(self,newData,uniqueId):
        
        newData["status"] = "updated"
        
        newData["updatedAt"] = datetime.now()
        
        self.db.listings.update_one(
            { "_id":uniqueId },
            { "$set":newData }
        )
        
        
        
    