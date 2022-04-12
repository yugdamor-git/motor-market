import pymongo
import os
from datetime import datetime

class Database:
    def __init__(self):
        
        db_name = "motormarket-logs"
        username = os.environ.get("MONGO_USERNAME")
        password = os.environ.get("MONGO_PASSWORD")
        connection_uri = f'mongodb://{username}:{password}@mongodb:27017/?authSource=admin'
        client = pymongo.MongoClient(connection_uri)
        db = client[db_name]
        self.logs = db["logs"]
        

class logsHandler:
    def __init__(self) -> None:
        print("logsHandler init")
        
        self.database = Database()
        
    def insert(self,data):
        createdAt = datetime.now()
        
        data["createdAt"] = createdAt
        
        self.database.logs.insert_one(data)
        
        
        
        
        
    