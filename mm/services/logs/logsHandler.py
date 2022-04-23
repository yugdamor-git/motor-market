import pymongo
import os
from datetime import datetime


class Database:
    def __init__(self):

        db_name = "motormarket-logs"
        username = os.environ.get("MONGO_USERNAME")
        password = os.environ.get("MONGO_PASSWORD")
        connection_uri = f"mongodb://{username}:{password}@mongodb/?authSource=admin"
        client = pymongo.MongoClient(connection_uri)
        db = client[db_name]
        self.logs = db["logs"]
        self.listingCount = db["listing-count"]
        self.logCount = db["log-error-count"]


class logsHandler:
    def __init__(self) -> None:
        print("logsHandler init")

        self.database = Database()

    def insertLog(self, data):
        
        currentTime = datetime.now()
        
        today = currentTime.strftime("%d-%m-%Y")

        data["createdAt"] = currentTime

        self.database.logs.insert_one(data)

    def increaseListingCount(self, countFor):

        currentTime = datetime.now()
        
        today = currentTime.strftime("%d-%m-%Y")

        result = list(self.database.find({"timestamp":today,"type":countFor}))
        
        if len(result) > 0:
            self.database.listingCount.update_one(
            {"timestamp": today,"type":countFor}, {
                "$set":{"$inc": {"count": 1},"updatedAt":currentTime}
            }
            )
        else:
            self.database.listingCount.update_one(
                {"timestamp": today , "type":countFor},
                {"$setOnInsert": {"type": countFor, "count": 0,"createdAt":currentTime,"updatedAt":currentTime}},
                upsert=True,
            )
    def increaseErrorCount(self, countFor):

        currentTime = datetime.now()
        
        today = currentTime.strftime("%d-%m-%Y")
        
        result = list(self.database.find({"timestamp":today,"type":countFor}))

        if len(result) > 0:
            self.database.logCount.update_one(
            {"timestamp": today,"type":countFor}, {
                "$set":{"$inc": {"count": 1},"updatedAt":currentTime}
            }
            )
        else:
            self.database.logCount.update_one(
                {"timestamp": today , "type":countFor},
                {"$setOnInsert": {"type": countFor, "count": 0,"createdAt":currentTime,"updatedAt":currentTime}},
                upsert=True,
            )

        