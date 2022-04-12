from Database import Database


db = Database()

db.connect()

listings = db.recSelect("fl_listings")

print(listings)

db.disconnect()