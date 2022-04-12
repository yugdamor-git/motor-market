
import sys
import time
sys.path.append("../modules")

from messenger import messenger


from Master import Master

from smartproxy_alert import smartproxy_alert


from abc import ABC,abstractmethod

class validations:
    def __init__(self):
        obj_master = Master()
        self.obj_master = obj_master
        self.mm_bot = messenger()
        self.db = obj_master.obj_db
        self.smartproxy = smartproxy_alert()
    
    def connect_db(self):
        self.obj_master.obj_db.connect()

    def disconnect_db(self):
        self.obj_master.obj_db.disconnect()

    def check_duplicate_listings(self):
        self.obj_master.obj_db.connect()
        query = "SELECT `product_url` FROM fl_listings GROUP BY `product_url` HAVING COUNT(`product_url`) > 1"
        result = self.obj_master.obj_db.recCustomQuery(query)
        self.obj_master.obj_db.disconnect()
        
        if len(result) > 0:
            messages = []
            messages.append(f'*Duplicate Listing Alert*')
            messages.append(f'> i found some duplicate listings in db.')
            messages.append(f'> {result}`')
            self.mm_bot.send_message("\n".join(messages))
    
    def generate_mm_stats(self):
        message = None
        try:
            total_listings_query = 'SELECT COUNT(ID) FROM `fl_listings` WHERE 1'
            total_active_query = 'SELECT COUNT(ID) FROM `fl_listings` WHERE Status="active" AND Category_ID != 0'
            total_to_parse_query = 'SELECT COUNT(ID) FROM `fl_listings` WHERE Status="to_parse" AND Category_ID = 0'
            total_expired_query = 'SELECT COUNT(ID) FROM `fl_listings` WHERE Status="expired"'
            self.connect_db()
            total_listings = self.db.recCustomQuery(total_listings_query)[0]["COUNT(ID)"]
            total_active = self.db.recCustomQuery(total_active_query)[0]["COUNT(ID)"]
            total_to_parse = self.db.recCustomQuery(total_to_parse_query)[0]["COUNT(ID)"]
            total_expired = self.db.recCustomQuery(total_expired_query)[0]["COUNT(ID)"]
            message = f'*MM STATS*\n> total listings : *{total_listings}*\n> total active : *{total_active}*\n> to parse : *{total_to_parse}*\n> total expired : *{total_expired}*'
            self.mm_bot.send_message(message)
        except Exception as e:
            self.mm_bot.send_message(f'*Exception*\n> error message : {str(e)}')
        finally:
            self.disconnect_db

    def smart_proxy_data_usage_check(self):
        responses = self.smartproxy.alert()
        for resp in responses:
            if resp["status"] == True:
                self.mm_bot.send_message(f'*Smart Proxy*\n> {resp["message"]}')
            else:
                self.mm_bot.send_message(f'*Smart Proxy*\n> {resp["message"]}')
    
    def check_black_listed_dealer_listings(self):
        self.connect_db()
        try:
            messages = []
            messages.append("*BLACK LISTED LISTING CHECK*")
            listings = self.db.recCustomQuery("""SELECT fl_dealer_blacklist.dealer_id,fl_listings.dealer_id,fl_listings.ID,fl_listings.Status FROM fl_dealer_blacklist INNER JOIN fl_listings ON fl_dealer_blacklist.dealer_id = fl_listings.dealer_id""")
            for listing in listings:
                if listing["Status"] == "active":
                    self.db.recUpdate("fl_listings",{"Status":"expired","mannual_expire":1},{"ID":listing["ID"]})
                    print(f'Expired : {listing["ID"]}')
                    messages.append(f'Expired : {listing["ID"]}')
            if len(messages) == 1:
                messages.append("> 0 black listed listing found !")
            self.mm_bot.send_message("\n".join(messages))
        except Exception as e:
            print(f'check_black_listed_dealer_listings : {str(e)}')
        self.disconnect_db()
    
if __name__ == "__main__":
    validator = validations()
    time.sleep(2)
    validator.check_duplicate_listings()
    time.sleep(2)
    validator.generate_mm_stats()
    time.sleep(2)
    validator.smart_proxy_data_usage_check()
    time.sleep(2)
    validator.check_black_listed_dealer_listings()