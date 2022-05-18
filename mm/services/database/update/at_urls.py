
import sys
sys.path.append("/libs")

from Database import Database

from pulsar_manager import PulsarManager

import traceback

class topicHandler:
    def __init__(self):
        print("topic handler init")
        
        pulsar_manager = PulsarManager()
        
        self.topics = pulsar_manager.topics
        
        self.logs_producer = pulsar_manager.create_producer(pulsar_manager.topics.LOGS)
        
        self.at_urls_update_consumer = pulsar_manager.create_consumer(pulsar_manager.topics.AT_URLS_UPDATE)
        
        self.db = Database()
        
    def increase_expired_count(self):
        self.logs_producer.produce_message(
                {
                    "eventType":"listingCount",
                    "data":{
                            "countFor":"expired"
                        }
                    }
                )
        
    def handle_update_event(self,what,where):
        what["updated_at"] = {"func":"now()"}
       
        if "Status" in what:
            if what["Status"] == "expired":
                self.increase_expired_count()
        
        self.db.connect()
        
        try:
            self.db.recUpdate("AT_urls",what,where)
        except Exception as e:
            print(f'error : {str(e)}')
        
        self.db.disconnect()
            
    def main(self):
        
        while True:
            try:
                message =  self.at_urls_update_consumer.consume_message()
                
                data = message.get("data",None)
                
                if data == None:
                    print(f'this message does not contain data object.')
                    print(f'message : {message}')
                    continue
                
                
                what = data.get("what",None)
                where = data.get("where",None)
                
                if what == None or where == None:
                    print(f'what and where is not present. skipping...')
                    continue
                
                self.handle_update_event(what,where)
                
            except Exception as e:
                print(f'error : {str(e)}')
                
                log = {}
                
                log["sourceUrl"] = None
                
                log["service"] = self.topics.AT_URLS_UPDATE.value
                
                log["errorMessage"] = traceback.format_exc()
                
                self.logs_producer.produce_message({
                    "eventType":"insertLog",
                    "data":log
                })

if __name__ == "__main__":
    th = topicHandler()
    th.main()