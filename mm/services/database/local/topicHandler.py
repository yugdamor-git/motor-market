from localDbHandler import localDbHandler
from topic import producer,consumer


class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.database.local'
        
        self.publish = 'motormarket.scraper.autotrader.listing.database.production'

        self.localdb = localDbHandler()
        
        self.producer = producer.Producer(self.publish)
        
        self.consumer = consumer.Consumer(self.subscribe)
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                self.localdb.upsertListing(data)
                
                # self.producer.produce(data)
                
            except Exception as e:
                print(f'error : {str(e)}')
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()