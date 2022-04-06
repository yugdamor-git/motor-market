from transform import Transform

from topic import producer,consumer


class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.transform'
        
        self.publish = 'motormarket.scraper.autotrader.listing.predict.makemodel'

        self.transform = Transform()
        
        self.producer = producer.Producer(self.publish)
        
        self.consumer = consumer.Consumer(self.subscribe)
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                transformedData = self.transform.transformData(data["data"])
                
                data["data"] = transformedData
                
                print(data)
                
                self.producer.produce(data)
                
            except Exception as e:
                print(f'error : {str(e)}')
                
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()