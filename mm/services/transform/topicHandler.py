from transform import Transform

from topic import producer,consumer



class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.transform'
        
        self.publish = 'motormarket.scraper.autotrader.listing.predict.makemodel'

        logsTopic = "motormarket.scraper.logs"
    
        self.logsProducer = producer.Producer(logsTopic)
        
        self.transform = Transform()
        
        self.producer = producer.Producer(self.publish)
        
        self.consumer = consumer.Consumer(self.subscribe,)
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                rawData = data["rawData"]
                
                transformedData = self.transform.transformData(rawData)
                
                data["rawData"] = transformedData
                
                data["data"].update(rawData)
                
                self.producer.produce(data)
                
            except Exception as e:
                print(f'error : {str(e)}')
                log = {}
                log["errorMessage"] = str(e)
                log["service"] = "services.transform"
                log["sourceUrl"] = meta["sourceUrl"]
                self.logsProducer.produce(log)
                
                
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()