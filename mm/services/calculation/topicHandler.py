from calculation import Calculation
from topic import producer,consumer

class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.calculation'
        
        self.publish = 'motormarket.scraper.autotrader.listing.videoid'

        logsTopic = "motormarket.scraper.logs"
    
        self.logsProducer = producer.Producer(logsTopic)
        
        self.calculation = Calculation()
        
        self.producer = producer.Producer(self.publish)
        
        self.consumer = consumer.Consumer(self.subscribe)
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                meta = data["meta"]
                
                pcpapr = self.calculation.calculatePcpApr(data["data"])
                
                data["data"]["pcpapr"] = pcpapr
                
                print(data)
                
                self.producer.produce(data)
                
            except Exception as e:
                print(f'error : {str(e)}')
                log = {}
                log["errorMessage"] = str(e)
                log["service"] = "services.calculation"
                log["sourceUrl"] = meta["sourceUrl"]
                
                self.logsProducer.produce(log)
                
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()