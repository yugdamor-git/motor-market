from topic import producer,consumer

from predictor import Predictor

import traceback

class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.predict.makemodel'
        
        self.publish = 'motormarket.scraper.autotrader.listing.predict.seat'
        
        logsTopic = "motormarket.scraper.logs"
    
        self.logsProducer = producer.Producer(logsTopic)

        self.predictor = Predictor()
        
        self.producer = producer.Producer(self.publish)
        
        self.consumer = consumer.Consumer(self.subscribe)
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                rawData = data["rawData"]
                
                title = rawData.get("title")
                
                if title == None:
                    print('title not found')
                    continue
                    # log error here
                
                prediction = self.predictor.predict(title)
                
                data["data"].update(prediction)
                
                print(data)
                
                self.producer.produce(data)
                
                # break
                
            except Exception as e:
                print(f'error : {str(e)}')
                log = {}
                
                log["sourceUrl"] = data["data"]["sourceUrl"]
                log["service"] = self.subscribe
                log["errorMessage"] = traceback.format_exc()
                
                self.logsProducer.produce({
                    "eventType":"insertLog",
                    "data":log
                })
                
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()