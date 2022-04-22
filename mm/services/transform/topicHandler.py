from transform import Transform

from topic import producer,consumer

import traceback


class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.transform'
        
        self.publish = 'motormarket.scraper.autotrader.listing.predict.makemodel'
        
        self.postCalculationTopic = 'motormarket.scraper.autotrader.listing.post.calculation'

        logsTopic = "motormarket.scraper.logs"
    
        self.logsProducer = producer.Producer(logsTopic)
        
        self.transform = Transform()
        
        self.producer = producer.Producer(self.publish)
        
        self.consumer = consumer.Consumer(self.subscribe,)
        
        self.postCalculationProducer = producer.Producer(self.postCalculation)
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                scraperType = data["data"].get("scraperType")
                
                if scraperType == "validator":
                    transformedData = self.transform.transformValidatorData(data["data"])
                    data["data"].update(transformedData)
                    self.postCalculationProducer.produce(data)
                    continue
                
                elif scraperType == "normal":
                    transformedData = self.transform.transformData(data)
                
                    data["data"].update(transformedData)
                
                print(data)
                
                self.producer.produce(data)
                
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