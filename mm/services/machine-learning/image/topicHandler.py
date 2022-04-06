from topic import producer,consumer

from MakeModelPredictor import MakeModelPredictor

class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.predict.image'
        
        self.publish = 'motormarket.scraper.autotrader.listing.validation'

        self.predictor = MakeModelPredictor()
        
        self.producer = producer.Producer(self.publish)
        
        self.consumer = consumer.Consumer(self.subscribe)
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                title = data["data"].get("title")
                
                if title == None:
                    print('title not found')
                    # log error here
                
                prediction = self.predictor.predict(title)
                
                data["data"].update(prediction)
                
                print(data)
                
                self.producer.produce(data)
                
                # break
                
            except Exception as e:
                print(f'error : {str(e)}')
                
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()