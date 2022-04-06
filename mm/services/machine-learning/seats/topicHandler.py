from topic import producer,consumer

from predictor import Predictor

class topicHandler:
    def __init__(self):
        print("seat topic handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.predict.seat'
        
        self.publish = 'motormarket.scraper.autotrader.listing.validation'

        self.predictor = Predictor()
        
        self.producer = producer.Producer(self.publish)
        
        self.consumer = consumer.Consumer(self.subscribe)
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                make = data["data"]["predictedMake"]
                model = data["data"]["predictedModel"]
                
                prediction = self.predictor.predict(make,model)
                
                data["data"].update(prediction)
                
                print(data)
                
                self.producer.produce(data)
                
                # break
                
            except Exception as e:
                print(f'error : {str(e)}')
                
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()