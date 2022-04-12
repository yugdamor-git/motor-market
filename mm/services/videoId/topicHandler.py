from topic import producer,consumer

from videoId import videoId


class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.videoid'
        
        self.publish = 'motormarket.scraper.autotrader.listing.predict.test'

        logsTopic = "motormarket.scraper.logs"
    
        self.logsProducer = producer.Producer(logsTopic)
        
        self.videoId = videoId()
        
        self.producer = producer.Producer(self.publish)
        
        self.consumer = consumer.Consumer(self.subscribe,)
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                meta = data["meta"]
                
                make = data["data"]["predictedMake"]
                model = data["data"]["predictedModel"]
                built = data["data"]["built"]
                
                videoId = self.videoId.get_video_id(
                    make,model,built
                )
                
                data["data"]["videoId"] = videoId
                
                print(data)
                
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