from logsHandler import logsHandler

from topic import consumer


class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        self.subscribe = 'motormarket.scraper.logs'

        self.logsHandler = logsHandler()
        
        self.consumer = consumer.Consumer(self.subscribe)
        
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                print(data)
                
                eventType = data["eventType"]
                
                if eventType == "insertLog":
                    self.logsHandler.insertLog(data["data"])
                    self.logsHandler.increaseErrorCount(data["data"]["service"])
                elif eventType == "listingCount":
                    self.logsHandler.increaseListingCount(data["data"]["countFor"])
                else:
                    print(f'unkown event Type')
                
            except Exception as e:
                print(f'error : {str(e)}')
                
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()