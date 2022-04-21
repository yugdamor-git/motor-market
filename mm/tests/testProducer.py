from topic import producer


if __name__ == "__main__":
    
    topic = 'motormarket.scraper.autotrader.listing.scrape'
    # topic = "motormarket.scraper.autotrader.listing.validation"
    
    p = producer.Producer(topic)
    
    # data ={
    #     "data": {
    #         "sourceId": "202112200679657",
    #         "sourceUrl": "https://www.autotrader.co.uk/car-details/202112200679657",
    #         "websiteId":"S21",
    #         "accountId":12345
    #         },
    #     }
    data ={
        "data": {
            "sourceId":"202112200679657",
            "sourceUrl": "https://www.autotrader.co.uk/car-details/202112200679657",
            "websiteId":"S21",
            "accountId":12345
            },
        }
    # for i in range(0,10):
    #     p.produce(data)
    for i in range(0,1):
        p.produce(data)
        print(i)
    print("data produced")