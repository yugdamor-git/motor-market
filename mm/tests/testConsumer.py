from topic import consumer


if __name__ == "__main__":
    topic = 'motormarket.scraper.autotrader.listing.scrape'
    c = consumer.Consumer(topic)
    while True:
        print(c.consume())