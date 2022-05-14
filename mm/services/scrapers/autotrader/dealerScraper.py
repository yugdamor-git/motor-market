import json
from graphql import graphql
import requests
from concurrent.futures import ThreadPoolExecutor,as_completed
from datetime import datetime


class dealerScraper:
    
    def __init__(self) -> None:
        self.graphql = graphql.Graphql()
        self.proxy = self.graphql.proxy
        self.maxRetry = 3
        
        self.accountId = 24898
        
        self.invalid_category = ["S", "C", "D", "N"]

        self.headers = {
            'authority': 'www.autotrader.co.uk',
            'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
            'device_used': 'desktop',
            'sec-ch-ua-mobile': '?0',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
            'accept': '*/*',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://www.autotrader.co.uk/',
            'accept-language': 'en-US,en;q=0.9'
            } 


    def getTotalPageCount(self,dealerId):
        url = f'https://www.autotrader.co.uk/json/dealers/stock?advertising-location=at_cars&advertising-location=at_profile_cars&dealer={dealerId}'
        
        response = requests.get(url,headers=self.headers,proxies=self.proxy)
        
        jsonResponse = response.json()
        
        totalPages = int(jsonResponse["stockResponse"]["totalPages"])
        
        return totalPages
    

    def getListings(self,dealerId,page):
        
        listings = []
        
        url = f'https://www.autotrader.co.uk/json/dealers/stock?advertising-location=at_cars&advertising-location=at_profile_cars&dealer={dealerId}&page={page}'
        
        for retry in range(0,self.maxRetry):
            try:
                response = requests.get(url,headers=self.headers,proxies=self.proxy)
                
                jsonResponse = response.json()
                
                stockResponse = jsonResponse["stockResponse"]
                
                results = stockResponse["results"]
                
                for item in results:
                    writeOffCategory = item['vehicle']['writeOffCategory']
                    
                    if not writeOffCategory in self.invalid_category:
                        listings.append(item["id"])
                break
            except Exception as e:
                print(f'error : {__file__} : {str(e)}')
                
        return listings
    
    def scrapeByDealerId(self,dealerId):
        totalListings = []
        
        threads = []
        
        try:
            totalPages = self.getTotalPageCount(dealerId)
            with ThreadPoolExecutor(max_workers=100) as executor:
                for page in range(1,totalPages + 1):
                    threads.append(executor.submit(self.getListings,dealerId,page))
                    
                for task in as_completed(threads):
                    listings = task.result()
                    for listing in listings:
                        totalListings.append(listing)
                        
        except Exception as e:
            print(f'error : {__file__} : {str(e)}')
        
        return totalListings
        
        
        
        
if __name__ == "__main__":
    s = dealerScraper()
    t1 = datetime.now()
    print(s.scrapeByDealerId("10019071"))
    t2 = datetime.now()
    
    print(f'total secs : {(t2 - t1).seconds}')