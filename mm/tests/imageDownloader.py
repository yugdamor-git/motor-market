from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import os
from urllib import request
import uuid
import requests

class ImageDownloader:
    def __init__(self) -> None:
        print("image downloader init")
        
        self.cwd = Path.cwd()
        
        self.images = self.cwd.joinpath("images")
        
        if not self.images.exists():
            self.images.mkdir()
        
        self.proxy = {
            "http":os.environ.get("DATACENTER_PROXY"),
            "https":os.environ.get("DATACENTER_PROXY")
        }
        
        self.headers  = {
        'authority': 'm.atcdn.co.uk',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="99", "Google Chrome";v="99"',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36',
        'sec-ch-ua-platform': '"Windows"',
        'accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
        'sec-fetch-site': 'cross-site',
        'sec-fetch-mode': 'no-cors',
        'sec-fetch-dest': 'image',
        'referer': 'https://www.autotrader.co.uk/',
        'accept-language': 'en-US,en;q=0.9,ca;q=0.8,cs;q=0.7,fr;q=0.6,hi;q=0.5'
        }
        
        
    def download_image(self,url):
        
        try:
        
            fileName = f'{str(uuid.uuid4())}.png'
            
            filePath = self.images.joinpath(fileName)
            
            response = requests.get(
                url = url,
                headers = self.headers
            )
            
            filePath.write_bytes(response.content)
            
            return {
                "status":True,
                "url":url,
                "filePath":filePath
            }
        except Exception as e:
            print(f'error : {str(e)}')
        
        return {
                "status":False,
                "url":url,
                "filePath":None
            }
    
    def download_multiple_images(self,urls):
        
        downloadedImages = []
        
        threads = []
        
        with ThreadPoolExecutor(max_workers=30) as executor:
            for url in urls:
                threads.append(executor.submit(self.download_image,url))
        
            for task in as_completed(threads):
                data = task.result()
                
                if data["status"] == False:
                    print(f'failed to donwload image : {data["url"]}')
                    continue
                
                downloadedImages.append(data)
                
        return downloadedImages

    