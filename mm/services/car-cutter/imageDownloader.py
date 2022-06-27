from concurrent.futures import ThreadPoolExecutor,as_completed
import hashlib
from pathlib import Path
import os
import requests

class ImageDownloader:
    def __init__(self) -> None:
        print("image downloader init")
        
        self.cwd = Path.cwd()
        
        self.media = self.cwd.joinpath("media")
        
        if not self.media.exists():
            self.media.mkdir()
            
        self.max_retry = 10
        
        self.datacenterProxy = os.environ.get("DATACENTER_PROXY")
        print(self.datacenterProxy)
        self.proxy = {
            "http":self.datacenterProxy,
            "https":self.datacenterProxy
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
        
    def download_image(self,url,websiteId,sourceId,position,websiteDir,listingDir):
        
        imageId = hashlib.sha1(
                url.encode("utf-8")
            ).hexdigest()
        
        
        
        filePath = listingDir.joinpath(f'{imageId}.png')
        
        predictionImagePath = listingDir.joinpath(f'prediction_{imageId}.png')
        
        if filePath.exists():
            return {
                "status":True,
                "url":url,
                "path":filePath,
                "id":imageId,
                "position":position
                }
        
        try:
            
            for retry in range(0,self.max_retry):
                try:
                    response = requests.get(
                        url = url,
                        headers = self.headers,
                        proxies=self.proxy
                    )
                    break
                except Exception as e:
                    print(f'error downloading image : {str(e)}')
                    
            
            filePath.write_bytes(response.content)
            
            
            return {
                "status":True,
                "url":url,
                "path":filePath,
                "id":imageId,
                "position":position
                }
            
        except Exception as e:
            print(f'error : {str(e)}')
        
        return {
                "status":False,
                "url":url,
                "path":None
            }
    
    def download_multiple_images(self,urls,websiteId,sourceId):
        
        downloadedImages = []
        
        threads = []
        
        websiteDir = self.media.joinpath(websiteId)
        
        if not websiteDir.exists():
            websiteDir.mkdir()
        
        listingDir = websiteDir.joinpath(sourceId)
        
        if not listingDir.exists():
            listingDir.mkdir()
        
        with ThreadPoolExecutor(max_workers=30) as executor:
            for position,url in enumerate(urls):
                threads.append(executor.submit(self.download_image,url,websiteId,sourceId,position,websiteDir,listingDir))
        
            for task in as_completed(threads):
                data = task.result()
                
                if data["status"] == False:
                    print(f'failed to donwload image : {data["url"]}')
                    continue
                
                downloadedImages.append(data)
                
        return downloadedImages


if __name__ == "__main__":
    
    # testing...
    
    downloader = ImageDownloader()
    
    urls = ["https://m.atcdn.co.uk/a/media/{resize}/710e498f1b584428b8e690bf39056299.jpg", "https://m.atcdn.co.uk/a/media/{resize}/151a19e80b52448aa51621a52df1bb0e.jpg", "https://m.atcdn.co.uk/a/media/{resize}/4f8745de95e84631aaf25b0f42cbba44.jpg", "https://m.atcdn.co.uk/a/media/{resize}/117028d5758c4705830307519552cee2.jpg", "https://m.atcdn.co.uk/a/media/{resize}/d5cd447790dc421babb769b803d630af.jpg", "https://m.atcdn.co.uk/a/media/{resize}/a708d155277740be83668dd52c395d33.jpg", "https://m.atcdn.co.uk/a/media/{resize}/ea0c223f5d324bbc80c6e7d554da768f.jpg", "https://m.atcdn.co.uk/a/media/{resize}/d2c496966d8b479e8b9661042d6f5600.jpg", "https://m.atcdn.co.uk/a/media/{resize}/62581f5b080a4c6b82780a42c9269036.jpg", "https://m.atcdn.co.uk/a/media/{resize}/7455ea23e7404dde96953265b3925195.jpg", "https://m.atcdn.co.uk/a/media/{resize}/71b7e7f78d8f466ab3f2bed80defd9b4.jpg", "https://m.atcdn.co.uk/a/media/{resize}/f979516bf03a437e824574ba0822102c.jpg", "https://m.atcdn.co.uk/a/media/{resize}/47840075257344b583974d714024c493.jpg", "https://m.atcdn.co.uk/a/media/{resize}/d07711a42a364291968c76a9379bb2a4.jpg", "https://m.atcdn.co.uk/a/media/{resize}/b095c1954fe24306bb38a559eb5e9c05.jpg", "https://m.atcdn.co.uk/a/media/{resize}/bb6f9a0172384e4cb1889a9bee20fe90.jpg", "https://m.atcdn.co.uk/a/media/{resize}/7dac620a25a14400b5829d823735cd84.jpg", "https://m.atcdn.co.uk/a/media/{resize}/b519eddf7b624aa68455ea81f397eb9b.jpg", "https://m.atcdn.co.uk/a/media/{resize}/4a3d8811ae154366a9582290d0e6aef6.jpg"]
    websiteId = "S17"
    
    sourceId = "12345"
        
    downloader.download_multiple_images(urls,websiteId,sourceId)


