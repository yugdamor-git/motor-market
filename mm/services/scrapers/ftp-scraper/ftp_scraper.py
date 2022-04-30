from pathlib import Path
from datetime import datetime
import shutil
import time

class ftp_scraper:
    def __init__(self) -> None:
        self.cwd = Path.cwd()
        
        self.files = self.cwd.joinpath("files")
        
        if not self.files.exists():
            self.files.mkdir()
        
        self.processedFilesDir = self.cwd.joinpath("processed-files")
        
        if not self.processedFilesDir.exists():
            self.processedFilesDir.mkdir()
            
    def scrape(self,filePath):
        pass
    
    def move(self,file):
        try:
            now = datetime.now().strftime("%d-%m-%Y-%H-%M")
            
            name = f'{now}_{file.name}'
                
            processedFile = self.processedFilesDir.joinpath(name)
            
            shutil.move(file,processedFile)
            
            print(f'file moved -> \nfrom : {file}\nto : {processedFile}')
        except Exception as e:
            print(f'failed to move file : {str(file)}')
        
    
    def main(self):
        newFiles =list( self.files.glob("*.zip"))
        
        if len(newFiles) == 0:
            print(f'there are no files to process')
            return
        
        for file in newFiles:
            
            self.scrape(file)
            
            self.move(file)
            
if __name__ == "__main__":
    scraper = ftp_scraper()
    
    sleepSeconds = 5 * 60
    
    while True:
        scraper.main()
        print(f'sleeping for {sleepSeconds}')
        time.sleep(sleepSeconds)
        