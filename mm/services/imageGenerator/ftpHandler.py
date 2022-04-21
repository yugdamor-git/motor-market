import hashlib
from io import BufferedIOBase, BytesIO
import os
from ftplib import FTP,error_perm
from zipfile import Path
import posixpath
from posixpath import dirname

class ftpHandler:
    def __init__(self) -> None:
        self.username = os.environ.get("PRODUCTION_SERVER_FTP_USERNAME")
        self.password = os.environ.get("PRODUCTION_SERVER_FTP_PASSWORD")
        self.host = os.environ.get("PRODUCTION_SERVER_IP_ADDRESS")
        self.port = int(os.environ.get("PRODUCTION_SERVER_FTP_PORT"))
        self.imageDir = os.environ.get("PRODUCTION_SERVER_IMAGE_DIR_PREFIX")
        
        self.uri = f'ftp://{self.host}:{self.port}{self.imageDir}'
        
        self.overwrite = "STOR"
        
        self.connection = FTP()
        
        self.connect()
        
        self.connection.cwd(self.imageDir)
        
    def connect(self):
        response = self.connection.connect(self.host,self.port)
        
        if not "220" in response:
            return False
        
        print(self.username)
        print(self.password)
        response = self.connection.login(self.username,self.password)
        
        if not "230" in response:
            return False
        
        if not "530" in response:
            return False
        
        return True
    
    def disconnect(self):
        self.connection.quit()
    
    def isConnected(self):
        try:
            self.connection.voidcmd("NOOP")
            return True
        except Exception as e:
            print(f'ftp disconnected : {str(e)}')
            return False
    
    def getFileStats(self,filePath):
        try:
            if self.isConnected() == False:
                self.connect()
            
            lastModified = float(self.connection.voidcmd(f'MDTM {filePath}')[4:].strip())
            m = hashlib.md5()
            self.connection.retrbinary(f'RETR {filePath}',m.update)
            
            return {
                'exists':True,
                'lastModified':lastModified,
                "checksum":m.hexdigest()
            }
            
        except Exception as e:
            return {
                'exists':False
            }
            
    def createDirectory(self,path,firstCall=True):
        self.connection.cwd(self.imageDir)
        try:
            self.connection.cwd(path)
        except error_perm:
            print(f'directory does not exists : {path}')
            
            self.createDirectory(dirname(path),False)
            
            self.connection.mkd(path)
            
            if firstCall:
                self.connection.cwd(path)
                
    def uploadFile(self,filePath:Path,file:BufferedIOBase):
        
        file.seek(0)
        
        if self.isConnected() == False:
            self.connect()
        
        self.connection.storbinary(f'{self.overwrite} {filePath}',file)
        
        file.close()


if __name__ == "__main__":
    # testing
    ftp = ftpHandler()
    
    # load image and upload it to server on any tmp folder
    try:
        img = None
        with open("test.png","rb") as f:
            img = BytesIO(f.read())
        
        imagePath = f'{ftp.imageDir}/S56/ad12345/test.png'
        
        ftp.uploadFile(imagePath,img)
    except Exception as e:
        print(f'error : {str(e)}')
    
    ftp.disconnect()
    
    
    

