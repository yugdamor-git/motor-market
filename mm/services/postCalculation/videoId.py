
from pathlib import Path
import pickle


class videoId:
    def __init__(self) -> None:
        self.videoData = None
        
        self.cwd = Path.cwd()
        
        self.videoFilePath = self.cwd.joinpath("videoId.h5")
        
        self.load_video_data()
        
    def load_video_data(self):
        with open(self.videoFilePath,"rb") as f:
            self.videoData = pickle.load(f)
    
    def get_video_id(self,make,model,built):
        videoId = None
        make_model = f'{make};{model}'.lower()
        if make_model in self.videoData:
            carData = self.videoData[make_model]
            for vidData in carData["cars"]:
                if vidData["year_condition"] == True:
                    if built != None:
                        if built >= vidData["start_year"] and built <= vidData["end_year"]:
                            videoId = vidData["video_id"]
                            break
                else:
                    videoId = vidData["video_id"]
                    break
        return videoId