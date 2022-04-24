import base64
from fastai.vision import *
from pathlib import Path
import os
from datetime import datetime

class Predictor:
    def __init__(self):
        print("image predictor init")

        self.model = load_learner('model','plate_or_not_8.pkl')
        
        self.cwd = Path.cwd()
        
    def predict(self,path):
        
        img = open_image(path)
        
        pred_class,pred_idx,outputs = self.model.predict(img)
        
        if not str(pred_class) in ["plate_number"]:
            return False
        else:
            return True


if __name__ == "__main__":
    p = Predictor()
    
    t1 = datetime.now()
    print(p.predict("numberplate.jpg"))
    t2 = datetime.now()
    
    print((t2 -t1))