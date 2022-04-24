import base64
from fastai.vision import *
from pathlib import Path
import os
from datetime import datetime

class Predictor:
    def __init__(self):
        print("image predictor init")

        self.model = load_learner('model')
        
        self.cwd = Path.cwd()
        
    def predict(self,path):
        
        img = open_image(path)
        
        pred_class,pred_idx,outputs = self.model.predict(img)
        
        return pred_class


if __name__ == "__main__":
    p = Predictor()
    
    print("resized")
    t1 = datetime.now()
    print(p.predict(Path.cwd().joinpath("prediction_test.png")))
    t2 = datetime.now()
    
    print((t2 -t1))
    
    
    print("normal ")
    t1 = datetime.now()
    print(p.predict(Path.cwd().joinpath("test.png")))
    t2 = datetime.now()
    
    print((t2 -t1))