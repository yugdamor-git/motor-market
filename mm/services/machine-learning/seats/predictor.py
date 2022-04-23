import pickle

from pathlib import Path

from sklearn.metrics.pairwise import cosine_similarity

from sklearn.feature_extraction.text import TfidfVectorizer

import re

from numpy import dot

import pandas as pd

from numpy.linalg import norm

from redisHandler import redisHandler

import json

class Predictor:
    
    def __init__(self) -> None:
        print(f'SeatsPrediction init')
        
        self.cwd = Path.cwd()
        
        self.dataDir = self.cwd.joinpath("data")
        
        self.data = []
        
        self.dataFile = self.dataDir.joinpath("data.csv")
        
        self.labels = []
        
        self.strings = []
        
        self.vectors = None
        
        self.vectorizer = None
        
        self.tfidfs = []
        
        self.redis = redisHandler()
        
        self.load()
    
    def cosine_similarity(self,list_1, list_2):
        
        cos_sim = dot(list_1, list_2) / (norm(list_1) * norm(list_2))
        
        return cos_sim
  
    def predict(self,make,model):
        redisKey = f'seats.{make}-{model}'
        
        cache = self.redis.get(redisKey)
        
        if cache != None:
            cacheJson = json.loads(cache)
            
            seats = cacheJson["seats"]
            
            return {
                "predictedSeats":seats
            }
        
        makeModel = f'{make.lower().strip()} {model.lower().strip()}'
        
        makeModelVector = self.vectorizer.transform([makeModel]).todense().tolist()[0]
        
        data = {
            "strings":self.strings,
            "labels":self.labels,
            "tfidfs":self.tfidfs
        }
        
        df = pd.DataFrame(data)
        
        df["cosim"] = df["tfidfs"].apply(lambda x:self.cosine_similarity(makeModelVector,x))
        
        max_row = df[df['cosim'] == df['cosim'].max()]
        
        seats = int(float(max_row["labels"].to_list()[0].split(";")[-1]))
        
        score = max_row['cosim'].to_list()[0]
        
        cacheVal  = {
            "seats":seats
        }
        
        self.redis.set(redisKey,json.dumps(cacheVal))
           
        return {
            "predictedSeats":seats
        }
    
    def load(self):
        
        data = pd.read_csv(self.dataFile)
        
        for index,row in data.iterrows():
            key = "{};{};{}".format(str(row['make']).strip().lower(),str(row['model']).strip().lower(),str(row['seats']).strip())
            
            value = "{} {}".format(str(row['make']).strip().lower(),str(row['model']).strip().lower())
            
            self.data.append({"key":key,"value":value})
            
        for item in self.data:
            self.strings.append(item["value"])
            self.labels.append(item["key"])
        
        self.vectorizer = TfidfVectorizer(analyzer="char_wb")
        
        self.vectors = self.vectorizer.fit_transform(self.strings)
        
        tfidf = self.vectorizer.transform(self.strings).todense().tolist()
        
        for item in tfidf:
            self.tfidfs.append(item)
        
        


if __name__ == "__main__":
    mmp = Predictor()
    
    # 76,abarth,punto evo,4
    
    # 8118,bmw,x5,5
    
    # 8119,bmw,x5,7
    
    print(mmp.predict("bmw","x5"))