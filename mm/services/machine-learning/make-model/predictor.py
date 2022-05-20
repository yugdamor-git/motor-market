import pickle

from pathlib import Path

from sklearn.metrics.pairwise import cosine_similarity

from sklearn.feature_extraction.text import TfidfVectorizer

import re

import json

from redisHandler import redisHandler

class Predictor:
    
    def __init__(self) -> None:
        print(f'MakeModelPrediction init')
        
        self.cwd = Path.cwd()
        
        self.dataDir = self.cwd.joinpath("data")
        
        self.labelsFile = self.dataDir.joinpath("labels.h5")
        
        self.stringsFile = self.dataDir.joinpath("strings.h5")
        
        self.labels = None
        
        self.strings = None
        
        self.vectors = None
        
        self.vectorizer = None
        
        self.redis = redisHandler()
        
        self.stop_words = ['cdti', '5dr', 'limited edition', 'limited', 'edition', 'tfsi', 'thp', 'gdi', 'cvt', 'tip',
                      'td', 'aircross', 'dci', 'sline', 'tgi', 'premium', 'quattro', 'sport', 'coupe', 'amg', 'motor uk']
        
        self.load()
    
    def predict(self,title):
        
        cache = self.redis.get(f'makemodel.{title}')
        
        if cache != None:
            cacheJson = json.loads(cache)
            make = cacheJson["make"]
            model = cacheJson["model"]
            return {
            "predictedMake":make.title(),
            "predictedModel":model.title()
                }
        
        titleVector = self.vectorizer.transform([title])
        
        cosim = cosine_similarity(titleVector,self.vectors)
        
        label = self.labels[cosim.argmax()]
        
        make = label.split(";")[0]
        
        model = label.split(";")[1]
        
        cacheVal = {
            "make":make,
            "model":model
        }
        self.redis.set(f'makemodel.{title}',json.dumps(cacheVal))
        
        return {
            "predictedMake":make.title(),
            "predictedModel":model.title()
        }

    def load(self):
        
        with open(self.labelsFile,"rb") as f:
            self.labels = pickle.load(f)
            
        with open(self.stringsFile,"rb") as f:
            self.strings = pickle.load(f)
        
        self.vectorizer = TfidfVectorizer(analyzer='word',use_idf=False,tokenizer=self.customTokenizer)
        
        self.vectors = self.vectorizer.fit_transform(self.strings)
        
    def customTokenizer(self,title):
        
        title = str(title).lower().replace("-"," ")
        
        re_exps = ["\(.*?\)","\d+\.\d+","\[.*?\]"]
        
        for exp in re_exps:
            bracket = re.findall(exp,title)
            if len(bracket) >= 1:
                for brac in bracket:
                    title = title.replace(brac,"").strip()
        
        replace_punc = ['[',']','(',')','/','&','+','!']
        
        for punc in replace_punc:
            title = title.replace(punc," ")
        
        title_words = title.split(" ")

        for index,word in enumerate(title_words):
            if len(word) == 1:
                if index == 0:
                    title_words[index] = word + title_words[index+1]
                else:
                    title_words.append(title_words[index-1])
                    title_words[index - 1] = title_words[index-1] + word
                    if index != len(title_words) -1:
                        title_words.pop(index)
        
        unique_words = []
        
        for word in title_words:
            if word not in unique_words:
                if len(word) > 1:
                    unique_words.append(word.strip())
                    
        for word in self.stop_words:
            if len(word) > 1:
                if word.strip() in unique_words:
                    unique_words.pop(unique_words.index(word.strip()))
        
        last_out =[]
        
        for word in unique_words:
            if len(word) > 1:
                last_out.append(word)
        
        return last_out


# if __name__ == "__main__":
#     mmp = MakeModelPrediction()
    
#     print(mmp.predict("bmw 1 series"))