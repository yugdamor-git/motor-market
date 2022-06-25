import hashlib
import json


def load_images():
    data = None
    images = []
    with open("input.json","rb") as f:
        data = json.loads(f.read())
    
    for index,i in enumerate(data):
        images.append({
            "url":i["url"],
            "id":generate_sha1_hash(i["url"]),
            "position":index
        })
    
    return images


def generate_sha1_hash(data):
    sha1 = hashlib.sha1()
    
    sha1.update(str(data).encode("utf-8"))
    
    return str(sha1.hexdigest())