import hashlib
import shutil
import uuid
from datetime import datetime
def generate_unique_uuid():
    return str(uuid.uuid4())

def get_current_datetime():
    return datetime.now()

def clean_string(data):
    
    if data == None:
        return None
    
    return str(data).lower().strip()

def clean_int(data):
    
    if data == None:
        return None
    
    try:
        return int(data)
    except:
        return None

def generate_title(make,model,trim):
    title = ""
    
    if make != None:
        if len(make) != 0:
            title = title + make
            
    if model != None:
        if len(model) != 0:
            title = title + " " + model

    if trim != None:
        if len(trim) != 0:
            title = title + " " + trim
    
    return title.strip()

def generate_sha1(data):
    h = hashlib.sha1()
    h.update(str(data).encode("utf-8"))
    return h.hexdigest()

def delete_directory(path):
    try:
        shutil.rmtree(str(path))
    except Exception as e:
        print(f'error : {str(e)}')