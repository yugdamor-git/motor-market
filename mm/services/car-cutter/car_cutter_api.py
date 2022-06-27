from pathlib import Path
import time
import requests
from helper import load_images,generate_sha1_hash

class CarCutter:
    def __init__(self) -> None:
        self.api_key = "pidiw7jdlul9p36xeie1"
        
        self.cwd = Path.cwd()
        
        self.background_remove_angles = [
            "exterior_front-left",
            "exterior_front-right",
            "exterior_rear-right",
            "exterior_rear-left"
        ]
        
        self.processed_image_dir = self.cwd.joinpath("processed-images")
        
        if not self.processed_image_dir.exists():
            self.processed_image_dir.mkdir()
        
        self.raw_images = self.cwd.joinpath("raw-images")
        
        if not self.raw_images.exists():
            self.raw_images.mkdir()
            
        self.max_retry = 20
    
    
    def submit_images(self,images):
        
        url = "https://api.car-cutter.com/vehicle/image/submission"
        
        payload={'image_url': images}
                
        headers = {
        'Authorization': f'Bearer {self.api_key}'
        }

        json_response = None
        
        for i in range(0,self.max_retry):
            response = requests.request("POST", url, headers=headers, data=payload)
            if response.status_code == 200:
                json_response = response.json()
                break
        
        return json_response
    
    def check_status(self,image_url):
        
        url = f'https://api.car-cutter.com/vehicle/image/status?image_url={image_url}'
        
        headers = {
        'Authorization': f'Bearer {self.api_key}'
        }

        response = requests.request("GET", url, headers=headers,)

        json_response = response.json()
        
        return json_response
    
    def save_image(self,content,path):
        
        file_path = Path(path)
        
        if file_path.exists() == True:
            print(f'deleted existing file')
            file_path.unlink()
        
        time.sleep(2)
        
        with open(file_path,"wb") as f:
            f.write(content)
        
    
    def get_result(self,image_url,file_path):
        status = False
        url = f'https://api.car-cutter.com/vehicle/image/result?image_url={image_url}'
        
        headers = {
        'Authorization': f'Bearer {self.api_key}'
        }
        
        for i in range(0,self.max_retry):
            response = requests.request("GET", url, headers=headers)
            if response.status_code == 200:
                self.save_image(response.content,file_path)
                print(f'downloaded : {file_path}')
                status = True
                break
            time.sleep(1)
            
        return status
        
        

    
    def process_images(self,images):
        interior = []
        exterior = []
        all_images_by_id = {}
        car_cutter_images = []
        for image in images:
            all_images_by_id[image["id"]] = image
            car_cutter_images.append(image["url"])
        processed_images = []
        result = self.submit_images(car_cutter_images)
        time.sleep(2)
        index = 0
        for item in result["data"]["images"]:
            url = item["image"]
            id = generate_sha1_hash(url)
            angle = "_".join(item["angle"]).lower()
            if "exterior" in angle:
                if angle in self.background_remove_angles:
                    if "front" in angle:
                        if id in all_images_by_id:
                            img_item = all_images_by_id[id].copy()
                            img_item["angle"] = angle
                            exterior.insert(0,img_item)
                    elif "rear" in angle:
                        if id in all_images_by_id:
                            img_item = all_images_by_id[id].copy()
                            img_item["angle"] = angle
                            exterior.append(img_item)
                else:
                    pass
            elif "interior" in angle:
                if id in all_images_by_id:
                    img_item = all_images_by_id[id].copy()
                    img_item["angle"] = angle
                    interior.append(img_item)
        index = 0
        for i in exterior:
            tmp = i.copy()
            tmp["position"] = index
            
            if self.get_result(tmp["url"],tmp["path"]) == False:
                
                continue
            
            processed_images.append(tmp)
            index += 1
        
        for i in interior:
            print(i)
            tmp = i.copy()
            tmp["position"] = index
            processed_images.append(tmp)
            index += 1
        
        return processed_images


# if __name__ == "__main__":
#     cc = CarCutter()
#     images = load_images()
#     # for i in images:
#     #     print(f'image_url:{i}')
#     for i in cc.process_images(images):
#         print(i)