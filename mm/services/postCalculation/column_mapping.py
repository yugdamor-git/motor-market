import json

class ColumnMapping:
    def __init__(self) -> None:
        self.insert_mapping = self.load_column_map_json("insert_column_mapping.json")
        self.update_mapping = self.load_column_map_json("update_column_mapping.json")
            
    def load_column_map_json(self,file_name):
        data = None
        with open(file_name,"r") as f:
            data = json.loads(f.read())
        return data
    
    def map_columns(self,data,columnMapping):
        
        mappedData = {}
        
        dataTmp = {}
        
        dataTmp.update(data["data"])
        dataTmp.update(data["data"]["pcpapr"])
        dataTmp.update(data["data"]["ltv"])
        
        for item in columnMapping:
            key = item["key"]
            val = item["value"]
            if key in dataTmp:
                mappedData[val] = dataTmp[key]
                    
        return mappedData