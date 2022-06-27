from pathlib import Path
import pandas as pd



class StarkWood:
    
    def __init__(self) -> None:
        
        self.cwd = Path.cwd()
        
        self.processed_dir = self.cwd.joinpath("processed")
        
        self.pending_dir = self.cwd.joinpath("pending")
        
        self.required_cols = [
            "Body_Type",
            "Make",
            "Model",
            "Version",
            "Engine_Size",
            "Transmission",
            "Fuel_Type",
            "Odometer",
            "Colour",
            "Price",
            "Image_References",
            "Full_Registration",
            "Doors",
            "Vehicle_Type",
            "Reg_Date",
            "Reg_Year",
            "Vehicle_Id"
        ]
        
        self.column_mapping = {
            "Body_Type":"bodyStyle",
            "Make":"make",
            "Model":"model",
            "Version":"trim",
            "Engine_Size":"engineCylinders",
            "Transmission":"transmission",
            "Fuel_Type":"fuel",
            "Odometer":"mileage",
            # "Colour":"",
            "Price":"price",
            "Image_References":"images",
            "Full_Registration":"registration",
            "Doors":"doors",
            "Vehicle_Type":"vehicle_type",
            "Reg_Date":"registrationDate",
            "Reg_Year":"built",
            "Vehicle_Id":"sourceId"
        }
        
        
        
    def process_csv(self,file_name):
        df = pd.read_csv(file_name)
        df.rename(columns=self.column_mapping,inplace=True)
        
        req_col = []
        for c in self.column_mapping:
            req_col.append(self.column_mapping[c])
        
        df = df[req_col]
        
        print(df.columns)
        
    
    def main(self):
        
        for file in self.pending_dir.glob("*.csv"):
            print(file)
            self.process_csv(file)
    
    
if __name__ == "__main__":
    
    sw = StarkWood()
    
    sw.main()