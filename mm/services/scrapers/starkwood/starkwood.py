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
        
        # self.column_mapping = {
        #     "Body_Type":"bodyStyle",
        #     "Make",
        #     "Model",
        #     "Version",
        #     "Engine_Size",
        #     "Transmission",
        #     "Fuel_Type",
        #     "Odometer",
        #     "Colour",
        #     "Price",
        #     "Image_References",
        #     "Full_Registration",
        #     "Doors",
        #     "Vehicle_Type",
        #     "Reg_Date",
        #     "Reg_Year",
        #     "Vehicle_Id"
        # }
        
    def process_csv(self,file_name):
        df = pd.read_csv(file_name)
        
        print(df.shape)
        print(df.columns)
    
    def main(self):
        
        for file in self.pending_dir.glob("*.csv"):
            print(file)
            self.process_csv(file)
    
    
if __name__ == "__main__":
    
    sw = StarkWood()
    
    sw.main()