

class Transform:
    def __init__(self):
        print(f'transform init')
        
        self.transmissionCode = {
            "automatic":1,
            "manual":2,
            "automanual":3,
            "auto":1,
            "auto clutch":1,
            "auto/manual mode":1,
            "cvt":1,
            "cvt/manual mode":1,
            "manual transmission":2,
            "semi-automatic":1,
            "sequential":1
        }
        
        self.fuelCode = {
            "petrol":1,
            "diesel":2,
            "gas":3,
            "hybrid":5,
            "electric":6,
            "hybrid – petrol/elec":7,
            "hybrid –":8,
        }
        
        self.builtCode = {
            "64":2014,
            "14":2014,
            "65":2015,
            "15":2015,
            "66":2016,
            "16":2016,
            "67":2017,
            "17":2017,
            "68":2018,
            "18":2018,
            "69":2019,
            "19":2019,
            "70":2020,
            "20":2020,
            "71":2021,
            "21":2021
        }
        
        self.bodyStyles = [
                           {"from":"coupe","to":"Coupe"},
                           {"from":"stationwagon","to":"Estate"},
                           {"from":"van","to":"Van"},
                           {"from":"lvc","to":"Van"},
                           {"from":"standard roof minibus","to":"Minibus"},
                           {"from":"estate","to":"Estate"},
                           {"from":"convertible","to":"Convertible"},
                           {"from":"mpv","to":"MPV"},
                           {"from":"sedan","to":"Saloon"},
                           {"from":"hatchback","to":"Hatchback"},
                           {"from":"saloon","to":"Saloon"},
                           {"from":"suv","to":"SUV"},
                           {"from":"combi van","to":"Van"},
                           {"from":"wheelchair adapted vehicle - w","to":"Wheelchair"},
                           {"from":"double cab pick-up","to":"Pickup"}
        ]
        
    
    def calculateMargin(self,make,model,cc):
        
        if make == None or model == None or cc == None:
            return None
        
        key = f'{make}_{model}'
        
        if make in self.make4x4:
            category = "4x4"
        elif make in self.makeLuxury:
            category = "luxury"
        else:
            category = self.carTypesDict.get(key,None)
            
            if category == None:
                category = "others"
        
        margin = None
        
        marginDict = self.marginBasedOnCC[category]
        
        for key in marginDict:
            data = marginDict[key]
            
            if cc >= data["min"] and cc < data["max"]:
                margin = data["margin"]
                break

        return margin
    
    def transformValidatorData(self,data):
        if data["price"] != None:
            price = int(data["price"])
            data["price"] = price
        else:
            price =0
        
        data["cal_price_from_file"] = price
        
        # adminFee
        if data["adminFee"] != None:
            adminFee = data["adminFee"].replace("£","").strip()
            data["adminFee"] = int(adminFee)
        else:
            adminFee = 0
            data["adminFee"] = adminFee
            

       
        # at price
        
        data["sourcePrice"] = data.get("price") + data.get("adminFee",0)
        
        
        # mileage
        if data["mileage"] != None:
            mileage = int(data["mileage"])
            data["mileage"] = mileage
        
        # built
        if data["built"] != None:
            built = int(data["built"])
            data["built"] = built
        
        # dealer_id
        if "dealer_id" in data:
            data["dealerId"] = data["dealer_id"]
        
        return data
    
    def transformData(self,data):
        
        # dealer Name
        if data["dealerName"] != None:
            dealerName = data["dealerName"].strip().lower()
            data["dealerName"] = dealerName
            
        # dealer Number
        if data["dealerNumber"] != None:
            dealerNumber = data["dealerNumber"].strip().lower()
            data["dealerNumber"] = dealerNumber
            
        # dealer Location
        if data["dealerLocation"] != None:
            dealerLocation = data["dealerLocation"].strip().replace(" ","").upper()
            data["dealerLocation"] = dealerLocation
            
        #  location
        if data["location"] != None:
            location = data["location"].strip().replace(" ","").upper()
            data["location"] = location
            
        # dealer Id
        if data["dealerId"] != None:
            dealerId = data["dealerId"].strip()
            data["dealerId"] = dealerId
        
        #  wheelBase
        
        # cabtype
        if data["cabType"] != None:
            cabType = str(data["cabType"]).lower().strip()
            data["cabType"] = cabType
        
        # make
        if data["make"] != None:
            make = str(data["make"]).lower().strip()
            data["orignalMake"] = make
        
        # model
        if data["model"] != None:
            model = str(data["model"]).lower().strip()
            data["orignalModel"] = model
        
        # engineCylinders
        if data["engineCylinders"] != None:
            engineCylindersCC = int(data["engineCylinders"])
            data["engineCylindersCC"] = engineCylindersCC
            
            engineCylindersLitre = engineCylindersCC/1000
            data["engineCylindersLitre"] = round(engineCylindersLitre,2)
        else:
            data["engineCylindersCC"] = None
            data["engineCylindersLitre"] = None
        
        # registration
        if data["registration"] != None:
            registration = str(data["registration"]).strip().upper()
            data["orignalRegistration"] = registration
        
        # built
        if data["built"] != None:
            built = int(data["built"])
            data["built"] = built
        
        
        # seats
        if data["seats"] != None:
            seats = int(data["seats"])
            data["seats"] = seats
            data["predictedSeats"] = seats
        
        # mileage
        if data["mileage"] != None:
            mileage = int(data["mileage"])
            data["mileage"] = mileage
        
        # fuel
        if data["fuel"] != None:
            fuel = str(data["fuel"]).strip().lower()
            data["fuel"] = fuel
        
        # writeOffCategory
        if data["writeOffCategory"] != None:
            writeOffCategory = str(data["writeOffCategory"]).strip().lower()
            data["writeOffCategory"] = writeOffCategory
            
        # doors
        if data["doors"] != None:
            doors = int(data["doors"])
            data["doors"] = doors
        
        # bodyStyle
        if data["bodyStyle"] != None:
            bodyStyle = str(data["bodyStyle"]).strip().lower()
            data["orignalBodyStyle"] = bodyStyle
            data["predictedBodyStyle"] = None
            
            for bs in self.bodyStyles:
                if bodyStyle in bs["from"].lower():
                    data["predictedBodyStyle"] = bs["to"]
                    break
        
        # price
        if data["price"] != None:
            price = int(data["price"])
            data["price"] = price
        
        data["cal_price_from_file"] = price
        
        # priceIndicator
        if data["priceIndicator"] != None:
            priceIndicator = str(data["priceIndicator"]).strip().lower()
            data["priceIndicator"] = priceIndicator
        
        # adminFee
        if data["adminFee"] != None:
            adminFee = data["adminFee"].replace("£","").strip()
            data["adminFee"] = int(adminFee)
        else:
            adminFee = 0
            data["adminFee"] = adminFee
        
        # trim
        if data["trim"] != None:
            trim = str(data["trim"]).strip().lower()
            data["trim"] = trim
        
        # vehicleType
        if data["vehicleType"] != None:
            vehicleType = str(data["vehicleType"]).strip().lower()
            data["vehicleType"] = vehicleType
        
        # emissionScheme
        
        # transmission
        if data["transmission"] != None:
            transmission = str(data["transmission"]).strip().lower()
            data["transmission"] = transmission
        
        # id
        if data["id"] != None:
            id = str(data["id"]).strip()
            data["id"] = id
        
        # images
        images = []
        for img in data["images"]:
            images.append(img["url"])
        
        data["images"] = images
        
        # url
        
        # add / calc new fields
        # title
        
        data["title"] = f'{data["orignalMake"]} {data["orignalModel"]} {data["trim"]}'.replace("None","").title()
        
        # transmissionCode
        if data["transmission"] in self.transmissionCode:
            data["transmissionCode"] = self.transmissionCode[data["transmission"]]
        else:
            data["transmissionCode"] = 4
        
        # fuelCode
        if data["fuel"] in self.fuelCode:
            data["fuelCode"] = self.fuelCode[data["fuel"]]
        else:
            data["fuelCode"] = 4
        
        # at price
        data["sourcePrice"] = data.get("price") + data.get("adminFee",0)
        
        return data