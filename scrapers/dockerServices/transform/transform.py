import pulsar
import json


class transform:
    def __init__(self):
        print(f'transform init')
        
        self.topicTransform = 'motormarket.scrapers.autotrader.listing.transform'
        
        self.topicValidate = 'motormarket.scrapers.autotrader.listing.validate'
        
        self.uri = 'pulsar://pulsar'
        
        self.client = pulsar.Client(self.uri)
        
        self.producer = self.client.create_producer(self.topicValidate)
        
        self.consumer = self.client.subscribe(self.topicTransform, 'Transform-subscription')
        
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
        
    def consume(self):
        
        while True:
            message = self.consumer.receive()
            
            try:
                data = json.loads(message.data)
                
                self.consumer.acknowledge(message)
                
                transformedData = self.transformData(data["data"])
                
                self.produce(transformedData)
                
            except Exception as e:
                print(f'error : {str(e)}')
    
    def produce(self,data):
        
        self.producer.send(
            json.dumps(data).encode("utf-8")
        )
        
    def __del__(self):
        self.client.close()
    
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
            data["make"] = make
        
        # model
        if data["model"] != None:
            model = str(data["model"]).lower().strip()
            data["model"] = model
        
        # engineCylinders
        if data["engineCylinders"] != None:
            engineCylindersCC = int(data["engineCylinders"])
            data["engineCylindersCC"] = engineCylindersCC
            
            engineCylindersLitre = engineCylindersCC/1000
            data["engineCylindersLitre"] = round(engineCylindersLitre,2)
        
        # registration
        if data["registration"] != None:
            registration = str(data["registration"]).strip().upper()
            data["registration"] = registration
        
        # built
        if data["built"] != None:
            built = int(data["built"])
            data["built"] = built
        else:
            code = [str(char) for char in data["registration"] if char.isdigit()]
            code = "".join(code)
            
            if code in self.builtCode:
                built = self.builtCode[code]
                data["built"] = built
        
        # seats
        if data["seats"] != None:
            seats = int(data["seats"])
            data["seats"] = seats
        
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
            data["bodyStyle"] = bodyStyle
        
        # price
        if data["price"] != None:
            price = int(data["price"])
            data["price"] = price
        
        # priceIndicator
        if data["priceIndicator"] != None:
            priceIndicator = str(data["priceIndicator"]).strip().lower()
            data["priceIndicator"] = priceIndicator
        
        # adminFee
        if data["adminFee"] != None:
            adminFee = int(data["adminFee"])
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
        
        data["title"] = f'{data["make"]} {data["model"]} {data["trim"]}'.replace("None","").title()
        
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
        
        return data