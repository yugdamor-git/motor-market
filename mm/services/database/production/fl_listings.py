from Database import Database

from topic import producer,consumer

from MMUrlGenerator import MMUrlGenerator

import traceback

class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.database.production'
        # table name
        
        self.publish = 'motormarket.scraper.autotrader.listing.generate.image'

        self.db = Database()
        
        self.urlGenerator = MMUrlGenerator()
        
        logsTopic = "motormarket.scraper.logs"
    
        self.logsProducer = producer.Producer(logsTopic)
        
        self.producer = producer.Producer(self.publish)
        
        self.consumer = consumer.Consumer(self.subscribe)
    
    def mapColumnsInsert(self,data):
        columnMapping = {
                'status':'Status',
                'accountId':'Account_ID', 
                'sourceId': 'sourceId',
                'sourceUrl': 'sourceUrl',
                'product_url':'product_url',
                'websiteId': 'Website_ID',
                'dealerName': 'dealer_name',
                'dealerNumber': 'dealer_number',
                'dealerLocation': 'dealer_location',
                'dealerId': 'dealer_id',
                'wheelBase': 'wheelbase',
                'cabType': 'cabtype',
                'make': 'make',
                'model': 'model',
                'built': 'built',
                'seats': 'seats',
                'mileage': 'mileage',
                'fuel': 'fuel',
                'registration': 'registration',
                'writeOffCategory': 'insurance_category',
                'doors': 'doors',
                'bodyStyle': 'org_bodystyle',
                'bodyStylePredicted':'body_style',
                'priceIndicator': 'price_indicator',
                'adminFee': 'admin_fees',
                'trim': 'trim',
                'vehicleType': 'vehicle_type',
                'emissionScheme': 'emission_scheme',
                'transmission': 'transmission_org',
                'engineCylindersLitre': 'engine_cylinders',
                'title': 'title',
                'transmissionCode': 'transmission',
                'fuelCode': 'fuel',
                'margin': 'margin',
                'mmPrice': 'price',
                'sourcePrice': 'sourcePrice',
                '0.069': '0.069',
                '0.079': '0.079',
                '0.089': '0.089',
                '0.099': '0.099',
                '0.109': '0.109',
                '0.119': '0.119',
                '0.129': '0.129',
                '0.139': '0.139',
                '0.149': '0.149',
                '0.159': '0.159',
                '0.169': '0.169',
                '0.179': '0.179',
                '0.189': '0.189',
                '0.199': '0.199',
                '0.209': '0.209',
                '0.219': '0.219',
                '0.229': '0.229',
                '0.239': '0.239',
                '0.249': '0.249',
                '0.259': '0.259',
                '0.269': '0.269',
                '0.279': '0.279',
                '0.289': '0.289',
                '0.299': '0.299',
                '0.309': '0.309',
                '0.319': '0.319',
                '0.329': '0.329',
                '0.339': '0.339',
                '0.349': '0.349',
                '0.359': '0.359',
                '0.369': '0.369',
                '0.379': '0.379',
                '0.389': '0.389',
                '0.399': '0.399',
                '0.409': '0.409',
                '0.419': '0.419',
                '0.429': '0.429',
                '0.439': '0.439',
                '0.449': '0.449',
                '0.459': '0.459',
                '0.469': '0.469',
                '0.479': '0.479',
                '0.489': '0.489',
                '0.499': '0.499',
                '0.399_48': '0.399_48',
                '0.499_48': '0.499_48',
                '0.299_48': '0.299_48',
                'QCF_Oodle_AB': 'QCF_Oodle_AB',
                'QCF_Oodle_C': 'QCF_Oodle_C',
                'QCF_Billing': 'QCF_Billing',
                'GCC': 'GCC',
                'AM_TierIn': 'AM_TierIn',
                'AM_TierEx': 'AM_TierEx',
                'QCF_Adv_E': 'QCF_Adv_E',
                'QCF_Adv_D': 'QCF_Adv_D',
                'QCF_Adv_C': 'QCF_Adv_C',
                'QCF_Adv_AB': 'QCF_Adv_AB',
                'QCF_SMF': 'QCF_SMF',
                'QCF_MB_NT': 'QCF_MB_NT',
                'QCF_MB_T': 'QCF_MB_T',
                'BMF': 'BMF',
                'categoryId': 'Category_ID',
                'videoId': 'video_id'}
        
        mappedData = {}
        
        dataTmp = {}
        
        dataTmp.update(data["data"])
        dataTmp.update(data["data"]["pcpapr"])
        dataTmp.update(data["data"]["ltv"])
        
        for key in columnMapping:
            if key in dataTmp:
                mappedData[columnMapping[key]] = dataTmp[key]
                
        return mappedData
    
    def mapColumnsUpdate(self,data):
        columnMapping = {
                'status':'Status',
                'accountId':'Account_ID', 
                'sourceId': 'sourceId',
                'sourceUrl': 'sourceUrl',
                'product_url':'product_url',
                'websiteId': 'Website_ID',
                'dealerName': 'dealer_name',
                'dealerNumber': 'dealer_number',
                'dealerLocation': 'dealer_location',
                'dealerId': 'dealer_id',
                'wheelBase': 'wheelbase',
                'cabType': 'cabtype',
                'make': 'make',
                'model': 'model',
                'built': 'built',
                'seats': 'seats',
                'mileage': 'mileage',
                'fuel': 'fuel',
                'registration': 'registration',
                'writeOffCategory': 'insurance_category',
                'doors': 'doors',
                'bodyStyle': 'org_bodystyle',
                'bodyStylePredicted':'body_style',
                'priceIndicator': 'price_indicator',
                'adminFee': 'admin_fees',
                'trim': 'trim',
                'vehicleType': 'vehicle_type',
                'emissionScheme': 'emission_scheme',
                'transmission': 'transmission_org',
                'engineCylindersLitre': 'engine_cylinders',
                'title': 'title',
                'transmissionCode': 'transmission',
                'fuelCode': 'fuel',
                'margin': 'margin',
                'mmPrice': 'price',
                'sourcePrice': 'sourcePrice',
                '0.069': '0.069',
                '0.079': '0.079',
                '0.089': '0.089',
                '0.099': '0.099',
                '0.109': '0.109',
                '0.119': '0.119',
                '0.129': '0.129',
                '0.139': '0.139',
                '0.149': '0.149',
                '0.159': '0.159',
                '0.169': '0.169',
                '0.179': '0.179',
                '0.189': '0.189',
                '0.199': '0.199',
                '0.209': '0.209',
                '0.219': '0.219',
                '0.229': '0.229',
                '0.239': '0.239',
                '0.249': '0.249',
                '0.259': '0.259',
                '0.269': '0.269',
                '0.279': '0.279',
                '0.289': '0.289',
                '0.299': '0.299',
                '0.309': '0.309',
                '0.319': '0.319',
                '0.329': '0.329',
                '0.339': '0.339',
                '0.349': '0.349',
                '0.359': '0.359',
                '0.369': '0.369',
                '0.379': '0.379',
                '0.389': '0.389',
                '0.399': '0.399',
                '0.409': '0.409',
                '0.419': '0.419',
                '0.429': '0.429',
                '0.439': '0.439',
                '0.449': '0.449',
                '0.459': '0.459',
                '0.469': '0.469',
                '0.479': '0.479',
                '0.489': '0.489',
                '0.499': '0.499',
                '0.399_48': '0.399_48',
                '0.499_48': '0.499_48',
                '0.299_48': '0.299_48',
                'QCF_Oodle_AB': 'QCF_Oodle_AB',
                'QCF_Oodle_C': 'QCF_Oodle_C',
                'QCF_Billing': 'QCF_Billing',
                'GCC': 'GCC',
                'AM_TierIn': 'AM_TierIn',
                'AM_TierEx': 'AM_TierEx',
                'QCF_Adv_E': 'QCF_Adv_E',
                'QCF_Adv_D': 'QCF_Adv_D',
                'QCF_Adv_C': 'QCF_Adv_C',
                'QCF_Adv_AB': 'QCF_Adv_AB',
                'QCF_SMF': 'QCF_SMF',
                'QCF_MB_NT': 'QCF_MB_NT',
                'QCF_MB_T': 'QCF_MB_T',
                'BMF': 'BMF',
                'categoryId': 'Category_ID',
                'videoId': 'video_id'}
        
        mappedData = {}
        
        dataTmp = {}
        
        dataTmp.update(data["data"])
        dataTmp.update(data["data"]["pcpapr"])
        dataTmp.update(data["data"]["ltv"])
        
        for key in columnMapping:
            if key in dataTmp:
                mappedData[columnMapping[key]] = dataTmp[key]
                
        return mappedData
        
        
    
    def handleEvent(self,event,data):
        if event == "update":
            eventData = data["eventData"]
            what = eventData["what"]
            where = eventData["where"]
            self.db.recUpdate("fl_listings",what,where)
    
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                event = data.get("event",None)
                
                print(f'event : {event}')
                
                if event != None:
                    self.handleEvent(event,data)
                    continue
                print(f'normal execution')
                websiteId = data["data"]["websiteId"]
                
                sourceUrl = data["data"]["sourceUrl"]
                
                where = {
                    "Website_ID":websiteId,
                    "product_url":sourceUrl
                }
                
                self.db.connect()
                records = self.db.recSelect("fl_listings",where)
                self.db.disconnect()
                
                id = None
                
                upsert = None
                
                if len(records) > 0:
                    # update
                    id = records[0]["ID"]
                    mappedData = self.mapColumnsInsert(data)
                    mappedData["product_url"] = mappedData["sourceUrl"]
                    mappedData["updated_at"] = {"func":"now()"}
                    
                    where = {
                        "ID":id
                    }
                    
                    status = records[0]["Status"]
                    
                    self.db.connect()
                    if status == "sold":
                        continue
                    elif status == "pending":
                        self.db.recUpdate("fl_listings",mappedData,where)
                    elif status == "manual_expire":
                        continue
                    elif status == "to_parse":
                        self.db.recUpdate("fl_listings",mappedData,where)
                    elif status == "active":
                        self.db.recUpdate("fl_listings",mappedData,where)
                    elif status == "expired":
                        self.db.recUpdate("fl_listings",mappedData,where)
                    data["data"]["status"] = status
                    upsert = "update"
                    self.db.disconnect()
                else:
                    # insert
                    mappedData = self.mapColumnsInsert(data)
                    mappedData["Status"] = "to_parse"
                    mappedData["product_url"] = mappedData["sourceUrl"]
                    
                    self.db.connect()
                    id = self.db.recInsert("fl_listings",mappedData)
                    self.db.disconnect()
                    
                    make = mappedData["make"]
                    model = mappedData["model"]
                    title = mappedData["title"]
                    
                    mmUrl = self.urlGenerator.generateMMUrl(make,model,title,id)
                    
                    data["data"]["status"] = "to_parse"
                    
                    data["data"]["mmUrl"] = mmUrl
                    
                    upsert = "insert"
                
                data["data"]["upsert"] = upsert
                
                data["data"]["id"] = id
                        
                print(data)
                
                self.producer.produce(data)
                
            except Exception as e:
                print(f'error : {str(e)}')
                
                log = {}
                
                log["sourceUrl"] = data["data"]["sourceUrl"]
                log["service"] = self.subscribe
                log["errorMessage"] = traceback.format_exc()
                
                self.logsProducer.produce({
                    "eventType":"insertLog",
                    "data":log
                })
                
                
if __name__ == "__main__":
    th = topicHandler()
    th.main()