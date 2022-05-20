from Database import Database

from topic import producer,consumer

from MMUrlGenerator import MMUrlGenerator

import traceback

class topicHandler:
    def __init__(self):
        print("transform topic handler init")
        
        self.subscribe = 'motormarket.scraper.autotrader.listing.database.production'
        
        self.publish = 'motormarket.scraper.autotrader.listing.generate.image'
        
        self.urlScraperTopic = 'motormarket.scraper.autotrader.listing.database.urlscaper'

        self.db = Database()
        
        self.urlGenerator = MMUrlGenerator()
        
        logsTopic = "motormarket.scraper.logs"
    
        self.logsProducer = producer.Producer(logsTopic)
        
        self.producer = producer.Producer(self.publish)
        
        self.consumer = consumer.Consumer(self.subscribe)
        
        self.urlScraperProducer = producer.Producer(self.urlScraperTopic)
    
    def mapColumnsInsert(self,data):
        columnMapping =[
        {'key':'featuredId','value':'Featured_ID'},
        {'key':'planId','value':'Plan_ID'},
        {'key':'priority','value':'Priority'},
        {'key':'customPriceEnabled','value':'customPriceEnabled'},
        {'key':'customPriceEnabled','value':'custom_price_Enable'},
        {'key':'customPrice','value':'customPrice'},
        {'key':'ltvStatus','value':'ltvStatus'},
        {'key': 'status', 'value': 'Status'},
        {'key': 'accountId', 'value': 'Account_ID'},
        {'key': 'sourceId', 'value': 'sourceId'},
        {'key': 'sourceUrl', 'value': 'sourceUrl'},
        {'key': 'sourceUrl', 'value': 'product_url'},
        {'key': 'websiteId', 'value': 'Website_ID'},
        {'key': 'dealerName', 'value': 'dealer_name'},
        {'key': 'dealerNumber', 'value': 'dealer_number'},
        {'key': 'dealerLocation', 'value': 'dealer_location'},
        {'key': 'dealerId', 'value': 'dealer_id'},
        {'key': 'wheelBase', 'value': 'wheelbase'},
        {'key': 'cabType', 'value': 'cabtype'},
        {'key': 'orignalMake', 'value': 'orignalMake'},
        {'key': 'orignalModel', 'value': 'orignalModel'},
        {'key': 'predictedMake', 'value': 'predictedMake'},
        {'key': 'predictedModel', 'value': 'predictedModel'},
        {'key': 'predictedMake', 'value': 'make'},
        {'key': 'predictedModel', 'value': 'model'},
        {'key': 'built', 'value': 'built'},
        {'key': 'predictedSeats', 'value': 'predictedSeats'},
        {'key': 'predictedSeats', 'value': 'seats'},
        {'key': 'mileage', 'value': 'mileage'},
        {'key': 'fuel', 'value': 'fuel'},
        {'key': 'orignalRegistration', 'value': 'orignalRegistration'},
        {'key': 'predictedRegistration', 'value': 'predictedRegistration'},
        {'key': 'predictedRegistration', 'value': 'product_website_id'},
        {'key': 'predictedRegistration', 'value': 'registration'},
        {'key': 'predictedRegistration', 'value': 'reference_number'},
        {'key': 'writeOffCategory', 'value': 'insurance_category'},
        {'key': 'doors', 'value': 'doors'},
        {'key': 'orignalBodyStyle', 'value': 'orignalBodyStyle'},
        {'key': 'predictedBodyStyle', 'value': 'predictedBodyStyle'},
        {'key': 'orignalBodyStyle', 'value': 'org_bodystyle'},
        {'key': 'predictedBodyStyle', 'value': 'body_style'},
        {'key': 'priceIndicator', 'value': 'price_indicator'},
        {'key': 'adminFee', 'value': 'admin_fees'},
        {'key': 'trim', 'value': 'trim'},
        {'key': 'vehicleType', 'value': 'vehicle_type'},
        {'key': 'emissionScheme', 'value': 'emission_scheme'},
        {'key': 'transmission', 'value': 'transmission_org'},
        {'key': 'engineCylindersCC', 'value': 'engineCylindersCC'},
        {'key': 'engineCylindersLitre', 'value': 'engineCylindersLitre'},
        {'key': 'engineCylindersLitre', 'value': 'engine_cylinders'},
        {'key': 'scraperName', 'value': 'scraperName'},
        {'key': 'title', 'value': 'title'},
        {'key': 'transmissionCode', 'value': 'transmission'},
        {'key': 'fuelCode', 'value': 'fuel'},
        {'key': 'margin', 'value': 'margin'},
        {'key': 'mmPrice', 'value': 'price'},
        {'key': 'mmPrice', 'value': 'mmPrice'},
        {'key': 'sourcePrice', 'value': 'sourcePrice'},
        {'key': 'cal_price_from_file', 'value': 'cal_price_from_file'},
        {'key': '0.069', 'value': '0.069'},
        {'key': '0.079', 'value': '0.079'},
        {'key': '0.089', 'value': '0.089'},
        {'key': '0.099', 'value': '0.099'},
        {'key': '0.109', 'value': '0.109'},
        {'key': '0.119', 'value': '0.119'},
        {'key': '0.129', 'value': '0.129'},
        {'key': '0.139', 'value': '0.139'},
        {'key': '0.149', 'value': '0.149'},
        {'key': '0.159', 'value': '0.159'},
        {'key': '0.169', 'value': '0.169'},
        {'key': '0.179', 'value': '0.179'},
        {'key': '0.189', 'value': '0.189'},
        {'key': '0.199', 'value': '0.199'},
        {'key': '0.209', 'value': '0.209'},
        {'key': '0.219', 'value': '0.219'},
        {'key': '0.229', 'value': '0.229'},
        {'key': '0.239', 'value': '0.239'},
        {'key': '0.249', 'value': '0.249'},
        {'key': '0.259', 'value': '0.259'},
        {'key': '0.269', 'value': '0.269'},
        {'key': '0.279', 'value': '0.279'},
        {'key': '0.289', 'value': '0.289'},
        {'key': '0.299', 'value': '0.299'},
        {'key': '0.309', 'value': '0.309'},
        {'key': '0.319', 'value': '0.319'},
        {'key': '0.329', 'value': '0.329'},
        {'key': '0.339', 'value': '0.339'},
        {'key': '0.349', 'value': '0.349'},
        {'key': '0.359', 'value': '0.359'},
        {'key': '0.369', 'value': '0.369'},
        {'key': '0.379', 'value': '0.379'},
        {'key': '0.389', 'value': '0.389'},
        {'key': '0.399', 'value': '0.399'},
        {'key': '0.409', 'value': '0.409'},
        {'key': '0.419', 'value': '0.419'},
        {'key': '0.429', 'value': '0.429'},
        {'key': '0.439', 'value': '0.439'},
        {'key': '0.449', 'value': '0.449'},
        {'key': '0.459', 'value': '0.459'},
        {'key': '0.469', 'value': '0.469'},
        {'key': '0.479', 'value': '0.479'},
        {'key': '0.489', 'value': '0.489'},
        {'key': '0.499', 'value': '0.499'},
        {'key': '0.399_48', 'value': '0.399_48'},
        {'key': '0.499_48', 'value': '0.499_48'},
        {'key': '0.299_48', 'value': '0.299_48'},
        {'key': 'QCF_Oodle_AB', 'value': 'QCF_Oodle_AB'},
        {'key': 'QCF_Oodle_C', 'value': 'QCF_Oodle_C'},
        {'key': 'QCF_Billing', 'value': 'QCF_Billing'},
        {'key': 'GCC', 'value': 'GCC'},
        {'key': 'AM_TierIn', 'value': 'AM_TierIn'},
        {'key': 'AM_TierEx', 'value': 'AM_TierEx'},
        {'key': 'QCF_Adv_E', 'value': 'QCF_Adv_E'},
        {'key': 'QCF_Adv_D', 'value': 'QCF_Adv_D'},
        {'key': 'QCF_Adv_C', 'value': 'QCF_Adv_C'},
        {'key': 'QCF_Adv_AB', 'value': 'QCF_Adv_AB'},
        {'key': 'QCF_SMF', 'value': 'QCF_SMF'},
        {'key': 'QCF_MB_NT', 'value': 'QCF_MB_NT'},
        {'key': 'QCF_MB_T', 'value': 'QCF_MB_T'},
        {'key': 'BMF', 'value': 'BMF'},
        {'key': 'categoryId', 'value': 'Category_ID'},
        {'key': 'videoId', 'value': 'video_id'},
        {'key':'registrationStatus','value':'registrationStatus'},
        {'key':'registrationStatus','value':'number_plate_flag'},
        {'key': 'dealerForecourtPrice', 'value': 'dealerForecourtPrice'},
        {'key':'tradeLifecycleStatus','value':'tradeLifecycleStatus'},
        {'key':'registration_date','value':'registration_date'},
        {'key': 'photosCount', 'value': 'Photos_count'}]
        
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
                
        print(mappedData)  
        return mappedData
    
    def mapColumnsUpdate(self,data):
        columnMapping =[
        {'key':'customPriceEnabled','value':'customPriceEnabled'},
        {'key':'customPrice','value':'customPrice'},
        {'key':'ltvStatus','value':'ltvStatus'},
        {'key': 'status', 'value': 'Status'},
        {'key':'customPriceEnabled','value':'custom_price_Enable'},
        {'key': 'cal_price_from_file', 'value': 'cal_price_from_file'},
        # {'key': 'accountId', 'value': 'Account_ID'},
        # {'key': 'sourceId', 'value': 'sourceId'},
        # {'key': 'sourceUrl', 'value': 'sourceUrl'},
        # {'key': 'sourceUrl', 'value': 'product_url'},
        # {'key': 'websiteId', 'value': 'Website_ID'},
        # {'key': 'dealerName', 'value': 'dealer_name'},
        # {'key': 'dealerNumber', 'value': 'dealer_number'},
        # {'key': 'dealerLocation', 'value': 'dealer_location'},
        # {'key': 'dealerId', 'value': 'dealer_id'},
        # {'key': 'wheelBase', 'value': 'wheelbase'},
        # {'key': 'cabType', 'value': 'cabtype'},
        # {'key': 'orignalMake', 'value': 'orignalMake'},
        # {'key': 'orignalModel', 'value': 'orignalModel'},
        # {'key': 'predictedMake', 'value': 'predictedMake'},
        # {'key': 'predictedModel', 'value': 'predictedModel'},
        # {'key': 'predictedMake', 'value': 'make'},
        # {'key': 'predictedModel', 'value': 'model'},
        # {'key': 'built', 'value': 'built'},
        # {'key': 'predictedSeats', 'value': 'predictedSeats'},
        # {'key': 'predictedSeats', 'value': 'seats'},
        # {'key': 'mileage', 'value': 'mileage'},
        # {'key': 'fuel', 'value': 'fuel'},
        # {'key': 'orignalRegistration', 'value': 'orignalRegistration'},
        # {'key': 'predictedRegistration', 'value': 'predictedRegistration'},
        # {'key': 'predictedRegistration', 'value': 'product_website_id'},
        # {'key': 'predictedRegistration', 'value': 'registration'},
        # {'key': 'predictedRegistration', 'value': 'reference_number'},
        # {'key': 'writeOffCategory', 'value': 'insurance_category'},
        # {'key': 'doors', 'value': 'doors'},
        # {'key': 'orignalBodyStyle', 'value': 'orignalBodyStyle'},
        # {'key': 'predictedBodyStyle', 'value': 'predictedBodyStyle'},
        # {'key': 'orignalBodyStyle', 'value': 'org_bodystyle'},
        # {'key': 'predictedBodyStyle', 'value': 'body_style'},
        # {'key': 'priceIndicator', 'value': 'price_indicator'},
        {'key': 'adminFee', 'value': 'admin_fees'},
        {'key': 'dealerForecourtPrice', 'value': 'dealerForecourtPrice'},
        # {'key': 'trim', 'value': 'trim'},
        # {'key': 'vehicleType', 'value': 'vehicle_type'},
        # {'key': 'emissionScheme', 'value': 'emission_scheme'},
        # {'key': 'transmission', 'value': 'transmission_org'},
        # {'key': 'engineCylindersCC', 'value': 'engineCylindersCC'},
        # {'key': 'engineCylindersLitre', 'value': 'engineCylindersLitre'},
        # {'key': 'engineCylindersLitre', 'value': 'engine_cylinders'},
        {'key': 'scraperName', 'value': 'scraperName'},
        # {'key': 'title', 'value': 'title'},
        # {'key': 'transmissionCode', 'value': 'transmission'},
        # {'key': 'fuelCode', 'value': 'fuel'},
        # {'key': 'margin', 'value': 'margin'},
        # {'key': 'mmPrice', 'value': 'price'},
        {'key': 'mmPrice', 'value': 'mmPrice'},
        {'key': 'sourcePrice', 'value': 'sourcePrice'},
        # {'key': 'sourcePrice', 'value': 'cal_price_from_file'},
        {'key': '0.069', 'value': '0.069'},
        {'key': '0.079', 'value': '0.079'},
        {'key': '0.089', 'value': '0.089'},
        {'key': '0.099', 'value': '0.099'},
        {'key': '0.109', 'value': '0.109'},
        {'key': '0.119', 'value': '0.119'},
        {'key': '0.129', 'value': '0.129'},
        {'key': '0.139', 'value': '0.139'},
        {'key': '0.149', 'value': '0.149'},
        {'key': '0.159', 'value': '0.159'},
        {'key': '0.169', 'value': '0.169'},
        {'key': '0.179', 'value': '0.179'},
        {'key': '0.189', 'value': '0.189'},
        {'key': '0.199', 'value': '0.199'},
        {'key': '0.209', 'value': '0.209'},
        {'key': '0.219', 'value': '0.219'},
        {'key': '0.229', 'value': '0.229'},
        {'key': '0.239', 'value': '0.239'},
        {'key': '0.249', 'value': '0.249'},
        {'key': '0.259', 'value': '0.259'},
        {'key': '0.269', 'value': '0.269'},
        {'key': '0.279', 'value': '0.279'},
        {'key': '0.289', 'value': '0.289'},
        {'key': '0.299', 'value': '0.299'},
        {'key': '0.309', 'value': '0.309'},
        {'key': '0.319', 'value': '0.319'},
        {'key': '0.329', 'value': '0.329'},
        {'key': '0.339', 'value': '0.339'},
        {'key': '0.349', 'value': '0.349'},
        {'key': '0.359', 'value': '0.359'},
        {'key': '0.369', 'value': '0.369'},
        {'key': '0.379', 'value': '0.379'},
        {'key': '0.389', 'value': '0.389'},
        {'key': '0.399', 'value': '0.399'},
        {'key': '0.409', 'value': '0.409'},
        {'key': '0.419', 'value': '0.419'},
        {'key': '0.429', 'value': '0.429'},
        {'key': '0.439', 'value': '0.439'},
        {'key': '0.449', 'value': '0.449'},
        {'key': '0.459', 'value': '0.459'},
        {'key': '0.469', 'value': '0.469'},
        {'key': '0.479', 'value': '0.479'},
        {'key': '0.489', 'value': '0.489'},
        {'key': '0.499', 'value': '0.499'},
        {'key': '0.399_48', 'value': '0.399_48'},
        {'key': '0.499_48', 'value': '0.499_48'},
        {'key': '0.299_48', 'value': '0.299_48'},
        {'key': 'QCF_Oodle_AB', 'value': 'QCF_Oodle_AB'},
        {'key': 'QCF_Oodle_C', 'value': 'QCF_Oodle_C'},
        {'key': 'QCF_Billing', 'value': 'QCF_Billing'},
        {'key': 'GCC', 'value': 'GCC'},
        {'key': 'AM_TierIn', 'value': 'AM_TierIn'},
        {'key': 'AM_TierEx', 'value': 'AM_TierEx'},
        {'key': 'QCF_Adv_E', 'value': 'QCF_Adv_E'},
        {'key': 'QCF_Adv_D', 'value': 'QCF_Adv_D'},
        {'key': 'QCF_Adv_C', 'value': 'QCF_Adv_C'},
        {'key': 'QCF_Adv_AB', 'value': 'QCF_Adv_AB'},
        {'key': 'QCF_SMF', 'value': 'QCF_SMF'},
        {'key': 'QCF_MB_NT', 'value': 'QCF_MB_NT'},
        {'key': 'QCF_MB_T', 'value': 'QCF_MB_T'},
        {'key': 'BMF', 'value': 'BMF'},
        # {'key': 'categoryId', 'value': 'Category_ID'},
        # {'key': 'videoId', 'value': 'video_id'},
        {'key':'registrationStatus','value':'registrationStatus'},
        {'key':'registrationStatus','value':'number_plate_flag'},
        {'key':'registration_date','value':'registration_date'}
        # {'key': 'photosCount', 'value': 'Photos_count'}]
        ]
        
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
        print(mappedData)
        
        return mappedData
        
    
    
    def handleEvent(self,event,data):
        if event == "update":
            eventData = data["eventData"]
            what = eventData["what"]
            what["updated_at"] = {"func":"now()"}
            where = eventData["where"]
            
            print(eventData)
            
            if "Status" in what:
                if what["Status"] == "expired":
                    self.logsProducer.produce(
                    {
                        "eventType":"listingCount",
                        "data":{
                                "countFor":"expired"
                            }
                        }
                    )
            
            if "status" in what:
                if what["status"] == "expired":
                    self.logsProducer.produce(
                    {
                        "eventType":"listingCount",
                        "data":{
                                "countFor":"expired"
                            }
                        }
                    )
            
            
            
            self.db.recUpdate("fl_listings",what,where)
    
    def handleAtUrl(self,status,id,listingId,registrationStatus):
        what = {
            
        }
        
        if status == "to_parse":
            what["listing_status"] = "active"
        else:
            what["listing_status"] = status
        what["scraped"] = 1
        what["listing_id"] = id
        what["updated_at"] = {"func":"now()"}
        
        if registrationStatus == 1:
            what["number_plate_flag"] = 0
        elif registrationStatus == None:
            what["number_plate_flag"] = 2
        else:
            what["number_plate_flag"] = 2
        
        
        where = {
            "id":listingId
        }
        
        eventType = "update"
        
        self.urlScraperProducer.produce({
            "what":what,
            "where":where,
            "eventType":eventType
        })

    
    def main(self):
        print("listening for new messages")
        while True:
            try:
                data =  self.consumer.consume()
                
                event = data.get("event",None)
                
                print(f'event : {event}')
                
                if event != None:
                    self.db.connect()
                    self.handleEvent(event,data)
                    self.db.disconnect()
                    continue
                
                
                scraperType = data["data"].get("scraperType")
                
                scraperName = data["data"].get("scraperName",None)
                
                if scraperType == "validator":
                    mappedData = self.mapColumnsInsert(data)
                    mappedData["updated_at"] = {"func":"now()"}
                    
                    where = {
                            "ID":data["data"]["ID"]
                    }
                    
                    status = data["data"]["Status"]
                    
                    mappedData["Status"] = status
                    
                    if status == "expired":
                        mappedData["Status"] = "active"
                        
                    if data["data"]["registrationStatus"] == False:
                        mappedData["Status"] = "pending"
                    
                    if status in ["sold","pending","to_parse","manual_expire"]:
                        mappedData["Status"] = status
                        
                    
                    if scraperName == "url-scraper":
                        status = mappedData["Status"]
                        listingId = data["data"].get("listingId")
                        
                        registrationStatus = data["data"].get("registrationStatus",None)
                        
                        self.handleAtUrl(status,data["data"]["ID"],listingId,registrationStatus)
                    
                    if status in ["sold","pending","manual_expire"]:
                        continue
                    
                    self.db.connect()
                    self.db.recUpdate("fl_listings",mappedData,where)
                    self.db.disconnect()
                    
                    self.logsProducer.produce(
                    {
                        "eventType":"listingCount",
                        "data":{
                                "countFor":"update"
                            }
                        }
                    )
                    
                    continue
                
                
                elif scraperType == "normal":
                    print(f'normal execution')
                    websiteId = data["data"]["websiteId"]
                    
                    sourceId = data["data"]["sourceId"]
                    
                    where = {
                        "Website_ID":websiteId,
                        "sourceId":sourceId
                    }
                    
                    self.db.connect()
                    records = self.db.recSelect("fl_listings",where)
                    self.db.disconnect()
                    
                    id = None
                    
                    upsert = None
                    
                    if len(records) > 0:
                        # update
                        id = records[0]["ID"]
                        mappedData = self.mapColumnsUpdate(data)
                        mappedData["updated_at"] = {"func":"now()"}
                        
                        where = {
                            "ID":id
                        }
                        
                        status = records[0]["Status"]
                        mappedData["Status"] = status
                        if status == "expired":
                            mappedData["Status"] = "active"
                        
                        if data["data"]["registrationStatus"] == False:
                            mappedData["Status"] = "pending"
                        
                        if status in ["sold","pending","to_parse","manual_expire"]:
                            mappedData["Status"] = status
                        
                        if status in ["sold","pending","manual_expire"]:
                            continue
                        data["data"]["status"] = mappedData["Status"]
                        self.db.connect()
                        self.db.recUpdate("fl_listings",mappedData,where)
                        self.db.disconnect()
                        
                        upsert = "update"
                        
                    else:
                        # insert
                        mappedData = self.mapColumnsInsert(data)
                        mappedData["updated_at"] = {"func":"now()"}
                        mappedData["Status"] = "to_parse"
                        
                        if data["data"]["registrationStatus"] == False:
                            mappedData["Status"] = "pending"
                        
                        if data["data"]["ltv"]["ltvStatus"] == 0:
                            mappedData["Status"] = "pending"
                        
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
                        
                    if scraperName == "url-scraper":
                        status = mappedData["Status"]
                        listingId = data["data"].get("listingId")
                        
                        registrationStatus = data["data"].get("registrationStatus",None)
                        
                        self.handleAtUrl(status,id,listingId,registrationStatus)
                
                    data["data"]["upsert"] = upsert
                    
                    data["data"]["id"] = id
                        
                print(data)
                
                # increase count
                
                self.logsProducer.produce(
                 {
                    "eventType":"listingCount",
                    "data":{
                            "countFor":upsert
                        }
                    }
                )
                # 
                
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