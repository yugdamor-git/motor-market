import requests
import json
import os

class GraphqlApi:
    
    def __init__(self):
        self.residential_proxy = {
            "http":os.environ.get("RESIDENTIAL_PROXY"),
            "https":os.environ.get("RESIDENTIAL_PROXY")
        }
    
    def get_all_data_by_id(self,car_id,query_type):
        url = "https://www.autotrader.co.uk/at-graphql?opname=FPADataQuery"

        query = None

        price_data_query = """
        query FPADataQuery($advertId:String!,$searchOptions:SearchOptions){
            search{
                advert(advertId:$advertId,searchOptions:$searchOptions){
                id
                price
                }
            }
        }        
        """
        all_fields_query = """
        query FPADataQuery($advertId: String!,$numberOfImages: Int, $searchOptions: SearchOptions,$postcode: String) {
            search {
                advert(advertId: $advertId, searchOptions: $searchOptions) {
                id
                stockItemId
                isAuction
                hoursUsed
                serviceHistory
                title
                excludePreviousOwners
                advertisedLocations
                motExpiry
                heading {
                    title
                    subtitle
                    __typename
                }
                attentionGrabber
                rrp
                price
                priceCurrency
                priceExcludingFees
                suppliedPrice
                suppliedPriceExcludingFees
                priceOnApplication
                plusVatIndicated
                saving
                noAdminFees
                adminFee
                adminFeeInfoDescription
                dateOfRegistration
                homeDeliveryRegionCodes
                deliversToMyPostcode
                capabilities {
                    guaranteedPartEx {
                    enabled
                    __typename
                    }
                    marketExtensionHomeDelivery {
                    enabled
                    __typename
                    }
                    marketExtensionClickAndCollect {
                    enabled
                    __typename
                    }
                    marketExtensionCentrallyHeld {
                    enabled
                    __typename
                    }
                    sellerPromise {
                    enabled
                    __typename
                    }
                    __typename
                }
                collectionLocations {
                    locations {
                    ...CollectionLocationData
                    __typename
                    }
                    __typename
                }
                registration
                generation {
                    generationId
                    name
                    review {
                    ownerReviewsSummary {
                        aggregatedRating
                        countOfReviews
                        __typename
                    }
                    expertReviewSummary {
                        rating
                        reviewUrl
                        __typename
                    }
                    __typename
                    }
                    __typename
                }
                hasShowroomProductCode
                isPartExAvailable
                isPremium
                isFinanceAvailable
                isFinanceFullApplicationAvailable
                financeProvider
                financeDefaults {
                    term
                    mileage
                    depositAmount
                    __typename
                }
                retailerId
                hasClickAndCollect
                privateAdvertiser {
                    contact {
                    protectedNumber
                    email
                    __typename
                    }
                    location {
                    town
                    county
                    postcode
                    __typename
                    }
                    tola
                    __typename
                }
                advertiserSegment
                dealer {
                    ...DealerData
                    __typename
                }
                video {
                    url
                    preview
                    __typename
                }
                spin {
                    url
                    preview
                    __typename
                }
                imageList(limit: $numberOfImages) {
                    nextCursor
                    size
                    images {
                    url
                    templated
                    autotraderAllocated
                    __typename
                    }
                    __typename
                }
                priceIndicatorRating
                priceIndicatorRatingLabel
                priceDeviation
                mileageDeviation
                advertText
                mileage {
                    mileage
                    unit
                    __typename
                }
                plate
                year
                vehicleCheckId
                vehicleCheckStatus
                vehicleCheckSummary {
                    type
                    title
                    performed
                    writeOffCategory
                    checks {
                    key
                    failed
                    advisory
                    critical
                    warning
                    __typename
                    }
                    __typename
                }
                sellerName
                sellerType
                sellerProducts
                sellerLocation
                sellerLocationDistance {
                    unit
                    value
                    __typename
                }
                sellerContact {
                    phoneNumberOne
                    phoneNumberTwo
                    protectedNumber
                    byEmail
                    __typename
                }
                description
                colour
                manufacturerApproved
                insuranceWriteOffCategory
                owners
                vehicleCondition {
                    tyreCondition
                    interiorCondition
                    bodyCondition
                    __typename
                }
                specification {
                    operatingType
                    emissionClass
                    co2Emissions {
                    co2Emission
                    unit
                    __typename
                    }
                    topSpeed {
                    topSpeed
                    __typename
                    }
                    minimumKerbWeight {
                    weight
                    unit
                    __typename
                    }
                    endLayout
                    trailerAxleNumber
                    bedroomLayout
                    grossVehicleWeight {
                    weight
                    unit
                    __typename
                    }
                    capacityWeight {
                    weight
                    unit
                    __typename
                    }
                    liftingCapacity {
                    weight
                    unit
                    __typename
                    }
                    operatingWidth {
                    width
                    unit
                    __typename
                    }
                    maxReach {
                    length
                    unit
                    __typename
                    }
                    wheelbase
                    berth
                    bedrooms
                    engine {
                    power {
                        enginePower
                        unit
                        __typename
                    }
                    sizeLitres
                    sizeCC
                    manufacturerEngineSize
                    __typename
                    }
                    exteriorWidth {
                    width
                    unit
                    __typename
                    }
                    exteriorLength {
                    length
                    unit
                    __typename
                    }
                    exteriorHeight {
                    height
                    unit
                    __typename
                    }
                    capacityWidth {
                    width
                    unit
                    __typename
                    }
                    capacityLength {
                    length
                    unit
                    __typename
                    }
                    capacityHeight {
                    height
                    unit
                    __typename
                    }
                    seats
                    axleConfig
                    ulezCompliant
                    doors
                    bodyType
                    cabType
                    rawBodyType
                    fuel
                    transmission
                    style
                    subStyle
                    make
                    model
                    trim
                    vehicleCategory
                    optionalFeatures {
                    description
                    category
                    __typename
                    }
                    standardFeatures {
                    description
                    category
                    __typename
                    }
                    driverPosition
                    battery {
                    capacity {
                        capacity
                        unit
                        __typename
                    }
                    usableCapacity {
                        capacity
                        unit
                        __typename
                    }
                    range {
                        range
                        unit
                        __typename
                    }
                    charging {
                        quickChargeTime
                        chargeTime
                        __typename
                    }
                    __typename
                    }
                    techData {
                    co2Emissions
                    fuelConsumptionCombined
                    fuelConsumptionExtraUrban
                    fuelConsumptionUrban
                    insuranceGroup
                    minimumKerbWeight
                    zeroToSixtyMph
                    zeroToSixtyTwoMph
                    cylinders
                    valves
                    enginePower
                    topSpeed
                    engineTorque
                    vehicleHeight
                    vehicleLength
                    vehicleWidth
                    wheelbase
                    fuelTankCapacity
                    grossVehicleWeight
                    luggageCapacitySeatsDown
                    bootspaceSeatsUp
                    minimumKerbWeight
                    vehicleWidthInclMirrors
                    maxLoadingWeight
                    standardFeatures {
                        description
                        category
                        __typename
                    }
                    __typename
                    }
                    annualTax {
                    standardRate
                    __typename
                    }
                    oemDrivetrain
                    bikeLicenceType
                    derivative
                    derivativeId
                    __typename
                }
                stockType
                versionNumber
                tradeLifecycleStatus
                condition
                finance {
                    monthlyPayment
                    representativeApr
                    __typename
                }
                locationArea(postcode: $postcode) {
                    code
                    region
                    areaOfInterest {
                    postCode
                    manufacturerCodes
                    __typename
                    }
                    __typename
                }
                reservation {
                    status
                    eligibility
                    feeCurrency
                    feeInFractionalUnits
                    __typename
                }
                __typename
                }
                __typename
            }
            }

            fragment DealerData on Dealer {
            dealerId
            distance
            stockLevels {
                atStockCounts {
                car
                van
                __typename
                }
                __typename
            }
            assignedNumber {
                number
                __typename
            }
            awards {
                isWinner2018
                isWinner2019
                isWinner2020
                isWinner2021
                isFinalist2018
                isFinalist2019
                isFinalist2020
                isFinalist2021
                isHighlyRated2018
                isHighlyRated2019
                isHighlyRated2020
                isHighlyRated2021
                __typename
            }
            branding {
                accreditations {
                name
                __typename
                }
                brands {
                name
                imageUrl
                __typename
                }
                __typename
            }
            capabilities {
                instantMessagingChat {
                enabled
                provider
                __typename
                }
                instantMessagingText {
                enabled
                provider
                overrideSmsNumber
                __typename
                }
                __typename
            }
            reviews {
                numberOfReviews
                overallReviewRating
                __typename
            }
            location {
                addressOne
                addressTwo
                town
                county
                postcode
                latLong
                __typename
            }
            marketing {
                profile
                brandingBanner {
                href
                __typename
                }
                __typename
            }
            media {
                email
                dealerWebsite {
                href
                __typename
                }
                phoneNumber1
                phoneNumber2
                protectedNumber
                __typename
            }
            name
            servicesOffered {
                sellerPromise {
                monthlyWarranty
                minMOTAndService
                daysMoneyBackGuarantee
                moneyBackRemoteOnly
                __typename
                }
                services
                products
                safeSelling {
                bulletPoints
                paragraphs
                __typename
                }
                homeDelivery {
                bulletPoints
                paragraphs
                deliveryCostPerMile
                deliveryCostFlatFee
                freeDeliveryRadiusInMiles
                __typename
                }
                videoWalkAround {
                bulletPoints
                paragraphs
                __typename
                }
                clickAndCollect {
                bulletPoints
                paragraphs
                __typename
                }
                buyOnline {
                bulletPoints
                paragraphs
                __typename
                }
                nccApproved
                isHomeDeliveryProductEnabled
                isPartExAvailable
                hasSafeSelling
                hasHomeDelivery
                hasVideoWalkAround
                additionalLinks {
                title
                href
                __typename
                }
                __typename
            }
            __typename
            }

            fragment CollectionLocationData on CollectionLocation {
            id
            dealerId
            name
            town
            postcode
            distance
            geoCoordinate {
                latitude
                longitude
                __typename
            }
            badges {
                type
                label
                __typename
            }
            __typename
            }
        """
        required_data_query = """
        query FPADataQuery($advertId:String!,$searchOptions:SearchOptions){
            search{
                advert(advertId:$advertId,searchOptions:$searchOptions){
                    id
                    colour
                    adminFee
                    imageList{
                        images{
                           url 
                        }
                    }
                    registration
                    year
                    price
                    title
                    priceIndicatorRatingLabel
                    mileage{
                        mileage
                    }
                    sellerContact{
                        phoneNumberOne
                    }
                    vehicleCheckSummary{
                        writeOffCategory
                    }
                    specification{
                        doors
                        wheelbase
                        cabType
                        seats
                        make
                        model
                        trim
                        vehicleCategory
                        ulezCompliant
                        transmission
                        rawBodyType
                        engine{
                            sizeCC
                        }
                        fuel
                    }
                    dealer{
                        dealerId
                        name
                        location{
                            postcode
                        }                
                    }
                }
            }
        }
        """

        if query_type == "all":
            query = all_fields_query
        else:
            query = required_data_query

        headers = {
            'authority': 'www.autotrader.co.uk',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'x-sauron-app-name': 'sauron-adverts-app',
            'x-sauron-app-version': '1',
            'sec-ch-ua-mobile': '?0',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36',
            'content-type': 'application/json',
            'accept': '*/*',
            'sec-ch-ua-platform': '"Windows"',
            'sec-ch-ua': '"Google Chrome";v="95", "Chromium";v="95", ";Not A Brand";v="99"',
            'origin': 'https://www.autotrader.co.uk',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://www.autotrader.co.uk/',
            'accept-language': 'en-US,en;q=0.9',

        }
        
        payload = [
        {
            "operationName": "FPADataQuery",
            "variables": {
            "advertId": f'{car_id}',
            "numberOfImages": 100,
            "searchOptions": {
                "advertisingLocations": [
                "at_cars"
                ],
                "postcode": "wf160pr",
                "collectionLocationOptions": {
                "searchPostcode": "wf160pr"
                },
                "channel": "cars"
            },
            "postcode": "wf160pr"
            },
            "query": query 
        }
        ]

        
        response = requests.post(url, headers=headers, data=json.dumps(payload),proxies=self.residential_proxy)

        return response

    def extract_required_data_from_json(self, response):
        json_data = response.json()[0]["data"]["search"]["advert"]
        temp_car_data = {}
        tmp_imgs = []
        dealer = json_data["dealer"]
        try:
            temp_car_data["dealer_name"] = dealer['name']
        except:
            temp_car_data["dealer_name"] = ""
        try:
            temp_car_data["dealer_number"] = json_data["sellerContact"]['phoneNumberOne']
        except:
            temp_car_data["dealer_number"] = ""
        try:
            temp_car_data["dealer_location"] = dealer['location']['postcode'].replace(
                " ", "").strip().upper()
            temp_car_data["location"] = temp_car_data["dealer_location"]
        except:
            temp_car_data["dealer_location"] = ""
            temp_car_data["location"] = ""
        try:
            temp_car_data["dealer_id"] = dealer['dealerId']
        except:
            temp_car_data["dealer_id"] = ""
        ##########################################################################################
        specification = json_data["specification"]
        
        try:
            temp_car_data["wheelbase"] = specification["wheelbase"]
        except:
            temp_car_data["wheelbase"] =  None
        
        try:
            temp_car_data["cabtype"] = specification["cabType"]
        except:
            temp_car_data["cabtype"] = None
        
        try:
            temp_car_data['make'] = specification['make']
        except:
            temp_car_data['make'] = None

        try:
            temp_car_data['model'] = specification['model']
        except:
            temp_car_data['model'] = None

        try:
            temp_car_data['engine_cylinders'] = int(specification["engine"]["sizeCC"])
        except:
            temp_car_data['engine_cylinders'] = None

        try:
            temp_car_data['built'] = json_data["year"]
        except:
            temp_car_data['built'] = None

        try:
            temp_car_data['seats'] = specification["seats"]
        except:
            temp_car_data['seats'] = None

        try:
            temp_car_data['mileage'] = json_data["mileage"]["mileage"]
        except:
            temp_car_data['mileage'] = None

        try:
            temp_car_data['fuel'] = specification["fuel"]
        except:
            temp_car_data['fuel'] = None

        try:
            temp_car_data['registration'] = json_data["registration"]
        except:
            temp_car_data['registration'] = None

        try:
            temp_car_data['writeOffCategory'] = json_data["vehicleCheckSummary"]["writeOffCategory"]
        except:
            temp_car_data['writeOffCategory'] = None

        try:
            temp_car_data['doors'] = specification['doors']
        except:
            temp_car_data['doors'] = None

        try:
            temp_car_data['body_style'] = specification['rawBodyType']
        except:
            temp_car_data['body_style'] = None

        try:
            temp_car_data['price'] = int(json_data['price'])
        except:
            temp_car_data['price'] = None

        try:
            temp_car_data["price_indicator"] = json_data["priceIndicatorRatingLabel"]
        except:
            temp_car_data["price_indicator"] = None

        try:
            if json_data["adminFee"] != None:
                temp_car_data['admin_fees'] = self.extract_admin_fees(
                    json_data["adminFee"])
            else:
                temp_car_data['admin_fees'] = 0
        except:
            temp_car_data['admin_fees'] = 0

        temp_car_data["price"] = temp_car_data["price"] + temp_car_data["admin_fees"]

        try:
            temp_car_data['trim'] = specification['trim']
        except:
            temp_car_data['trim'] = None
        
        try:
            temp_car_data["vehicle_type"] = specification["vehicleCategory"].lower().strip()
        except:
            temp_car_data["vehicle_type"] = None
        
        try:
            temp_car_data["emission_scheme"] = specification["ulezCompliant"]
        except:
            temp_car_data["emission_scheme"] = 0

        try:
            temp_car_data['transmission'] = specification['transmission']
        except:
            temp_car_data['transmission'] = None

        try:
            temp_car_data['product_url'] = "https://www.autotrader.co.uk/{}-details/{}".format( temp_car_data["vehicle_type"],json_data['id'].strip())
        except:
            temp_car_data['product_url'] = None

        ##########################################################################################
        for img in json_data["imageList"]["images"]:
            tmp_imgs.append(img["url"])

        try:
            if str(temp_car_data["dealer_id"]) in ["10017779", "27396"]:
                tmp_imgs.pop(0)
        except:
            pass

        temp_car_data["images"] = tmp_imgs

        return temp_car_data



    def extract_admin_fees(self, admin_f):
        try:
            temp = admin_f.strip()
            temp = temp.replace("£", "")
            temp_list = []
            for i in temp:
                try:
                    int(i)
                    temp_list.append(i)
                except:
                    pass
            temp = "".join(temp_list)
            return int(temp)
        except:
            return 0
    
    def fetch(self,car_id,query_type):
        response = self.get_all_data_by_id(car_id,query_type)
        if query_type != "all":
            data = self.extract_required_data_from_json(response)
        else:
            data = response.json()[0]["data"]["search"]["advert"]
        return data