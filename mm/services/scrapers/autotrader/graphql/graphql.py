import os
import json

class Graphql:
    def __init__(self):
        
        self.residentialProxy = os.environ.get("RESIDENTIAL_PROXY")
        self.datacenterProxy = os.environ.get("DATACENTER_PROXY")
        
        
        self.proxy = {
            "http":self.residentialProxy,
            "https":self.residentialProxy
        }
        
        self.url = "https://www.autotrader.co.uk/at-graphql?opname=FPADataQuery"
        
        self.priceFieldQuery = """
        query FPADataQuery($advertId:String!,$searchOptions:SearchOptions){
            search{
                advert(advertId:$advertId,searchOptions:$searchOptions){
                id
                price
                adminFee
                tradeLifecycleStatus
                }
            }
        }        
        """
        
        self.all_fields_query = """
        query FPADataQuery($advertId:String!,$searchOptions:SearchOptions){
            search{
                advert(advertId:$advertId,searchOptions:$searchOptions){
                id
                stockItemId
                isAuction
                hoursUsed
                serviceHistory
                title
                excludePreviousOwners
                advertisedLocations
                motExpiry
                heading{
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
                capabilities{
                    guaranteedPartEx{
                    enabled
                    __typename
                    }
                    marketExtensionHomeDelivery 
                    enabled
                    __typename
                    }
                    marketExtensionClickAndCollect{
                    enabled
                    __typename
                    }
                    marketExtensionCentrallyHeld{
                    enabled
                    __typename
                    }
                    sellerPromise{
                    enabled
                    __typename
                    }
                    __typename
                }
                collectionLocations{
                    locations{
                    ...CollectionLocationData
                    __typename
                    }
                    __typename
                }
                registration
                generation{
                    generationId
                    name
                    review{
                    ownerReviewsSummary {
                        aggregatedRating
                        countOfReviews
                        __typename
                    }
                    expertReviewSummary{
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
                financeDefaults{
                    term
                    mileage
                    depositAmount
                    __typename
                }
                retailerId
                hasClickAndCollect
                privateAdvertiser{
                    contact{
                    protectedNumber
                    email
                    __typename
                    }
                    location{
                    town
                    county
                    postcode
                    __typename
                    }
                    tola
                    __typename
                }
                advertiserSegment
                dealer{
                    ...DealerData
                    __typename
                }
                video{
                    url
                    preview
                    __typename
                }
                spin{
                    url
                    preview
                    __typename
                }
                imageList(){
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
                tradeLifecycleStatus
                versionNumber
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
        
        self.requiredFieldsQuery = """
        query FPADataQuery($advertId:String!,$searchOptions:SearchOptions){
            search{
                advert(advertId:$advertId,searchOptions:$searchOptions){
                    id
                    colour
                    adminFee
                    tradeLifecycleStatus
                    vehicleCheckStatus
                    dateOfRegistration
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
    
        self.headers = {
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
        
    def getPayload(self,carId,query):
        
        payload = [
        {
            "operationName": "FPADataQuery",
            "variables": {
            "advertId": f'{carId}',
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
        
        return payload
    
    def extractValidatorDataFromJson(self, response:json) -> dict:
        
        jsonData = response[0]["data"]["search"]["advert"]
        
        carData = {}

        carData["price"] = jsonData.get("price",None)
        
        carData["tradeLifecycleStatus"] = jsonData.get("tradeLifecycleStatus")
        
        carData["adminFee"] = jsonData.get("adminFee",0)
        
        return carData
        
        
        
    def extractDataFromJson(self, response:json) -> dict:
        
        jsonData = response[0]["data"]["search"]["advert"]
        
        carData = {}
        
        dealer = jsonData["dealer"]
        
        carData["dealerName"] = dealer.get("name",None)
        
        sellerContact = jsonData.get("sellerContact",None)
        
        carData["dealerNumber"] = sellerContact.get("phoneNumberOne")
        
        dealerLocation = dealer.get("location",None)
        
        carData["dealerLocation"] = dealerLocation.get("postcode",None)
        
        carData["location"] = carData["dealerLocation"]
        
        carData["dealerId"] = dealer.get("dealerId",None)
        
        specification = jsonData.get("specification",None)
        
        carData["wheelBase"] = specification.get("wheelbase",None)
        
        carData["cabType"] = specification.get("cabType",None)
        
        carData["make"] = specification.get("make",None)
        
        carData["model"] = specification.get("model",None)
        
        engine = specification.get("engine",None)
        
        carData["engineCylinders"] = engine.get("sizeCC",None)
        
        carData["built"] = jsonData.get("year",None)
        
        carData["seats"] = specification.get("seats",None)
        
        mileage = jsonData.get("mileage",None)
        
        carData["mileage"] = mileage.get("mileage",None)
        
        carData["fuel"] = specification.get("fuel",None)
        
        carData["registration"] = jsonData.get("registration",None)
        
        vehicleCheckSummary = jsonData.get("vehicleCheckSummary",None)
        
        carData["writeOffCategory"] = vehicleCheckSummary.get("writeOffCategory")
        
        carData["doors"] = specification.get("doors",None)
        
        carData["bodyStyle"] = specification.get("rawBodyType",None)
        
        carData["price"] = jsonData.get("price",None)
        
        carData["priceIndicator"] = jsonData.get("priceIndicatorRatingLabel",None)

        carData["adminFee"] = jsonData.get("adminFee")
        
        carData["trim"] = specification.get("trim",None)
        
        carData["vehicleType"] = specification.get("vehicleCategory",None)
        
        carData["emissionScheme"] = specification.get("ulezCompliant",None)
        
        carData["transmission"] = specification.get("transmission",None)
        
        carData["tradeLifecycleStatus"] = jsonData.get("tradeLifecycleStatus",None)
        
        carData["registration_date"] = jsonData.get("dateOfRegistration",None)
        
        carData["vehicleCheckStatus"] = jsonData.get("vehicleCheckStatus",None)
        
        carData["id"] = jsonData.get("id",None)
        
        imageList =jsonData.get("imageList",None)
        
        carData["images"] = imageList.get("images",[])
        
        carData["url"] = f'https://www.autotrader.co.uk/{carData["vehicleType"]}-details/{carData["id"]}'

        return carData