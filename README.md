# Motor-Market

# services

### 1. Autotrader Graphql Api
    - port : 5000
    - this api accept auto trader listing id as input and return data about that listing
    - example :
        - auto trader url : https://autotrader.co.uk/car-details/202201030964186
        - the listing id in this url is -> 202201030964186
        - url -> http://{host/ip-address}:5000/autotrader/graphql?id=202201030964186&token={auth-token}

        - in above url, host is ip address of server
        - you can find the auth token inside .env file on server. (make sure you won't expose that token.)

        - above url will return the data which we currently using in our site. but if you need full raw graph-ql response then you should pass query parameter : type=all

        example :
            - http://{host/ip-address}:5000/autotrader/graphql?id=202201030964186&token={auth-token}&type=all

### 2. Redis
    - port 5001
    - for any caching related task this redis instance port should be used.

### 3. Pulsar
    - port 5002,5003
    - pulsar is used for data pipeline. for more info you can refer pulsar doc : https://pulsar.apache.org
    - 


## follow below steps to run all services.

- 1. clone this repo

- 2. install docker

- 3. docker-compose build

- 4. docker-compose up -d

## to stop all services

- docker-compose down

## to start all services

- docker-compose up


