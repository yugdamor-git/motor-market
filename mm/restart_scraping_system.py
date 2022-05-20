import os
import time

# stop current running services
stop_command = "docker-compose down"
os.system(stop_command)

print(f'all services are down...')
# wait for 10 seconds..
delay = 10
for t in range(0,delay):
    print(f'restarting in {delay - t} seconds')

# start parent services
start_parent_command = "docker-compose up -d pulsar mongodb api redis ftps ftp-scraper"
os.system(start_parent_command)

print(f'parent services are up and running...')

# start child services
start_child_services_command = "docker-compose --compatibility up -d logs fl-listings-find fl-listings-find-at-urls fl-listings-update fl-listings-update-at-urls at-urls-update fl-listings-insert fl-listings-insert-at-urls fl-listingphotos-insert fl-listingphotos-insert-at-urls listing-scraper listing-scraper-at-urls transform transform-at-urls pre-validation pre-validation-at-urls makemodel-prediction makemodel-prediction-at-urls seat-prediction seat-prediction-at-urls image-prediction image-prediction-at-urls post-validation post-validation-at-urls numberplate-prediction numberplate-prediction-at-urls post-calculation post-calculation-at-urls image-generation image-generation-at-urls at-urls-scraper"
os.system(start_child_services_command)

print(f'child services are up and running...')