import os

# first stop the scraping system
stop_command = "docker-compose down"
os.system(stop_command)

# reboot the server
server_restart_command = "sudo reboot now"
os.system(server_restart_command)