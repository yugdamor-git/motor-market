
from abc import ABC,abstractmethod

class root_messenger(ABC):
    @abstractmethod
    def send_message(self,message):
        pass
    

class slack_messenger(root_messenger):
    
    def send_message(self,message):
        import requests
        import json

        url = "https://hooks.slack.com/services/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"

        payload = json.dumps({
        "text": message
        })
        headers = {
        'Content-type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        if response.text == "ok":
            return True
        else:
            return False

class messenger:
    def __init__(self):
        print("messenger init")
    
    def send_message(self,message,message_type="slack"):
        messenger = None
        if message_type == "slack":
            messenger = slack_messenger()
        if messenger != None:
            messenger.send_message(message)
            
            