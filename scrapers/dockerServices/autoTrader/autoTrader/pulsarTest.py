import pulsar
import os

### produce message to topic

topic = "pulsar.test.topic"

uri = f'pulsar://pulsar'
print(uri)

client = pulsar.Client(uri)

producer = client.create_producer(topic)

for i in range(10):
    producer.send(('Hello-%d' % i).encode('utf-8'))

client.close()


## consume message from topic
client = pulsar.Client(uri)

consumer = client.subscribe(topic, f'{__name__}-subscription')

while True:
    msg = consumer.receive()
    try:
        print("Received message '{}' id='{}'".format(msg.data(), msg.message_id()))
        # Acknowledge successful processing of the message
        consumer.acknowledge(msg)
        
        break
    except:
        # Message failed to be processed
        consumer.negative_acknowledge(msg)

client.close()