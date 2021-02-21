import json
from PubSub import PubSubFactory, consumer

BACKEND = 'gcp'
FROM_TOPIC_ID = 'results-topic'
FROM_SUBSCRIPTION_ID = 'logger-sub'


def callback(key, value):
    value = json.loads(value.decode())
    print(f"Log Received\n\tkey:\t{key}\n\tvalue:\n{json.dumps(value, indent=2)}\n\n")

# create a consumer
pubsub = PubSubFactory(BACKEND)
consumer = pubsub.create_consumer(FROM_TOPIC_ID, callback, FROM_SUBSCRIPTION_ID)
print("Logger service started...")
