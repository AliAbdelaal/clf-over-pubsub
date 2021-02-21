import io
import json
import torch

from MultiClassImageClassifier import Brain
from PubSub import PubSubFactory

BACKEND = 'gcp'
FROM_TOPIC_ID = 'images-topic'
# specific to google pubsub
FROM_SUBSCRIPTION_ID = 'clf_sub'
TO_TOPIC_ID = 'results-topic'
global brain, producer


def predict_img(key, value):
    global brain, producer
    img = torch.load(io.BytesIO(value))
    predictions = brain.predict_tensor(img)
    # print(f'predictions are: {json.dumps(predictions, indent=2)}')
    producer.push_msg(json.dumps(predictions).encode(), key=key.encode())
    print(f'got an image with shape {img.shape} and key {key}, classified and pushed.')
    print()

# create a brain object
brain = Brain()
# load the brain weights
brain.load_model('clf-checkpoint')
pubsub = PubSubFactory(BACKEND)

# create a producer to publish predictions
producer = pubsub.create_producer(TO_TOPIC_ID)

# create a consumer to load images
consumer = pubsub.create_consumer(FROM_TOPIC_ID, predict_img, FROM_SUBSCRIPTION_ID)
print("classifier consumer service started...")
print("classifier producer service started...")


