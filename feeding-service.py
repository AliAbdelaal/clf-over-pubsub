import io
import torch
import torchvision
from torchvision import transforms

from PubSub import PubSubFactory

BACKEND = 'gcp'
TOPIC_ID = 'images-topic'

# loading data 
test_set = torchvision.datasets.FashionMNIST(
    "./data", download=True, train=False, transform=transforms.Compose([transforms.ToTensor()]))

# create a producer object
pubsub = PubSubFactory(BACKEND)
producer = pubsub.create_producer(TOPIC_ID)
print("feeding service started...")

# start pushing images to the topic
for i, img in enumerate(test_set):
    img, label = img[0], img[1]
    # transform image tensor to bytes string
    with io.BytesIO() as buff:
        buff = io.BytesIO()
        torch.save(img, buff)
        buff.seek(0)
        key = f'img-{i}-label-{label}'
        producer.push_msg(buff.getvalue(), key=key.encode())
        print(f'pushed image with key {key}, labeled {test_set.classes[label]}.')
    q = input("cont:?")
    if q =='q':
        break