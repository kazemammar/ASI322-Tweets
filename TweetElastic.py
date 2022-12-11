import json
from datetime import datetime
from elasticsearch import Elasticsearch
es = Elasticsearch(hosts="http://elastic:changeme@localhost:9200/")

from kafka import KafkaConsumer
consumer = KafkaConsumer("worldcup")

while True:
        tweet = json.loads(next(iter(consumer)).value)

        resp = es.index(index="worldcup-tweet-collector", id=tweet["id"], document=tweet)
        print(resp['result'])

        # resp = es.get(index="test-index", id=i)
        # print(resp['_source'])