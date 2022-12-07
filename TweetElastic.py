import json
from datetime import datetime
from elasticsearch import Elasticsearch
es = Elasticsearch(hosts="http://elastic:changeme@localhost:9200/")

from kafka import KafkaConsumer
consumer = KafkaConsumer("worldcup")

i=12
while True:
        tweet = json.loads(next(iter(consumer)).value)
        doc = {
            'author': 'kazem_ammar',
            'text': tweet,
            'timestamp': datetime.now(),
        }

        resp = es.index(index="test-index", id=i, document=doc)
        print(resp['result'])

        i+=1

        # resp = es.get(index="test-index", id=i)
        # print(resp['_source'])