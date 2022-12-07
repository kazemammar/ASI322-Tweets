from datetime import datetime
from elasticsearch import Elasticsearch
es = Elasticsearch(hosts="http://elastic:changeme@localhost:9200/")

doc = {
    'author': 'kazem_ammar00',
    'text': 'Bonjour contdhkajshdjkashkdaent...',
    'timestamp': datetime.now(),
}
resp = es.index(index="test-index", id=2, document=doc)
print(resp['result'])

resp = es.get(index="test-index", id=1)
print(resp['_source'])