from elasticsearch import Elasticsearch
from base64 import encodebytes
# Password for the 'elastic' user generated by Elasticsearch
ELASTIC_PASSWORD = "kasi123"
from datetime import datetime

# Create the client instance
client = Elasticsearch(
    "https://localhost:9200",
    verify_certs=False,
    basic_auth=("elastic", ELASTIC_PASSWORD)
)

# Successful response!
print(client.info())
# {'name': 'instance-0000000000', 'cluster_name': ..

doc = {
    'author': 'author_name',
    'text': 'Interensting content...',
    'timestamp': datetime.now(),
}
resp = client.index(index="test-index", id=1, document=doc)
print(resp['result'])