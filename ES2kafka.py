from elasticsearch import Elasticsearch
import logging
import json
from pykafka import KafkaClient


####get data from ES
es = Elasticsearch(["localhost:9200"])
res=es.search(index="ddos-2016.09.26", body={"query": {"match_all": {}}})
data=res['hits']['hits']


#for source in data:
#    print source['_source']



####

client = KafkaClient(hosts="172.0.0.1:9092,172.0.0.2:9092")
input=raw_input('Please enter the name of the topic:')
topic = client.topics[input]

        
with topic.get_sync_producer() as producer:
    for source in data:
        data_str=json.dumps(source['_source'])
        producer.produce(data_str)
