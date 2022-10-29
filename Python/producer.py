from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
import json

class Producer:
    def __init__(self):
        self.admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='kafka_')
        new_topic = NewTopic(name="streaming", num_partitions=2, replication_factor=1)
        self.admin_client.create_topics(new_topics= [new_topic])
        self.producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer= self.serializer)

    def serializer(self, data):
        return json.dumps(data).encode('utf-8')

    def produce(self,data):
        self.producer.send("streaming", data)
