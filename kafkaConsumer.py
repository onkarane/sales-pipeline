from kafka import KafkaConsumer
from pymongo import MongoClient
import json

#set the consumer
consumer = KafkaConsumer(
    'sales',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

#set the mongo client
client = MongoClient('localhost', 27017)
sales = client.salesDB.get_collection('salesRecords')

#loop
for message in consumer:
    message = message.value
    print(message)
    sales.insert_one(message)


