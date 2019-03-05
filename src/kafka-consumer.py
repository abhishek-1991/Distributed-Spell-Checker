from kafka import KafkaConsumer
from kafka import TopicPartition
from json import loads

consumer  = KafkaConsumer(bootstrap_servers='152.46.18.86:9092,152.46.18.71:9092,152.46.17.184:9092',group_id='dic',auto_offset_reset='earliest',enable_auto_commit=True)
topic = TopicPartition('output',0)
consumer.assign([topic])

for msg in consumer:
	if "/" in msg.key:
                print(msg.key.split("/")[1]+"\t"+msg.value)
	else:
                print(msg.key+"\t"+msg.value)
