from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
import json
import os

os.system("/opt/kafka/kafka_2.12-1.0.1/bin/kafka-topics.sh --zookeeper \"152.46.18.86:2181,152.46.18.71:2181,152.46.17.184:2181\" --delete --topic \"output\" > /dev/null 2>&1")
os.system("/opt/kafka/kafka_2.12-1.0.1/bin/kafka-topics.sh --zookeeper \"152.46.18.86:2181,152.46.18.71:2181,152.46.17.184:2181\" --delete --topic \"words\" > /dev/null 2>&1")

producer = KafkaProducer(bootstrap_servers='152.46.18.86:9092,152.46.18.71:9092,152.46.17.184:9092')
producer.send('output',key = b'File Name',value = b'Wrong Words')
producer.send('words',key = b'start',value = b'start')

d = open('d1.txt')
dic = list()

for w in d:
        dic.append(w.strip().lower())

def handler(message):
	records = message.collect()
	for record in records:
		if record[1].strip().lower() not in dic:
			producer.send('output',key = record[0].encode(),value = record[1].encode())
			producer.flush()

def main():
	sc = SparkContext(appName="PythonSparkStreamingKafka")
	sc.setLogLevel("FATAL")
	ssc = StreamingContext(sc,60)
	kafkaStream = KafkaUtils.createDirectStream(ssc, ['words'], {'metadata.broker.list':'152.46.18.86:9092,152.46.18.71:9092,152.46.17.184:9092'})
	kafkaStream.foreachRDD(handler)

	ssc.start()  
	ssc.awaitTermination()

if __name__ == "__main__":
	main()
