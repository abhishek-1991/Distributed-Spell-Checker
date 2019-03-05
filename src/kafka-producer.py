from kafka import KafkaProducer
from kafka import TopicPartition
from json import loads
import glob

producer = KafkaProducer(bootstrap_servers='152.46.18.86:9092,152.46.18.71:9092,152.46.17.184:9092')

files = glob.glob("input/*.txt")
for fil in files:
	f = open(fil)

	for line in f:
		wordlist = line.split(" ")
		for word in wordlist:
			producer.send('words',key = fil.encode(),value=word.encode())

	producer.flush()
producer.close()
