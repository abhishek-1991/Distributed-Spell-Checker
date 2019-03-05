#!/usr/bin/python

from pyspark import SparkContext

sc = SparkContext()

wordsFile = sc.textFile("hdfs://node-master:9000/user/ajain28/input/doc5")

dictFile = sc.textFile("hdfs://node-master:9000/user/ajain28/dictonary/d1.txt")
dictWords = dictFile.collect()

words = wordsFile.flatMap(lambda line: line.split(" "))

misspelledWords = words.filter(lambda s: s not in dictWords)
misspelledWords.saveAsTextFile("hdfs://node-master:9000/user/ajain28/outcome.txt")
#misspelledWords.collect()
print "Number of misspelled: "
print misspelledWords.collect()
