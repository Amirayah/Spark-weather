import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

	sc = SparkContext("local", "PySpar word count")
	
	words = 
sc.textFile("C:\Users\roger\PYSPARK\wordc.txt").flatMap(lambda 
line: line.split(" "))

	wordCounts = words.map(lambda word: (word,1)).reduceByKey(lambda 
a,b: a +b)

	wordCounts.saveAsTextFile("C:\Users\roger\PYSPARK\output")
