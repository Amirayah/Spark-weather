import sys
from __future__ import print_function
from operator import add
from pyspark import SparkContext, SparkConf

#read data from text file and split each line into words, using Spark contex
#sc, we read a text file
input_file  = sc.textFile("C:/Users/roger/PYSPARK/wordc.txt")

#count the occurence of each word, we can split each line of input
#using space"" as separator, and we map each word to a tuple (word,1)
map = input_file.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1))

#Reduce all the words based on Key. Here a, b are values 
#and for the same key, values are reduced to a+b.
counts = map.reduceByKey(lambda a,b: a + b)

#save the counts to output
counts.saveAsTextFile("C:/Users/roger/PYSPARK/wordc.txt/output")
