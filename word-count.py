from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def normalizeWords(text):
    return text.lower().replace('.', '').replace(',', '').replace('(', '').replace(')', '').replace('"', '').replace('!', '').replace('?', '').replace(':','')  # Remove punctuation

def normalizeWordsWithRe(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

input = sc.textFile("Book.txt")
words = input.flatMap(normalizeWordsWithRe)
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

wordCounts = wordCounts.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)

results = wordCounts.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if word:
        print(word.decode() + ":\t\t" + count)
