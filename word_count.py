from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)


lines = sc.textFile("file:///SparkCourse/Book.txt")

count = lines.flatMap(lambda x: x.split())
wordCount = count.countByValue()

for word, count in wordCount.items():
	cleanWord = word.encode('ascii', 'ignore')
	if (cleanWord):
		print cleanWord, count