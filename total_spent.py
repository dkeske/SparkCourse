from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

orders = sc.textFile("file:///sparkcourse/customer-orders.csv")

customer_value = orders.map(lambda x: (int(x.split(',')[0]),float(x.split(',')[2])))

customer_total = customer_value.reduceByKey(lambda x,y: x+y)

total_customer = customer_total.map(lambda (x,y):(y,x)).sortByKey()

result = total_customer.collect()

for res in result:
	print "{:.2f}".format(res[0]) + ": " + str(res[1])