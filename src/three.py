from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("test1")
sc = SparkContext(conf=conf)

inRDD = sc.textFile("airpollution_2017.txt")
avg_2016 = sc.textFile("airpollution_2016_avg.txt") \
	.map(lambda line: line.split(",")) \
	.map(lambda line: (line[1], int(line[2])))

mapRDD = inRDD.map(lambda line: line.split(",")).map(lambda line: (line[1], int(line[3])))

result1 = mapRDD.combineByKey(lambda value: (value, 1),
                             lambda x, value: (x[0] + value, x[1] + 1),
                             lambda x, y: (x[0] + y[0], x[1] + y[1]))

avg_2017 = result1.map(lambda x: (x[0], x[1][0] / x[1][1]))

join_avg = avg_2017.join(avg_2016).mapValues(lambda x: (x[0]+x[1])/2)

for line in join_avg.collect():
        print(line)