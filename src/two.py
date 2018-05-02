from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("test1")
sc = SparkContext(conf=conf)

inRDD = sc.textFile("airpollution_2017.txt")
mapRDD = inRDD.map(lambda line: line.split(",")).map(lambda line: (line[4], int(line[3])))

result1 = mapRDD.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
result2 = mapRDD.combineByKey(lambda value: (value, 1),
                             lambda x, value: (x[0] + value, x[1] + 1),
                             lambda x, y: (x[0] + y[0], x[1] + y[1]))

average = result1.map(lambda x: (x[0], x[1][0] / x[1][1]))

for line in average.collect():
        print(line)
