from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("test1")
sc = SparkContext(conf=conf)

inRDD = sc.textFile("airpollution_2017.txt")
result = inRDD.filter(lambda line: "wonju" not in line)

for line in result.collect():
    print(line)