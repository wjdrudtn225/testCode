from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("test1")
sc = SparkContext(conf=conf)

inRDD = sc.textFile("mart.txt")

itemRDD = inRDD.map(lambda line: line.split(",")).map(lambda x: x[1:])
martRDD = inRDD.map(lambda line: line.split(",")).map(lambda x: (x[0], x[1:])).flatMapValues(lambda x: x)

result = martRDD.combineByKey(lambda value: (value, 1),
                             lambda x, value: (x[0]+","+value, x[1] + 1),
                             lambda x, y: (x[0]+","+value, x[1] + y[1]))

print('result')
for line in result.collect():
        print(line)
