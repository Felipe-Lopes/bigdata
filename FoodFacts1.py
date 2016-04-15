from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Food Facts")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    kategori = int(fields[14])
    karbo = int(fields[100])
    return (kategori, karbo)

lines = sc.textFile("file:///SparkCourse/FoodFacts.csv")
rdd = lines.map(parseLine)
maksimal_karbo = rdd.map(lambda x: (x[0], x[1]))
sort = maksimal_karbo.reduceByKey(lambda x, y: max(x,y))
results = sort.collect()

for result in results:
    print result
