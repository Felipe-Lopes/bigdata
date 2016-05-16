import re
import sys
from pyspark import SparkConf, SparkContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF

# Function for printing each element in RDD
def println(x):
    print x

# Boilerplate Spark stuff:
conf = SparkConf().setMaster("local[*]").setAppName("SparkTFIDF")
sc = SparkContext(conf = conf)

# Load documents (one per line).
rawData = sc.textFile("dataset.tsv")
#Make rawData to be lowercase
kecilRawData = rawData.map(lambda x: x.lower())
fields = kecilRawData.map(lambda x: x.split("\t"))
documents = fields.map(lambda x: x[2].split(" "))

documentId = fields.map(lambda x: x[0])

# Creating Hash table and TF table
hashingTF = HashingTF(10000000)
tf = hashingTF.transform(documents)

# Creating idf
tf.cache()
idf = IDF(minDocFreq=1).fit(tf)

# Calculate TF/IDF
tfidf = idf.transform(tf)

# Keyword yang akan dicari diubah ke Hash value <- Hash table di atas
# menyimpan Argument
kunci = str(sys.argv[1])
print "Anda mencari kata kunci " + kunci
#raw_input("Masukan kata kunci : ")
#Make kunci to be lowercase
keywordTF = hashingTF.transform([kunci.lower()])
keywordHashValue = int(keywordTF.indices[0])

# Temukan relevansinya dengan tabel tf-idf yang sudah dibuat
keywordRelevance = tfidf.map(lambda x: x[keywordHashValue])

zippedResults = keywordRelevance.zip(documentId)

print "Best document for keywords is:"
print zippedResults.max()