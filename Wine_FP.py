from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Know Quality of Wine-Red")
sc = SparkContext(conf = conf)

#List Fields
#0 Fixed Acidity
#1 Volatile Acidity
#2 Citrid Acid
#3 Residual Sugar
#4 Chlorides
#5 Free Sulfur Dioxide
#6 Total Sulfur Dioxide
#7 Density
#8 PH
#9 Sulphates
#10 Alcohol
#11 Quality

#ParseLine
def fixedAcidity(line):
    fields = line.split(';')
    fa = float(fields[0])
    quality = int(fields[11])
    return (quality, fa)
    
def volatileAcidity(line):
    fields = line.split(';')
    va = float(fields[1])
    quality = int(fields[11])
    return (quality, va)
    
def citridAcid(line):
    fields = line.split(';')
    ca = float(fields[2])
    quality = int(fields[11])
    return (quality, ca)
    
def residualSugar(line):
    fields = line.split(';')
    rs = float(fields[3])
    quality = int(fields[11])
    return (quality, rs)

def chlorides(line):
    fields = line.split(';')
    chlor = float(fields[4])
    quality = int(fields[11])
    return (quality, chlor)
    
def freeSulfurDioxide(line):
    fields = line.split(';')
    fsd = float(fields[5])
    quality = int(fields[11])
    return (quality, fsd)
    
def totalSulfurDioxide(line):
    fields = line.split(';')
    tsd = float(fields[6])
    quality = int(fields[11])
    return (quality, tsd)
    
def density(line):
    fields = line.split(';')
    density = float(fields[7])
    quality = int(fields[11])
    return (quality, density)

def ph(line):
    fields = line.split(';')
    ph = float(fields[8])
    quality = int(fields[11])
    return (quality, ph)
    
def sulphates(line):
    fields = line.split(';')
    sulph = float(fields[9])
    quality = int(fields[11])
    return (quality, sulph)
    
def alcohol(line):
    fields = line.split(';')
    alcohol = float(fields[10])
    quality = int(fields[11])
    return (quality, alcohol)
    
#Read the dataset
lines = sc.textFile("file:///SparkCourse/wine/winequality-red.csv")

#Get the data based by parseline
rdd_fa = lines.map(fixedAcidity)
rdd_va = lines.map(volatileAcidity)
rdd_ca = lines.map(citridAcid)
rdd_rs = lines.map(residualSugar)
rdd_chlor = lines.map(chlorides)
rdd_fsd = lines.map(freeSulfurDioxide)
rdd_tsd = lines.map(totalSulfurDioxide)
rdd_density = lines.map(density)
rdd_ph = lines.map(ph)
rdd_sulph = lines.map(sulphates)
rdd_alcohol = lines.map(alcohol)


#Find the average by quality
totalsByQuality_fa = rdd_fa.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByQuality_fa = totalsByQuality_fa.mapValues(lambda x: x[0] / x[1])
sort_fa = averagesByQuality_fa.map(lambda (x,y):(x,y)).sortByKey()
results_fa = sort_fa.collect()

totalsByQuality_va = rdd_va.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByQuality_va = totalsByQuality_va.mapValues(lambda x: x[0] / x[1])
sort_va = averagesByQuality_va.map(lambda (x,y):(x,y)).sortByKey()
results_va = sort_va.collect()

totalsByQuality_ca = rdd_ca.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByQuality_ca = totalsByQuality_ca.mapValues(lambda x: x[0] / x[1])
sort_ca = averagesByQuality_ca.map(lambda (x,y):(x,y)).sortByKey()
results_ca = sort_ca.collect()

totalsByQuality_rs = rdd_rs.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByQuality_rs = totalsByQuality_rs.mapValues(lambda x: x[0] / x[1])
sort_rs = averagesByQuality_rs.map(lambda (x,y):(x,y)).sortByKey()
results_rs = sort_rs.collect()

totalsByQuality_chlor = rdd_chlor.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByQuality_chlor = totalsByQuality_chlor.mapValues(lambda x: x[0] / x[1])
sort_chlor = averagesByQuality_chlor.map(lambda (x,y):(x,y)).sortByKey()
results_chlor = sort_chlor.collect()

totalsByQuality_fsd = rdd_fsd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByQuality_fsd = totalsByQuality_fsd.mapValues(lambda x: x[0] / x[1])
sort_fsd = averagesByQuality_fsd.map(lambda (x,y):(x,y)).sortByKey()
results_fsd = sort_fsd.collect()

totalsByQuality_tsd = rdd_tsd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByQuality_tsd = totalsByQuality_tsd.mapValues(lambda x: x[0] / x[1])
sort_tsd = averagesByQuality_tsd.map(lambda (x,y):(x,y)).sortByKey()
results_tsd = sort_tsd.collect()

totalsByQuality_density = rdd_density.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByQuality_density = totalsByQuality_density.mapValues(lambda x: x[0] / x[1])
sort_density = averagesByQuality_density.map(lambda (x,y):(x,y)).sortByKey()
results_density = sort_density.collect()

totalsByQuality_ph = rdd_ph.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByQuality_ph = totalsByQuality_ph.mapValues(lambda x: x[0] / x[1])
sort_ph = averagesByQuality_ph.map(lambda (x,y):(x,y)).sortByKey()
results_ph = sort_ph.collect()

totalsByQuality_sulph = rdd_sulph.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByQuality_sulph = totalsByQuality_sulph.mapValues(lambda x: x[0] / x[1])
sort_sulph = averagesByQuality_sulph.map(lambda (x,y):(x,y)).sortByKey()
results_sulph = sort_sulph.collect()

totalsByQuality_alcohol = rdd_alcohol.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByQuality_alcohol = totalsByQuality_alcohol.mapValues(lambda x: x[0] / x[1])
sort_alcohol = averagesByQuality_alcohol.map(lambda (x,y):(x,y)).sortByKey()
results_alcohol = sort_alcohol.collect()

#Print the Results
print "Quality",
print "\tFixed Acidity"
count = 0
for result in results_fa:
    print result[0] ,
    print "\t\t{:.2f}".format(result[1]),
    if count == 0 :
        spoint = result[1]
        count += 1
    print "\n",
    epoint = result[1]

if epoint > spoint :
    print "probably down-up"
else :
    print "probably up-down"
    
print "\n"        
print "Quality",
print "\tVolatile Acidity"
count = 0
for result in results_va:
    print result[0] ,
    print "\t\t{:.2f}".format(result[1]),
    if count == 0 :
        spoint = result[1]
        count += 1
    print "\n",
    epoint = result[1]

if epoint > spoint :
    print "probably down-up"
else :
    print "probably up-down"

print "\n"    
print "Quality",
print "\tCitrid Acid"
count = 0
for result in results_ca:
    print result[0] ,
    print "\t\t{:.2f}".format(result[1]),
    if count == 0 :
        spoint = result[1]
        count += 1
    print "\n",
    epoint = result[1]

if epoint > spoint :
    print "probably down-up"
else :
    print "probably up-down"

print "\n"
print "Quality",
print "\tResidual Sugar"
count = 0
for result in results_rs:
    print result[0] ,
    print "\t\t{:.2f}".format(result[1]),
    if count == 0 :
        spoint = result[1]
        count += 1
    print "\n",
    epoint = result[1]

if epoint > spoint :
    print "probably down-up"
else :
    print "probably up-down"
    
print "\n"
print "Quality",
print "\tChlorides"
count = 0
for result in results_chlor:
    print result[0] ,
    print "\t\t{:.2f}".format(result[1]),
    if count == 0 :
        spoint = result[1]
        count += 1
    print "\n",
    epoint = result[1]

if epoint > spoint :
    print "probably down-up"
else :
    print "probably up-down"

print "\n"    
print "Quality",
print "\tFree Sulfur Dioxide"
count = 0
for result in results_fsd:
    print result[0] ,
    print "\t\t{:.2f}".format(result[1]),
    if count == 0 :
        spoint = result[1]
        count += 1
    print "\n",
    epoint = result[1]

if epoint > spoint :
    print "probably down-up"
else :
    print "probably up-down"

print "\n"
print "Quality",
print "\tTotal Sulfur Dioxide"
count = 0
for result in results_tsd:
    print result[0] ,
    print "\t\t{:.2f}".format(result[1]),
    if count == 0 :
        spoint = result[1]
        count += 1
    print "\n",
    epoint = result[1]

if epoint > spoint :
    print "probably down-up"
else :
    print "probably up-down"
    
print "\n"
print "Quality",
print "\tDensity"
count = 0
for result in results_density:
    print result[0] ,
    print "\t\t{:.2f}".format(result[1]),
    if count == 0 :
        spoint = result[1]
        count += 1
    print "\n",
    epoint = result[1]

if epoint > spoint :
    print "probably down-up"
else :
    print "probably up-down"
    
print "\n"
print "Quality",
print "\tPH"
count = 0
for result in results_ph:
    print result[0] ,
    print "\t\t{:.2f}".format(result[1]),
    if count == 0 :
        spoint = result[1]
        count += 1
    print "\n",
    epoint = result[1]

if epoint > spoint :
    print "probably down-up"
else :
    print "probably up-down"

print "\n"
print "Quality",
print "\tSulphates"
count = 0
for result in results_sulph:
    print result[0] ,
    print "\t\t{:.2f}".format(result[1]),
    if count == 0 :
        spoint = result[1]
        count += 1
    print "\n",
    epoint = result[1]

if epoint > spoint :
    print "probably down-up"
else :
    print "probably up-down"

print "\n"    
print "Quality",
print "\tAlcohol"
count = 0
for result in results_alcohol:
    print result[0] ,
    print "\t\t{:.2f}".format(result[1]),
    if count == 0 :
        spoint = result[1]
        count += 1
    print "\n",
    epoint = result[1]

if epoint > spoint :
    print "probably down-up"
else :
    print "probably up-down"