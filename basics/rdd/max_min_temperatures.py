from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinMaxTemperatures")
sc = SparkContext(conf = conf)

# ITE00100554,18000101,TMAX,-75,,,E,
# ITE00100554,18000101,TMIN,-148,,,E,
def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3])
    return (stationID, entryType, temperature) # composed list [[x,y,z], [x1,y1,z1],...]

lines = sc.textFile("./data/1800.csv")
parsedLines = lines.map(parseLine) # get parsed (prepared) lines

# filter in all lines with TMAX
# remove entryType (x,y,z) =>  [[x,z], [x1,z1],...]
# same key, but max value only
# collect() <- the moment when Spark starts 'real' execution?
maxTemps = parsedLines \
    .filter(lambda x: "TMAX" in x[1]) \
    .map(lambda x: (x[0], x[2])) \
    .reduceByKey(lambda x, y: max(x,y)) \
    .collect()
    
minTemps = parsedLines \
    .filter(lambda x: "TMIN" in x[1]) \
    .map(lambda x: (x[0], x[2])) \
    .reduceByKey(lambda x, y: min(x,y)) \
    .collect()

for result in maxTemps:
    print(result[0] + "\t{:}C".format(result[1]))

for result in minTemps:
    print(result[0] + "\t{:}C".format(result[1]))
