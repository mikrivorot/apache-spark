from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

# ITE00100554,18000101,TMAX,-75,,,E,
# ITE00100554,18000101,TMIN,-148,,,E,
def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3])
    return (stationID, entryType, temperature) # composed list [[x,y,z], [x1,y1,z1],...]

lines = sc.textFile("data/1800.csv")
parsedLines = lines.map(parseLine) # get parsed (prepared) lines
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1]) # filter in all lines with TMAX
stationTemps = maxTemps.map(lambda x: (x[0], x[2])) # remove entryType (x[1]) =>  [[x,z], [x1,z1],...]
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x,y)) # same key, but max value only
results = maxTemps.collect(); # <- the moment when Spark starts 'real' execution?

for result in results:
    print(result[0] + "\t{:}C".format(result[1]))
