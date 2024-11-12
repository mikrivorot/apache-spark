from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

# ITE00100554,18000101,TMAX,-75,,,E,
# ITE00100554,18000101,TMIN,-148,,,E,
def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0 # transformation to F
    return (stationID, entryType, temperature) # composed list [[x,y,z], [x1,y1,z1],...]

lines = sc.textFile("data/1800.csv")
parsedLines = lines.map(parseLine) # get parsed (prepared) lines
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1]) # filter in all lines with TMIN
stationTemps = minTemps.map(lambda x: (x[0], x[2])) # remove entryType (x[1]) =>  [[x,z], [x1,z1],...]
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
results = minTemps.collect(); # <- the moment when Spark starts 'real' execution?

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
