from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf = conf)

# The characters we wish to find the degree of separation between:
startCharacterID = 5988 # SpiderMan
targetCharacterID = 14  # ADAM 3,031 (who?)

# Our accumulator, used to signal when we find the target character during our BFS traversal.
hitCounter = sc.accumulator(0)


# from 5983 1165 3836 4361 1282 716 4289 4646 6300 5084 2397 4454 1913 5861 5485 
# to (5983, ([1165 3836 4361 1282 716 4289 4646 6300 5084 2397 4454 1913 5861 5485], 9999, WHITE))
def convertToBFS(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))

    color = 'WHITE'
    distance = 9999

    if (heroID == startCharacterID):
        color = 'GRAY'
        distance = 0

    return (heroID, (connections, distance, color))


def createStartingRdd():
    inputFile = sc.textFile("./data/marvel-graph.txt")
    return inputFile.map(convertToBFS)

def bfsMap(node):
    # (5983, ([1165 3836 4361 1282 716 4289 4646 6300 5084 2397 4454 1913 5861 5485], 0, GRAY ))
    characterID = node[0] # 5983
    data = node[1] # ([1165 3836 4361 1282 716 4289 4646 6300 5084 2397 4454 1913 5861 5485], 0, GRAY)
    connections = data[0] # [1165 3836 4361 1282 716 4289 4646 6300 5084 2397 4454 1913 5861 5485]
    distance = data[1] # 9999 or 0
    color = data[2] # WHITE or GRAY

    results = []

    # If this node needs to be expanded === if node is GRAY === either we started from this hero or we marked it to visit neighbours
    if (color == 'GRAY'):
        # [1165 3836 4361 1282 716 4289 4646 6300 5084 2397 4454 1913 5861 5485]
        for connection in connections: # 1165
            newCharacterID = connection
            newDistance = distance + 1 # e.g. 0 -> 1
            newColor = 'GRAY'
            if (targetCharacterID == connection):
                hitCounter.add(1)

            # why is list of connections empty? -> list of connections will be filled later in bfsReduce()
            # (1165, [], 1, GRAY)
            # (3836, [], 1, GRAY)
            # ...
            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)

        #We've processed this node, so color it black
        color = 'BLACK'
        
    # if not (node is WHITE) - DO NOT visit neighbours
    
    # Emit the input node so we don't lose it.
    # (5983, ([1165 3836 4361 1282 716 4289 4646 6300 5084 2397 4454 1913 5861 5485], 0, GRAY ))
    results.append( (characterID, (connections, distance, color)) )
    return results

# (5983, ([1165 3836 4361 1282 716 4289 4646 6300 5084 2397 4454 1913 5861 5485], 0, GRAY ))
# (1165, [], 1, GRAY)
# (3836, [], 1, GRAY)
# ... all WHITE nodes

# We do reduce:
#   to merge initially duplicated line in the initial file
#   merge line like (217, [], 1, GRAY) with (217, [4080 1062 1908 3712 3805 4754 2650 3829 2494 132 6066 3515 5768 4607 2824 4473 3345 172 4109 5548 831 4805 2670 5982 1718 859 5096 6291 5795 2664 6003 4892 1403 6353 6229 528 424 1449 4647 4935 5066 3765 4318 2930 2588 2244 108 3073 3405 6155 3407 3059 5046 1347 5121 3705 1805 6012 6039 1828 2557 5716 3421 5440 3504 3505 3245 3015 6208 1582 2673 4650 3984 920 478 2693 2399], 999999, WHITE) 
def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = color1
    edges = []

    # See if one is the original node with its connections.
    # If so preserve them.
    if (len(edges1) > 0):
        edges.extend(edges1)
    if (len(edges2) > 0):
        edges.extend(edges2)

    # Preserve minimum distance
    if (distance1 < distance):
        distance = distance1

    if (distance2 < distance):
        distance = distance2

    # Preserve darkest color
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2

    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1

    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1

    return (edges, distance, color)


#Main program here:
iterationRdd = createStartingRdd()

for iteration in range(0, 10):
    print("Running BFS iteration# " + str(iteration+1))

    # Create new vertices as needed to darken or reduce distances in the
    # reduce stage. If we encounter the node we're looking for as a GRAY
    # node, increment our accumulator to signal that we're done.
    
    # 'mapped' content
    # (5983, ([1165 3836 4361 1282 716 4289 4646 6300 5084 2397 4454 1913 5861 5485], 0, GRAY ))
    # (1165, [], 1, GRAY)
    # (3836, [], 1, GRAY)
    # ... all WHITE nodes
    mapped = iterationRdd.flatMap(bfsMap)

    # Note that mapped.count() action here forces the RDD to be evaluated, and
    # that's the only reason our accumulator is actually updated.
    print("Processing " + str(mapped.count()) + " values.")

    if (hitCounter.value > 0):
        print("Hit the target character! From " + str(hitCounter.value) \
            + " different direction(s).")
        break

    # Reducer combines data for each character ID, preserving the darkest
    # color and shortest path.
    iterationRdd = mapped.reduceByKey(bfsReduce)
    iterationRdd.count()
