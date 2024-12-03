import re
from pyspark import SparkConf, SparkContext
import collections

def normalizeWords(text):
    # re.compile(pattern, flags=0): Compile a regular expression pattern into a regular expression object, 
    # which can be used for matching 
    
    # re.UNICODE: In Python 3, Unicode characters are matched by default for str patterns. 
    # This flag is therefore redundant with no effect and is only kept for backward compatibility.
    
    # re.ASCII: Make \w, \W, \b, \B, \d, \D, \s and \S perform ASCII-only matching instead of full Unicode matching. 
    # This is only meaningful for Unicode (str) patterns, and is ignored for bytes patterns.
    
    # Not perfect, it treats "it's" as two words = 'it' and 's'
    
    # "Self-Employment: Building an Internet Business of One"
    # into
    # Self-Employment:
    # Building
    # an
    # Internet
    # ...
    return re.compile(r'\W+', re.ASCII).split(text.lower()) # strip out punctuation

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("../data/Book")
words = input.flatMap(normalizeWords)

# Same as we do with friends -> manually count
wordCounts = words\
    .map(lambda x: (x, 1))\
    .reduceByKey(lambda x, y: x + y)
    
# flip
wordCountsSorted = wordCounts\
    .map(lambda x: (x[1], x[0]))\
    .sortByKey()

for result in wordCountsSorted.collect():
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + "\t\t" + count)