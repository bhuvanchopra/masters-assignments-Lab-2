from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string


inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('word count improved')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def words_once(w):
    return (w, 1)

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

def lowercase(s):
    return s.lower()

def split(line):
    wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))
    return wordsep.split(line)
    
text = sc.textFile(inputs)
text = text.flatMap(split)
text = text.map(lowercase)
text = text.filter(lambda n: len(n) > 0)
words = text.map(words_once)
wordcount = words.reduceByKey(operator.add)

outdata = wordcount.sortBy(get_key).map(output_format)
outdata.saveAsTextFile(output)

