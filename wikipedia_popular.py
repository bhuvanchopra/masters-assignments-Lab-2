from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string


inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('wikipedia popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def mapperout(w):
    return (w[0], (w[3], w[2]))

def get_key(kv):
    return kv[0]

#def output_format(kv):
#    k, v = kv
#    return '%s %i' % (k, v)

def lowercase(s):
    return s.lower()

def split(line):
    list1 = line.split()
    return (list1[0],list1[1],list1[2],int(list1[3]),list1[4])

def tab_separated(kv):
    return "%s\t%s" % (kv[0], kv[1])

text = sc.textFile(inputs)
text = text.map(split)
text = text.filter(lambda n: n[1] == "en" and n[2] != "Main_Page" and not(n[2].startswith("Special:")))

keyvalue = text.map(mapperout)
popular = keyvalue.reduceByKey(max)

outdata = popular.sortBy(get_key).map(tab_separated)

outdata.saveAsTextFile(output)

