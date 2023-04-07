import sys
import re
from operator import add
#qweqweqweqwe
# more changes
from pyspark import SparkContext

def map_phase(x):
    x = re.sub('--', ' ', x)
    x = re.sub("'", '', x)
    return re.sub('[?!@#$\'",.;:()]', '', x).lower()

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print >> sys.stderr, "Usage: wordcount <master> <inputfile> <outputfile>"
        exit(-1)
    sc = SparkContext(sys.argv[1], "python_wordcount_sorted in bigdataprogrammiing")
    lines = sc.textFile(sys.argv[2],2)

    tokyo_count = sc.accumulator(0)
    def extractCallsigns(line):
        global tokyo_count
        if (line =="Tokyo"):
            tokyo_count +=1
        return line.split(" ")

    callsign = lines.flatMap(extractCallsigns)
		#flatmap은 단일 원소로 쪼개준다

    print("파티션 개수: ",lines.getNumPartitions()) # print the number of partitions
    print("length...",callsign.count())
    print("Tokyo...",tokyo_count.value)

    pairs = lines.map(lambda x:(x.lower(),1))
    counts = pairs.reduceByKey(add)
    filtered = counts.filter(lambda x :x[0]!='Tokyo')
    sorted = filtered.sortBy(lambda x:x[1])
    sorted.saveAsTextFile("hdfs://localhost:9000/user/lim/output")
