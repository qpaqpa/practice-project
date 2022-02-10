import re
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import json
conf = SparkConf()
sc = SparkContext(conf=conf)

#reviewdata
rdd1= sc.textFile('reviews_Baby.json')#sys.argv[1]
rdd1=rdd1.filter(lambda x: 'overall' in eval(x).keys())
pair_rdd = rdd1.map(lambda x: (eval(x)['asin'],(eval(x)['overall'],1)))
avg_review = pair_rdd.reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1]))
avg_review = avg_review.map(lambda x: (x[0],(x[1][0]/x[1][1],x[1][1])))

#metadata
rdd2= sc.textFile('meta_Baby.json')#sys.argv[2]
rdd2=rdd2.filter(lambda x:'price' in eval(x).keys())
rdd_price = rdd2.map(lambda x:(eval(x)['asin'],eval(x)['price']))

#(id,(price,(rate,reviewnum)))
rdd_join=rdd_price.join(avg_review)
rdd_order=rdd_join.sortBy(keyfunc=lambda x:x[1][1][1],ascending=False)
rdd_result=rdd_order.map(lambda x:(x[0],x[1][1][0],x[1][0]))
sc.parallelize(rdd_result.take(15)).saveAsTextFile(sys.argv[3])
