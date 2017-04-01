# -*- coding: utf-8 -*-
"""
Created on Thu Aug 11 20:03:02 2016

@author: scuseei
"""

from pyspark import sql
from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("wrk0812").config("spark.some.config.option", "some-value").getOrCreate()

addr = "s3://weibo0810/"
df = spark.read.format('json').load('%sweibo_list_small.json' % addr)


############
#def json_write(term):
#    term.write.format('json').save('s3://emr2redshift/part-00004')
#    
#data = sc.parallelize(df.toJSON().collect())
#
##############
#for line in data.collect():
#    line.write.format('json').save('s3://emr2redshift/part-00005')
#
##############
#def f(each_row):
#    each_row.write.format('json').save('s3://emr2redshift/part-00020')
#
#
#df.foreachPartition(f)
#############


#############


df.write.format('json').save('s3://emr2redshift',mode='overwrite')
