# -*- coding: utf-8 -*-
"""
Created on Wed Aug 10 21:33:38 2016

@author: scuseei
"""

import json
from pyspark import SparkContext

addr = "s3://weibo0810/"

with open("%sweibo_list_small.json" % addr) as f_r:
    data = f_r.readline()
f_json = json.loads(data)

with open("%sweibo_list_small_sep.json" % addr, 'w') as f_w:
    for term in f_json:
        f_w.write(term + "\n")

data = []
with open("%sweibo_list_small_sep.json"  % addr, 'w') as f_r:
    for line in f_r:
        data.append(line.strip())

#data_raw = sc.textFile("%sweibo_list_small_sep.json" % addr)
sc.parallelize(data).map(lambda x: x)


