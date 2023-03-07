# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
import numpy as np
import random
# creating spark session object
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc=spark.sparkContext
lst = random.sample(range(1, 50), 7)
res=sc.parallelize(lst)
for i in res.collect():
    print(i)



# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/

# COMMAND ----------

#load asl file and convert it to rdd
data="/FileStore/tables/asl.csv"
rdd=spark.sparkContext.textFile(data)
skip=rdd.first()


# COMMAND ----------

#performing transformations and actions on aslrdd
#if u want to apply the logic on particular column u have to split the column using map() first then apply the logic
#but split bydefault convert everything to string type
#res=rdd.map(lambda x:x.split(","))

#skip the header using logic < skip=aslrdd.first() >
#res=rdd.filter(lambda x:x != skip).map(lambda x:x.split(",")).filter(lambda x:"hyd" in x[2])
#query output where city is hyd and age is >30
res=rdd.filter(lambda x: x != skip).map(lambda x:x.split(",")).filter(lambda x:"hyd" in x[2])

for x in res.collect():
    print(x)

# COMMAND ----------

#load asl file and convert it to rdd
data="/FileStore/tables/donations.csv"
drdd=spark.sparkContext.textFile(data)
skip=drdd.first()

# COMMAND ----------

#find total donation raise by each person use reduceByKey similar to group by in sql
res1=drdd.filter(lambda x: x != skip).map(lambda x:x.split(",")).map(lambda x:(x[0],int(x[2]))).reduceByKey(lambda x,y:x+y)

for i in res1.collect():
    print(i)

# COMMAND ----------

#rdd word count program
#flatmap recommanded for unstructure data 
kdata="/filestore/tables/rrr.txt"
krdd=sc.textFile(kdata)
lst=["the","is","to","a","and",".","of","in","was"]
pro=krdd.flatMap(lambda x:x.lower().split(" ")).filter(lambda x:x not in lst).filter(lambda y:y != '').map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b).sortBy(lambda x:x[1],False)

for x in pro.collect():
    print(x)


# COMMAND ----------


