# Databricks notebook source
dbutils.fs.put("/schenarios/aslnull.csv","""name,age,city,salary
venu,32,null,40000
null,49,null,33000
jyo,12,blr,56000
koti,null,blr,null
mastan,30,blr,56000
null,29,mum,7500
blru,11,hyd,null
deep,null,blr,null
rani,40,null,36000
null,99,mum,5700
gopal,44,blr,null """,True)

# COMMAND ----------

# MAGIC %fs head /schenarios/aslnull.csv

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

df=spark.read.format("csv").option("nullValue","null").option("header","true").load("/schenarios/aslnull.csv")
#df=spark.read.option("nullValue","null").csv("/schenarios/aslnull.csv",header=True)
asldf=df.withColumn("age",col("age").cast("int")).withColumn("salary",col("salary").cast("int"))
asldf.printSchema()
asldf.show()



# COMMAND ----------

#display no of null records per columns

display(asldf.where("salary is null").count())
#display(df.where("salary is null").count())



# COMMAND ----------

#using list comprehension approach to get columnwise null values
#display(asldf.select([count(when(col(i).isNull(),i)).alias(i) for i in asldf.columns]))
null_df=asldf.select([count(when(col(i).isNull(),i)).alias(i) for i in asldf.columns]).show()


# COMMAND ----------


