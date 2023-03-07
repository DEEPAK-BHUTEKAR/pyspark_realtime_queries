# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC 
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
# File location and type
file_location = "/FileStore/tables/donations-1.csv"

#read file using CSV options where The applied options are for CSV files, For other file types, these will be ignored.
df=spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep",",").load(file_location)
df.printSchema()
df.show()


# COMMAND ----------



#/* Query the created temp table in a SQL cell */

df.createOrReplaceTempView("donataiontab")
res=spark.sql("select * from donataiontab where amount >=6000")
res.printSchema()
res.show()


# COMMAND ----------

#usage of date function in pyspark
#spark bedefalut understand date 'yyyy/MM/dd' format only
#If read data has 'dd/MM/yyyy' format we hv to convert it into spark understandable format 
#to_date() -->use to convert input date to 'yyyy/MM/dd'
#trunc() -->first day of year,month,quarter
res=df.withColumn("today",current_date()).withColumn("curr_time",current_timestamp())\
    .withColumn("dt_diff", datediff(col("today"), col("dt")))\
    .withColumn("days_later" ,date_add(df.dt,100))\
    .withColumn("days_before" ,date_sub(df.dt,100))\
    .withColumn("lstdate", last_day(df.dt)).withColumn("lstday",date_format(last_day(df.dt),'EEE'))\
    .withColumn("nxtdate",next_day(df.dt ,"Friday"))\
    .withColumn("lastMonSun",date_add(next_day(last_day(df.dt),"Sun"),-7))\
    .withColumn("dayofWeek",dayofweek(df.dt)).withColumn("dayofmonth",dayofmonth(col("dt"))).withColumn("dayofYr",dayofyear(df.dt))\
    .withColumn("mon_bet",months_between(current_date(),df.dt))\
    .withColumn("trunc_day",date_trunc("mon",col("dt")).cast("date"))

res.show()




# COMMAND ----------

#write userDefineFunction(UDF) to get no of days pass in yr,mon,days format -->EX:1yr,3month,4days

def daystoYrMon(num):
    yrs=num//365
    mon=((num%365)//30)
    days=((num%365)%30)
    result=yrs, "years" ," ",mon ,"months"," ", days
    st=''.join(map(str,result))
    return st

#num=int(input("enter the values of no of days passed: "))
#format(num)
udffunc=udf(daystoYrMon,StringType())

ndf=res.drop("dayofWeek","dayofmonth","dayofYr","curr_time","days_later","days_before").withColumn("daystoYrMon" ,udffunc(col("dt_diff")))

ndf.show(5)

# COMMAND ----------




    





# COMMAND ----------


