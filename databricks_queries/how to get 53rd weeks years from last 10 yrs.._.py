# Databricks notebook source
df=spark.createDataFrame([(y,"01/01/"+str(y)+"") for y in range(1999 ,2050)],["id","year"])
display(df)

# COMMAND ----------


from pyspark.sql.functions import *
df=df.withColumn("dates",to_date("year","dd/MM/yyyy")).withColumn("week_num",weekofyear("dates")).where("week_num==53")
df.printSchema()
display(df)




# COMMAND ----------


