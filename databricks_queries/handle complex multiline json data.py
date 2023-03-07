# Databricks notebook source
#handling multiline complex json files
#json file are semistructure in nature we have to convert it into structure form in order to do processing
#complex datatypes: 1)struct -dict 2)array-list 3)map

# COMMAND ----------

dbutils.fs.put("/schenarios/complex_json.json", """[{
	"id": "0001",
	"type": "donut",
	"name": "Cake",
	"ppu": 0.55,
	"batters":
		{
			"batter":
				[
					{ "id": "1001", "type": "Regular" },
					{ "id": "1002", "type": "Chocolate" },
					{ "id": "1003", "type": "Blueberry" },
					{ "id": "1004", "type": "Devil's Food" }
				]
		},
	"topping":
		[
			{ "id": "5001", "type": "None" },
			{ "id": "5002", "type": "Glazed" },
			{ "id": "5005", "type": "Sugar" },
			{ "id": "5007", "type": "Powdered Sugar" },
			{ "id": "5006", "type": "Chocolate with Sprinkles" },
			{ "id": "5003", "type": "Chocolate" },
			{ "id": "5004", "type": "Maple" }
		]
}]""",True); 

# COMMAND ----------

df_json=spark.read.option("multiline","true").json("/schenarios/complex_json.json")
#df_json=spark.read.format("json").option("multiline","true").load("/schenarios/complex_json.json")
df_json.printSchema()
display(df_json)


# COMMAND ----------

#how to flatten complex datatypes? 
#use explode() function for array datatypes

from pyspark.sql.functions import *
res=df_json.withColumn("topping_explode",explode("topping"))\
    .withColumn("topping_id",col("topping_explode.id")).withColumn("topping_type",col("topping_explode.type"))\
    .drop("topping_explode","topping")\
    .withColumn("batters_explode",explode("batters.batter"))\
    .withColumn("batter_id",col("batters_explode.id")).withColumn("batters_type",col("batters_explode.type"))\
    .drop("batters","batters_explode")\
    


#res.show()
display(res)


# COMMAND ----------

res1=res.orderBy("Batters_type")
display(res1)

# COMMAND ----------


