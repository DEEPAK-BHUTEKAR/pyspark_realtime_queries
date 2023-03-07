# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.fs.put("/schenarios/multi_delim.csv","""name||age||city
venu||32||hyd
anu||49||mas
jyo||12||blr
koti||29||blr
mastan||30||blr
hydera||99||mas
nakul||71||hyd
bhim||22||pune
ashok||20||pune
hena||49||blr
rani||51||hyd
""",True)

# COMMAND ----------

#reading multi_delim.csv file 
%fs 

# COMMAND ----------

#clean the double delimiter data
df=spark.read.format("csv").option("header","true").option("sep","||").load("/schenarios/multi_delim.csv").show()
#df=spark.read.csv("/schenarios/multi_delim.csv",header=True,sep="||").show()


# COMMAND ----------

#cleaning multi_delimiter datafile which has more than one delimiter
dbutils.fs.put("/schenarios/double_delim.csv","""name,age,city,marks
venu,32,hyd,35|45|54|67
anu,49,mas,54|45|43|60
jyo,12,blr,45|52|45|75
koti,29,blr,35|45|54|67
mastan,30,blr,35|53|54|61
hydera,99,mas,35|45|54|27
nakul,71,hyd,35|77|54|75
bhim,22,pune,67|88|41|67
ashok,20,pune,55|45|54|67
hena,49,blr,35|35|74|67
rani,51,hyd,47|59|84|67
""",True)

# COMMAND ----------

#reading multi_delim.csv file 
%fs head /schenarios/double_delim.csv 


# COMMAND ----------

#read the /schenarios/double_delim.csv file having multiple delimiter
multi_df=spark.read.csv("/schenarios/double_delim.csv",header=True)
display(multi_df)
multi_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select split("1|2|3|4","[|]")

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

df=multi_df.withColumn("split_marks",split(col("marks"),"[|]"))\
    .withColumn("sub1",col("split_marks")[0])\
    .withColumn("sub2",col("split_marks")[1])\
    .withColumn("sub3",col("split_marks")[2])\
    .withColumn("sub4",col("split_marks")[3])\
    .drop("split_marks","marks")
    

df.show()

# COMMAND ----------


