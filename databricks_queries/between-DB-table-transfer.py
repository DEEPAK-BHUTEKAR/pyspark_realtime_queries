# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook shows you how to load data from JDBC databases using Spark SQL.
# MAGIC 
# MAGIC *For production, you should control the level of parallelism used to read data from the external database, using the parameters described in the documentation.*

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 1: Connection Information
# MAGIC 
# MAGIC This is a **Python** notebook so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` magic command. Python, Scala, SQL, and R are all supported.
# MAGIC 
# MAGIC First we'll define some variables to let us programmatically create these connections.

# COMMAND ----------

#C:\Users\Avita\PycharmProjects\pyspark_prog

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from configparser import ConfigParser

# creating spark session object
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

#data="D:\\Data_Engg_Notes\\spark\\Spark_dataset\\donations.csv"
#host="jdbc:mysql://sql-database.c9dlqkdtijxm.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
#user="myuser"
#pwd="mypassword"

#read mysql_DB credentials from config file
host="jdbc:mysql://sql-database.c9dlqkdtijxm.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
uname="myuser"
pwd="mypassword"
driver="com.mysql.jdbc.Driver"

#read postgresql_DB credentials from config file
#[pscred]
pshost="jdbc:postgresql://psdatabase.c9dlqkdtijxm.ap-south-1.rds.amazonaws.com:5432/postdb"
psuname="psuser"
pspwd="pdpassword"
psdriver="org.postgresql.Driver"




#reading mysql and postgresql database credentials from config.txt file 
# cred="/FileStore/tables/config.txt"
# conf = ConfigParser()
# conf.read(cred)
#read mysql_DB credentials from config file
# host = conf.get("mycred","myhost")
# uname = conf.get("mycred","myuser")
# pwd = conf.get("mycred","mypass")
# driver =conf.get("mycred","mydriver")

#read postgresql_DB credentials from config file
# pshost = conf.get("pscred","pshost")
# psuname = conf.get("pscred","puser")
# pspwd = conf.get("pscred","pdpwd")
# psdriver =conf.get("pscred","psdriver")


#-----------------------------------

#RDBMS JDBC driver name
#---MySQL  -->	"com.mysql.jdbc.Driver"
#---ORACLE -->	"oracle.jdbc.driver.OracleDriver"
#--postgresql --> "org.postgresql.Driver"








# COMMAND ----------

Step 2: Reading the data
Now that we specified our file metadata, we can create a DataFrame. You'll notice that we use an option to specify that we'd like to infer the schema from the file. We can also explicitly set this to a particular schema if we have one already.

First, let's create a DataFrame in Python, notice how we will programmatically reference the variables we defined above.

# COMMAND ----------

#read data from Mysql emp table
#df=spark.read.format("jdbc").option("url",host).option("user",uname).option("password",pwd).option("dbtable","emp").option("driver",driver).load()




#importing/reading multiple table as given below
tabs=["dept","empclean","asl"]

for x in tabs:
    print("importing table: " + x)
    df=spark.read.format("jdbc").option("url",host).option("user",uname).option("password", pwd)\
        .option("dbtable", x ).option("driver", driver).load()
    df.show()
#writing data to postgresql
   # df.na.fill(0).write.mode("append").format("jdbc").option("url",pshost).option("user",psuname).option("password",pspwd)\
    #         .option("dbtable",x + "new").option("driver",psdriver).save()


# COMMAND ----------

#importing / reading all tables from mysqldb and write into postgresql databse

qry="(select table_name from information_schema.tables where table_schema = 'mysqldb') tmp"
df=spark.read.format("jdbc").option("url",host).option("user",uname).option("password",pwd)\
   .option("dbtable",qry).option("driver",driver).load()

tabs=[x[0] for x in df.collect()]
for x in tabs:
    print("importing table: " + x)
    df=spark.read.format("jdbc").option("url",host).option("user",uname).option("password", pwd)\
        .option("dbtable", x ).option("driver", driver).load()
    df.show()
#writing data to postgresql
    df.na.fill(0).write.mode("append").format("jdbc").option("url",pshost).option("user",psuname).option("password",pspwd)\
             .option("dbtable",x + "new").option("driver",psdriver).save()


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 3: Querying the data
# MAGIC 
# MAGIC Now that we created our DataFrame. We can query it. For instance, you can select some particular columns to select and display within Databricks.

# COMMAND ----------

display(remote_table.select("EXAMPLE_COLUMN"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 4: (Optional) Create a view or table
# MAGIC 
# MAGIC If you'd like to be able to use query this data as a table, it is simple to register it as a *view* or a table.

# COMMAND ----------

remote_table.createOrReplaceTempView("YOUR_TEMP_VIEW_NAME")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can query this using Spark SQL. For instance, we can perform a simple aggregation. Notice how we can use `%sql` in order to query the view from SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT EXAMPLE_GROUP, SUM(EXAMPLE_AGG) FROM YOUR_TEMP_VIEW_NAME GROUP BY EXAMPLE_GROUP

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Since this table is registered as a temp view, it will be available only to this notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.

# COMMAND ----------

remote_table.write.format("delta").saveAsTable("MY_PERMANENT_TABLE_NAME")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This table will persist across cluster restarts as well as allow various users across different notebooks to query this data. However, this will not connect back to the original database when doing so.
