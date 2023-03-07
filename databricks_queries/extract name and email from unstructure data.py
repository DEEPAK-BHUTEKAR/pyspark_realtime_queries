# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC 
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file = "/FileStore/tables/emailsmay4.txt"
#creating rdd for input file
erdd=spark.sparkContext.textFile(file)

pro=erdd.map(lambda x:x.split(" ")).map(lambda x:(x[0],x[-1])).filter(lambda x:'@' in x[1])

#getting name and email id of people by converting it into df
df=pro.toDF(["name","email"])
df.show(truncate=False)

'''for i in pro.collect():
    print(i)
'''
                                        



# COMMAND ----------


# load email file into databricks
dbutils.fs.put("/FileStore/tables/extract_emails.txt","""vivek kumar (to Organizer(s) Only): 7:04 PM: have you taken the spark submit in aws env. session.   
Sandeep (to Everyone): 7:06 PM: m on mobile.. sorry
Gaurav Puri (to Organizer(s) Only): 7:20 PM: Hi Venu
Mohammad  Tahir (to Organizer(s) Only): 7:20 PM: sir i dont have credit card 
Gaurav Puri (to Organizer(s) Only): 7:20 PM: I am not able to complete it
Gaurav Puri (to Organizer(s) Only): 7:20 PM: getting microsoft is experiencing problems
Balu Mahendra (Private): 7:22 PM: Venu can i share  my screenn as presenter
Balu Mahendra (to Organizer(s) Only): 7:22 PM: Can you make me the presenter?
Balu Mahendra (Private): 7:23 PM: Balaji
chandu (to Organizer(s) Only): 7:23 PM: Can you make me the presenter?
chandu (to Organizer(s) Only): 7:23 PM: Can you make me the presenter?
Balu Mahendra (Private): 7:23 PM: Just Want to share the screen to sign up the azure portal
Balu Mahendra (Private): 7:24 PM: i missed the begining 
Balu Mahendra (Private): 7:24 PM: Ok I will take of myself
Venkatakrishna (Private): 7:27 PM: HI, do we training for data science and AI also..?
Venkatakrishna (Private): 7:36 PM: Cluster size not required to mention...?
Rachhpal  Kaur (to Organizer(s) Only): 7:37 PM: Could you please record the session?
Balu Mahendra (Private): 8:42 PM: hereisbalaji4u@gmail.com
K Venkatesh (to Organizer(s) Only): 8:42 PM: venki241@gmail.com
Mohammad  Tahir (to Organizer(s) Only): 8:42 PM: mtmohdtahir@outlook.com
Ajit (to Organizer(s) Only): 8:42 PM: jitmnk007@gmail.com
Raghava (to Organizer(s) Only): 8:42 PM: raghavendratirunagari9@gmail.com
Ranjit Patil (to Organizer(s) Only): 8:42 PM: ranjitpatil789@outlook.com
parag patil (to Organizer(s) Only): 8:42 PM: paragsoft@gmail.com
anuj sharma (to Organizer(s) Only): 8:42 PM: anuj.itindia@gmail.com
Chiranjeevi nalla (to Organizer(s) Only): 8:43 PM: eswar.nalla70@gmail.com
Khasim Shaik (Private): 8:43 PM: Khasim519@yahoo.com
Praveenkumar S (Private): 8:43 PM: Sathyapraveen.sk@yahoo.com
Naushad Shaik (Private): 8:43 PM: naushad520@gmail.com
Mastan Gummanampati (Private): 8:43 PM: mastanetl4@outlook.com
Balu Mahendra (Private): 8:43 PM: srinivaschintala125@gmail.com
PRIYESH N (to Everyone): 8:43 PM:  n.priyesh1989@outlook.com
Manjunath (Private): 8:43 PM: manjunath.ptr@gmail.com
Chandrika (to Organizer(s) Only): 8:43 PM: sreechandu207@gmail.com
Praveenkumar S (to Organizer(s) Only): 8:43 PM: Sathyapraveen.sk@gmail.com
Raghava (to Organizer(s) Only): 8:43 PM: Can you send me the azure and aws deployment videos
Dell (to Organizer(s) Only): 8:43 PM: raghucity@gmail.com
SUNIL KUMR (to Organizer(s) Only): 8:43 PM: sunilkumarseeram@gmail.com
Gaurav Puri (to Organizer(s) Only): 8:44 PM: gauravpuri145@yahoo.com
Mastan Gummanampati (Private): 8:51 PM: there is lot of backround voice ...
Divesh (to Everyone): 8:53 PM: divesh.jain@yahoo.com
Raghava (to Everyone): 9:00 PM: raghavendratirunagari9@yahoo.com
Balu Mahendra (Private): 9:01 PM: Thank You Sir
Ranjit Patil (to Organizer(s) Only): 9:01 PM: Thanks a lot Venu
K Venkatesh (to Organizer(s) Only): 9:01 PM: thank you Venu , nice session 
PRIYESH N (to Everyone): 9:02 PM: Thanks sir!!
SUNIL KUMR (to Organizer(s) Only): 9:02 PM: Thanks sir
Raghava (to Everyone): 9:02 PM: Thanks a lot sir
Divesh (to Everyone): 9:04 PM: Please add my email address as well



""",True)
                                

                                        

# COMMAND ----------

#----finding ho many gmail id are there in email4file.txt

data = "/FileStore/tables/extract_emails.txt"

#creating rdd for input file
rdd=spark.sparkContext.textFile(data)
res=rdd.flatMap(lambda x:x.split(" ")).filter(lambda x:'@' in x).map(lambda x:x.split("@")[1]).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)

for x in res.collect():
    print(x)
        

# COMMAND ----------

dbutils.fs.put("/FileStore/tables/mobile.txt","""Sandeep (to Everyone): 7:06 PM: m on mobile.. sorry
Gaurav Puri (to Organizer(s) Only): 7:20 PM: Hi Venu
Mohammad  Tahir (to Organizer(s) Only): 7:20 PM: sir i dont have credit card 
Gaurav Puri (to Organizer(s) Only): 7:20 PM: I am not able to complete it
Gaurav Puri (to Organizer(s) Only): 7:20 PM: getting microsoft is experiencing problems
Balu Mahendra (Private): 7:22 PM: Venu can i share  my screenn as presenter
Balu Mahendra (to Organizer(s) Only): 7:22 PM: Can you make me the presenter?
Balu Mahendra (Private): 7:23 PM: Balaji
chandu (to Organizer(s) Only): 7:23 PM: Can you make me the presenter?
chandu (to Organizer(s) Only): 7:23 PM: Can you make me the presenter?
Balu Mahendra (Private): 7:23 PM: Just Want to share the screen to sign up the azure portal
Balu Mahendra (Private): 7:24 PM: i missed the begining 8984034208
Balu Mahendra (Private): 7:24 PM: Ok I will take of myself 8984034208
Venkatakrishna (Private): 7:27 PM: HI, do we training for data science and AI also..?
Venkatakrishna (Private): 7:36 PM: Cluster size not required to mention...?
Rachhpal  Kaur (to Organizer(s) Only): 7:37 PM: Could you please record the session?
Balu Mahendra (Private): 8:42 PM: hereisbalaji4u@gmail.com 8984034208
K Venkatesh (to Organizer(s) Only): 8:42 PM: venki241@gmail.com 8984034209
Mohammad  Tahir (to Organizer(s) Only): 8:42 PM: mtmohdtahir@outlook.com
Ajit (to Organizer(s) Only): 8:42 PM: jitmnk007@gmail.com
Raghava (to Organizer(s) Only): 8:42 PM: raghavendratirunagari9@gmail.com
Ranjit Patil (to Organizer(s) Only): 8:42 PM: ranjitpatil789@outlook.com 9984034209
parag patil (to Organizer(s) Only): 8:42 PM: paragsoft@gmail.com 8484034209
anuj sharma (to Organizer(s) Only): 8:42 PM: anuj.itindia@gmail.com 8834034209
Chiranjeevi nalla (to Organizer(s) Only): 8:43 PM: eswar.nalla70@gmail.com 8420034209
Khasim Shaik (Private): 8:43 PM: Khasim519@yahoo.com 8404434209
Praveenkumar S (Private): 8:43 PM: Sathyapraveen.sk@yahoo.com 8984034209
Naushad Shaik (Private): 8:43 PM: naushad520@gmail.com 8934034209
Mastan Gummanampati (Private): 8:43 PM: mastanetl4@outlook.com 8584034209
Balu Mahendra (Private): 8:43 PM: srinivaschintala125@gmail.com
PRIYESH N (to Everyone): 8:43 PM:  n.priyesh1989@outlook.com 8985034209
Manjunath (Private): 8:43 PM: manjunath.ptr@gmail.com 8984134209
""",True)

# COMMAND ----------

#extract the people and their mobile numbers 
data="/FileStore/tables/mobile.txt"
mrdd=spark.sparkContext.textFile(data)
res1=mrdd.map(lambda x:x.split(" ")).map(lambda x:(x[0],x[-1])).filter(lambda x:x[1].isdigit())
for x in res1.collect():
    print(x)


# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select * from `emailsmay4_txt`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "emailsmay4_txt"

# df.write.format("parquet").saveAsTable(permanent_table_name)
