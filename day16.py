# Databricks notebook source
from pyspark.sql.functions import*
from pyspark.sql.types import*

# COMMAND ----------

print("hello")

# COMMAND ----------

df_001=spark.read.option("header",True).csv("/FileStore/gmde/googleplaystore.csv")


# COMMAND ----------

df_001.display()

# COMMAND ----------

sch= StructType().add("App",StringType(),True)\
    .add("Category",StringType(),True)\
    .add("Rating",DoubleType(),True)\
    .add("Reviews",IntegerType(),True)\
    .add("Size",StringType(),True)\
    .add("Installs",StringType(),True)\
    .add("Type",StringType(),True)\
    .add("Price",StringType(),True)\
    .add("Content Rating",StringType(),True)\
    .add("Genres",StringType(),True)\
    .add("Last Updated",StringType(),True)\
    .add("Current Ver",StringType(),True)\
    .add("Android Ver",StringType(),True)


# COMMAND ----------

df_001.printSchema()

# COMMAND ----------

df_001=spark.read.option("header",True).schema(sch).csv("/FileStore/gmde/googleplaystore.csv")


# COMMAND ----------

zip_df_01=df_001.withColumn("appgc",concat(col("App"),col("Genres")))


# COMMAND ----------

zip_df_01.display()

# COMMAND ----------

zip_df_02=df_001.withColumn("newcol",col("Category"))

# COMMAND ----------

zip_df_02.display()

# COMMAND ----------

zip_df_03=df_001.withColumn("owner",lit("ravi"))

# COMMAND ----------

zip_df_03.display()

# COMMAND ----------

zip_df_04=df_001.withColumn("owner",lit("raghu")).withColumn("newcol",col("Category")).withColumn("appgc",concat(col("App"),col("Genres")))

# COMMAND ----------

zip_df_04.display()

# COMMAND ----------

zip_df_05=df_001.withColumnRenamed("Rating1","Rating")

# COMMAND ----------

zip_df_05.display()

# COMMAND ----------



# COMMAND ----------

zip_df_06_select=df_001.select("App","Category")

# COMMAND ----------

zip_df_06_select.display()

# COMMAND ----------

zip_df_07_select=df_001.selectExpr('cast(Rating as integer) as Int_rating')

# COMMAND ----------

zip_df_07_select.display()

# COMMAND ----------

zip_df_08=df_001.sort(col("Rating").desc())

# COMMAND ----------

zip_df_08.display()

# COMMAND ----------

zip_df_08=df_001.orderBy(col("Reviews").desc())

# COMMAND ----------

zip_df_08.display()

# COMMAND ----------

zip_df_09=df_001.dropDuplicates(["Category","Rating"])

# COMMAND ----------

zip_df_09.display()

# COMMAND ----------

df_001.count()


# COMMAND ----------

zip_df_09.count()

# COMMAND ----------

zip_df_10=df_001.withColumn("info",when(col("Rating")>=4.0,"excellent")
                            .when((col("Rating")>3.0) & (col("Rating")<4.0),"good").
                            otherwise("bad"))

# COMMAND ----------

zip_df_10.display()

# COMMAND ----------

sch2= StructType().add("App",StringType(),True)\
    .add("Translated_Review",StringType(),True)\
    .add("Sentiment",StringType(),True)\
    .add("Sentiment_Polarity",DoubleType(),True)\
    .add("Sentiment_Subjectivity",DoubleType(),True)   

    

# COMMAND ----------

df_002=spark.read.option("header",True).schema(sch2).csv("/FileStore/gmde/googleplaystore_user_reviews.csv")

# COMMAND ----------

df_002.display()

# COMMAND ----------

zip_df_11=df_001.join(df_002,df_001.App==df_002.App,how="left").select(df_001['*'],df_002['Sentiment'])

# COMMAND ----------

zip_df_11.display()

# COMMAND ----------

zip_df_12=df_001.join(df_002,df_001.App==df_002.App,how="left").select(df_001['*'],df_002['Sentiment'])

# COMMAND ----------

zip_df_13 = zip_df_12.groupBy('App').avg('Rating')

# COMMAND ----------

zip_df_13.display()
