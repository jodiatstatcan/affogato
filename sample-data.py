# Databricks notebook source
# MAGIC %md
# MAGIC #### Generate Sample Data
# MAGIC 
# MAGIC 1. Create sample data dynamically
# MAGIC 2. Use databricks built in datasets
# MAGIC 
# MAGIC Cannot access databricks datasets

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Sources
# MAGIC - [Create Dataframe in Azure Databricks with Example](https://azurelib.com/create-dataframe-in-azure-databricks-with-example/#:~:text=6%20Final%20Thoughts-,How%20to%20Create%20a%20Dataframe%20in%20Databricks%20%3F,existing%20list%2C%20DataFrame%20and%20RDD.)

# COMMAND ----------

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

# COMMAND ----------

# MAGIC %md
# MAGIC Quickest and smallest single column dataframe

# COMMAND ----------

# Quick dataframe with one column of a sequence of numbers
df = spark.range(100)
df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Different ways to create a slightly more complex dataframe, with option to specify schema

# COMMAND ----------

# Provide columns and data
columns = ["id", "name"]
data = [("1", "John"), ("2", "Brock"),("3", "Dave")]

# COMMAND ----------

# Create dataframe from rdd with toDF() in Databricks
rdd = spark.sparkContext.parallelize(data)
df_from_rdd = rdd.toDF()
df_from_rdd.show() # no column names
df_from_rdd.printSchema()

# COMMAND ----------

# Create dataframe from rdd with createDataFrame() from SparkSession
df_from_rdd1 = spark.createDataFrame(rdd).toDF(*columns) # this adds the correct col names
df_from_rdd1.show()
df_from_rdd1.printSchema()

# COMMAND ----------

# Create dataframe with list of data directly (no rdd)
df_from_data = spark.createDataFrame(data).toDF(*columns)
df_from_data.show()
df_from_data.printSchema()

# COMMAND ----------

# Create dataframe with createDataFrame() with Row type
from pyspark.sql import Row
row_data = map(lambda x: Row(*x), data)
df_from_data1 = spark.createDataFrame(row_data, columns)
df_from_data1.show()
df_from_data1.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Create dataframe while declaring schema: specify column names along with data types and constraints

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
data2 = [("James","","Smith","36636","M"),
    ("Michael","Rose","","40288","M"),
    ("Robert","","Williams","42114","M"),
    ("Maria","Anne","Jones","39192","F"),
    ("Jen","Mary","Brown","","F")
]
 
schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
  ])

df_defined_schema = spark.createDataFrame(data=data2, schema=schema)
df_defined_schema.show(truncate=False)
df_defined_schema.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Create Random Uniform Distribution Dataframe

# COMMAND ----------

from pyspark.mllib.linalg import DenseVector
from pyspark.mllib.random import RandomRDDs

# Generate uniform vector RDDs and convert to dataframe
df_uniform  = (RandomRDDs.uniformVectorRDD(sc, 10,10)
                  .map(lambda a : DenseVector(a))
                  .map(lambda a : (a,))
                  .toDF(['features'])) # numpy.ndarray are not supported.

df_uniform.show()
df_uniform.printSchema()

# COMMAND ----------

# Vectors in a list to separate columns in df
df_uniform_multicols  = RandomRDDs.uniformVectorRDD(sc, 100,5).map(lambda a : a.tolist()).toDF()
df_uniform_multicols.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Sample and Fake Existing VPOP Tables

# COMMAND ----------

# Example VRF table
vrf_sdf = spark.table('vpop_refactored.silver_vrf')

# COMMAND ----------

?spark.table

# COMMAND ----------


