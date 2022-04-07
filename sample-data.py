# Databricks notebook source
# MAGIC %md
# MAGIC #### Generate Sample Data
# MAGIC 
# MAGIC 1. Create sample data dynamically
# MAGIC 2. Use databricks built in datasets
# MAGIC 
# MAGIC Cannot access databricks datasets

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

# Createa dataframe with createDataFrame() with Row type
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
