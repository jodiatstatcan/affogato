# Databricks notebook source
# MAGIC %md
# MAGIC #### Type of widgets
# MAGIC 
# MAGIC We have 4 different types of widgets
# MAGIC - text: input value in text box
# MAGIC - dropdown: select from list of provided values
# MAGIC - combobox: combination of text input and dropdown, select value or input one
# MAGIC - multiselect: select one or more values

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("text", "Hello World!")

# COMMAND ----------

dbutils.widgets.dropdown("values", "1", [str(x) for x in range(1,10)])

# COMMAND ----------

dbutils.widgets.combobox("letters", "A", ["A", "B", "C"])

# COMMAND ----------

dbutils.widgets.multiselect("response", "Yes", ["Yes", "No", "Maybe"])

# COMMAND ----------

text = dbutils.widgets.get("text")
print(text)
dropdown = dbutils.widgets.get("values")
print(dropdown)

# COMMAND ----------

ms = dbutils.widgets.get("response").split()
for item in ms:
  print(item)

# COMMAND ----------

dbutils.widgets.remove("values")

# COMMAND ----------

dbutils.widgets.removeAll()
