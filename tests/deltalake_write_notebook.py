# Databricks notebook source

# Instrument for unit tests. This is only executed in local unit tests, not in Databricks.
if 'dbutils' not in locals():
    import ecotricity_databricks_test
    ecotricity_databricks_test.inject_variables()

# COMMAND ----------
data = spark.range(0, 5)
data.write.format("delta").save(dbutils.widgets.get('output'))

