# Databricks notebook source

# COMMAND ----------

# Instrument for unit tests. This is only executed in local unit tests, not in Databricks.
if "dbutils" not in locals():
    import ecotricity_databricks_test

    ecotricity_databricks_test.inject_variables()

# COMMAND ----------

assert dbutils.widgets.get("input") == ""
