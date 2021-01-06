# databricks_test

## About

An experimental unit test framework for Databricks notebooks.

_This open-source project is not developed by nor affiliated with Databricks._

## Installing

```
pip install databricks_test
```

## Usage

Add a cell at the beginning of your Databricks notebook:

```python
# Instrument for unit tests. This is only executed in local unit tests, not in Databricks.
if 'dbutils' not in locals():
    import ecotricity_databricks_test
    ecotricity_databricks_test.inject_variables()
```

The `if` clause causes the inner code to be skipped when run in Databricks.
Therefore there is no need to install the `databricks_test` module on your Databricks environment.

Add your notebook into a code project, for example using [GitHub version control in Azure Databricks](https://docs.microsoft.com/en-us/azure/databricks/notebooks/azure-devops-services-version-control).

Set up pytest in your code project (outside of Databricks).

Create a test case with the following structure:

```python
import ecotricity_databricks_test

def test_method():
    with ecotricity_databricks_test.session() as dbrickstest:

        # Set up mocks on dbrickstest
        # ...

        # Run notebook
        dbrickstest.run_notebook("notebook_dir", "notebook_name_without_py_suffix")

        # Test assertions
        # ...
```

You can set up [mocks](https://docs.python.org/dev/library/unittest.mock.html) on
`dbrickstest.dbutils` and `dbrickstest.display`, for example:

```python
dbrickstest.dbutils.widgets.get.return_value = "myvalue"
```

See samples below for more examples.

## Supported features

* Spark context injected into Databricks notebooks: `spark`, `table`
* PySpark with all Spark features including reading and writing to disk, UDFs and Pandas UDFs
* Databricks Utilities (`dbutils`, `display`) with user-configurable mocks
* Mocking connectors such as Azure Storage, S3 and SQL Data Warehouse
* Helper function to compare and evaluate the results of Spark SQL queries

## Unsupported features

* Notebook formats other than `.py` (`.ipynb`, `.dbc`) are not supported
* Non-python cells such as `%scala` and `%sql` (those cells are skipped, as they are stored in `.py` notebooks as comments)
* Writing directly to `/dbfs` mount on local filesystem
* Databricks extensions to Spark such as `spark.read.format("binaryFile")`

## Helper functions

Helper functions are available to assist with Spark SQL queries:

### `Session.assert_queries_are_equal(actual_query, expected_query)`

* Asserts that the result sets returned by two supplied Spark SQL queries are equal
* A detailed table comparison output is only shown in the event the assertion fails. 
In the table comparison output, the first column (`m`) can have three different values:
  * the symbol `<` indicates that the row was found in the *expected* results but did not match anything in the *actual* results
  * the symbol `>` indicates that the row was found in the *actual* table but not in the *expected* table
  * the symbol `=` indicates that the row was matched between the *expected* and *actual* tables
  * *this behaviour is inspired by the `tSQLt` unit test framework for SQL Server*

### `Session.assert_query_returns_no_rows(actual_query)`

* Assets that the result set returned by a supplied Spark SQL query is empty
* A detailed table comparison output is only shown in the event that the result set is not empty

## Sample test

Sample test case for an ETL notebook reading CSV and writing Parquet.

```python
import pandas as pd
import ecotricity_databricks_test
from tempfile import TemporaryDirectory

from pandas.testing import assert_frame_equal

def test_etl():
    with ecotricity_databricks_test.session() as dbrickstest:
        with TemporaryDirectory() as tmp_dir:
            out_dir = f"{tmp_dir}/out"

            # Provide input and output location as widgets to notebook
            switch = {
                "input": "tests/etl_input.csv",
                "output": out_dir,
            }
            dbrickstest.dbutils.widgets.get.side_effect = lambda x: switch.get(
                x, "")

            # Run notebook
            dbrickstest.run_notebook(".", "etl_notebook")

            # Notebook produces a Parquet file (directory)
            resultDF = pd.read_parquet(out_dir)

        # Compare produced Parquet file and expected CSV file
        expectedDF = pd.read_csv("tests/etl_expected.csv")
        assert_frame_equal(expectedDF, resultDF, check_dtype=False)
```

In the notebook, we pass parameters using widgets.
This makes it easy to pass
a local file location in tests, and a remote URL (such as Azure Storage or S3)
in production.

```python
# Databricks notebook source
# This notebook processed the training dataset (imported by Data Factory)
# and computes a cleaned dataset with additional features such as city.
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import col, pandas_udf, PandasUDFType

# COMMAND ----------

# Instrument for unit tests. This is only executed in local unit tests, not in Databricks.
if 'dbutils' not in locals():
    import ecotricity_databricks_test
    ecotricity_databricks_test.inject_variables()

# COMMAND ----------

# Widgets for interactive development.
dbutils.widgets.text("input", "")
dbutils.widgets.text("output", "")
dbutils.widgets.text("secretscope", "")
dbutils.widgets.text("secretname", "")
dbutils.widgets.text("keyname", "")

# COMMAND ----------

# Set up storage credentials

spark.conf.set(
    dbutils.widgets.get("keyname"),
    dbutils.secrets.get(
        scope=dbutils.widgets.get("secretscope"),
        key=dbutils.widgets.get("secretname")
    ),
)

# COMMAND ----------

# Import CSV files
schema = StructType(
    [
        StructField("aDouble", DoubleType(), nullable=False),
        StructField("anInteger", IntegerType(), nullable=False),
    ]
)

df = (
    spark.read.format("csv")
    .options(header="true", mode="FAILFAST")
    .schema(schema)
    .load(dbutils.widgets.get('input'))
)
display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

# Inputs and output are pandas.Series of doubles
@pandas_udf('integer', PandasUDFType.SCALAR)
def square(x):
    return x * x


# COMMAND ----------

# Write out Parquet data
(df
    .withColumn("aSquaredInteger", square(col("anInteger")))
    .write
    .parquet(dbutils.widgets.get('output'))
 )
```


## Advanced mocking

Sample test case mocking PySpark classes for a notebook connecting to Azure SQL Data Warehouse.

```python
import ecotricity_databricks_test
import pyspark
import pyspark.sql.functions as F
from tempfile import TemporaryDirectory
from pandas.testing import assert_frame_equal
import pandas as pd


def test_sqldw(monkeypatch):
    with ecotricity_databricks_test.session() as dbrickstest, TemporaryDirectory() as tmp:

        out_dir = f"{tmp}/out"

        # Mock SQL DW loader, creating a Spark DataFrame instead
        def mock_load(reader):
            return (
                dbrickstest.spark
                .range(10)
                .withColumn("age", F.col("id") * 6)
                .withColumn("salary", F.col("id") * 10000)
            )

        monkeypatch.setattr(
            pyspark.sql.readwriter.DataFrameReader, "load", mock_load)

        # Mock SQL DW writer, writing to a local Parquet file instead
        def mock_save(writer):
            monkeypatch.undo()
            writer.format("parquet")
            writer.save(out_dir)

        monkeypatch.setattr(
            pyspark.sql.readwriter.DataFrameWriter, "save", mock_save)

        # Run notebook
        dbrickstest.run_notebook(".", "sqldw_notebook")

        # Notebook produces a Parquet file (directory)
        resultDF = pd.read_parquet(out_dir)

        # Compare produced Parquet file and expected CSV file
        expectedDF = pd.read_csv("tests/sqldw_expected.csv")
        assert_frame_equal(expectedDF, resultDF, check_dtype=False)
```

## Spark SQL comparison functions

A test comparing the output of two Spark SQL queries using the 
`assert_queries_are_equal` function:

```python
def test_results_do_not_match():
    with databricks_test.session() as dbrickstest:
        actual_query = """
        SELECT col1,col2
        FROM
        (VALUES
          (100,'foo'),
          (101,'bar'),
          (102,'baz')
        ) AS v (col1, col2)
        """

        expected_query = """
        SELECT col1,col2
        FROM
        (VALUES
          (100,'foo'),
          (110,'bar'),
          (999,'qux')
        ) AS v (col1, col2)
        """
        
        dbrickstest.assert_queries_are_equal(actual_query, expected_query)
```

A test validating that the output of a Spark SQL query returns no rows 
using the `assert_query_returns_no_rows` function:

```python
def test_no_rows_returned():
    with databricks_test.session() as dbrickstest:
        query = """
        SELECT 100 AS col1, 'abc' AS col2
        WHERE 1=2
        """

        dbrickstest.assert_query_returns_no_rows(query)
```

## Issues

Please report issues at [http://github.com/algattik/databricks_test](http://github.com/algattik/databricks_test).
