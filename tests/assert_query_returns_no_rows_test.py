import databricks_test
import pytest


def test_no_rows_returned():
    with databricks_test.session() as dbrickstest:
        query = """
        SELECT col1,col2
        FROM
        (VALUES 
          (100,'foo'),
          (101,'bar'),
          (102,'baz')
        ) AS v (col1, col2)
        WHERE 1=2
        """

        dbrickstest.assert_query_returns_no_rows(query)

def test_rows_returned():
    with databricks_test.session() as dbrickstest:
        query = """
        SELECT col1,col2
        FROM
        (VALUES 
          (100,'foo'),
          (101,'bar'),
          (102,'baz')
        ) AS v (col1, col2)
        ORDER BY col1
        """
        
        expected_message = """the result set was not empty:
+----+----+
|col1|col2|
+----+----+
|100 |foo |
|101 |bar |
|102 |baz |
+----+----+
"""
        with pytest.raises(Exception) as exception_message: 
            dbrickstest.assert_query_returns_no_rows(query)

        assert str(exception_message.value) == expected_message
