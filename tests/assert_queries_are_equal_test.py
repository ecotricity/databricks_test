import databricks_test
import pytest


def test_results_match():
    with databricks_test.session() as dbrickstest:
        query = """
        SELECT col1,col2
        FROM
        (VALUES 
          (100,'foo'),
          (101,'bar'),
          (102,'baz')
        ) AS v (col1, col2)
        """

        dbrickstest.assert_queries_are_equal(query, query)

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
        
        with pytest.raises(Exception) as exception_message: 
            dbrickstest.assert_queries_are_equal(actual_query, expected_query)

        assert str(exception_message.value).startswith("the result sets did not match:")

def test_unexpected_result():
    with databricks_test.session() as dbrickstest:
        actual_query = """
        SELECT col1,col2
        FROM
        (VALUES 
          (100,'foo'),
          (101,'bar')
        ) AS v (col1, col2)
        """

        expected_query = """
        SELECT col1,col2
        FROM
        (VALUES 
          (100,'foo')
        ) AS v (col1, col2)
        """
        
        expected_message="""the result sets did not match:
+---+----+----+
|m  |col1|col2|
+---+----+----+
|=  |100 |foo |
|>  |101 |bar |
+---+----+----+
"""

        with pytest.raises(Exception) as exception_message: 
            dbrickstest.assert_queries_are_equal(actual_query, expected_query)

        assert str(exception_message.value)==expected_message

def test_missing_result():
    with databricks_test.session() as dbrickstest:
        actual_query = """
        SELECT col1,col2
        FROM
        (VALUES 
          (100,'foo')
        ) AS v (col1, col2)
        """

        expected_query = """
        SELECT col1,col2
        FROM
        (VALUES 
          (100,'foo'),
          (101,'bar')
        ) AS v (col1, col2)
        """
        
        expected_message="""the result sets did not match:
+---+----+----+
|m  |col1|col2|
+---+----+----+
|=  |100 |foo |
|<  |101 |bar |
+---+----+----+
"""

        with pytest.raises(Exception) as exception_message: 
            dbrickstest.assert_queries_are_equal(actual_query, expected_query)

        assert str(exception_message.value)==expected_message
