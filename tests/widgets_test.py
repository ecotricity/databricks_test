import ecotricity_databricks_test


def test_widgets():
    with ecotricity_databricks_test.session() as dbrickstest:
        # Run notebook
        dbrickstest.run_notebook(".", "widgets_notebook")
