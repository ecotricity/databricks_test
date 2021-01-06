import ecotricity_databricks_test


def test_workflow():
    with ecotricity_databricks_test.session() as dbrickstest:
        # Run notebook
        dbrickstest.run_notebook(".", "workflow_notebook")
