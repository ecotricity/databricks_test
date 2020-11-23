import pandas as pd
import databricks_test
from tempfile import TemporaryDirectory


def test_deltalake_write():
    with databricks_test.session() as dbrickstest:
        with TemporaryDirectory() as tmp_dir:
            out_dir = f"{tmp_dir}/delta_out"

            # Provide input and output location as widgets to notebook
            switch = {
                "output": out_dir,
            }
            dbrickstest.dbutils.widgets.get.side_effect = lambda x: switch.get(
                x, "")

            # Run notebook
            dbrickstest.run_notebook(".", "deltalake_write_notebook")