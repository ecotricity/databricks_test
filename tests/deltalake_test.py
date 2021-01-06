import ecotricity_databricks_test
from tempfile import TemporaryDirectory


def test_deltalake_write():
    with ecotricity_databricks_test.session() as dbrickstest:
        with TemporaryDirectory() as tmp_dir:
            out_dir = f"{tmp_dir}/delta_out"

            # Provide input and output location as widgets to notebook
            switch = {
                "output": out_dir,
            }
            dbrickstest.dbutils.widgets.get.side_effect = lambda x: switch.get(x, "")

            # Run notebook
            dbrickstest.run_notebook(".", "deltalake_write_notebook")

            # Read delta
            df = dbrickstest.spark.read.format("delta").load(out_dir)

            # Validate dataframe contains the expected values
            rg = range(0, 5)
            for n in rg:
                assert df.filter(df["id"] == n).count() == 1

            # Validate dataframe contains no unexpected values
            assert df.count() == 5
