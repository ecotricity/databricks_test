import ecotricity_databricks_test
from ecotricity_databricks_test import SessionAlreadyExistsException


def test_forbidden_concurrent_sessions():
    with ecotricity_databricks_test.session() as dbrickstest:  # noqa: F841
        try:
            with ecotricity_databricks_test.session() as dbrickstest2:  # noqa: F841
                assert False, "should have failed"
        except SessionAlreadyExistsException:
            pass
