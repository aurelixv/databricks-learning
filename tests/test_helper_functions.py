import pytest
from app.functions import helper_functions


def test_get_spark_returns_spark_session(monkeypatch):
    class DummySparkSession:
        pass

    # Patch DatabricksSession and SparkSession
    class DummyDatabricksSession:
        class builder:
            @staticmethod
            def serverless():
                return DummyDatabricksSession.builder

            @staticmethod
            def getOrCreate():
                return DummySparkSession()

    class DummySparkSessionBuilder:
        @staticmethod
        def getOrCreate():
            return DummySparkSession()

    # Case 1: DatabricksSession import works
    monkeypatch.setattr(
        "databricks.connect.DatabricksSession", DummyDatabricksSession, raising=False
    )
    monkeypatch.setattr(helper_functions, "SparkSession", DummySparkSessionBuilder)
    monkeypatch.setattr(
        helper_functions, "DatabricksSession", DummyDatabricksSession, raising=False
    )
    result = helper_functions.get_spark()
    assert isinstance(result, DummySparkSession)

    # Case 2: DatabricksSession import fails, fallback to SparkSession
    def import_error(*args, **kwargs):
        raise ImportError

    monkeypatch.setattr(
        helper_functions, "DatabricksSession", import_error, raising=False
    )
    result = helper_functions.get_spark()
    assert isinstance(result, DummySparkSession)


def test_get_spark_docstring():
    assert helper_functions.get_spark.__doc__ is not None
    assert "Returns a Spark session" in helper_functions.get_spark.__doc__
