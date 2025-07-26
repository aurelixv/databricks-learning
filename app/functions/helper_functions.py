from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """
    Returns a Spark session connected to Databricks.

    This function initializes and returns a Spark session using the Databricks Connect library.
    It is used to interact with Spark clusters in a Databricks environment.

    Returns:
        DatabricksSession: An active Spark session connected to Databricks.
    """
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.serverless().getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()
