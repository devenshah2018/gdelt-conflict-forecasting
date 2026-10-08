"""Shared SparkSession factory used by the CLI and the web app."""
from pyspark.sql import SparkSession


def get_spark(app_name: str = "gdelt-conflict-forecasting") -> SparkSession:
    """Return a local SparkSession sized for a laptop."""
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.driver.memory", "4g")
        .config("spark.driver.extraJavaOptions", "-XX:ReservedCodeCacheSize=512m")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
