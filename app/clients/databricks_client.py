from pyspark.sql import DataFrame, SparkSession


class DatabricksClient:
    """Lightweight client for reading Unity Catalog tables via Spark."""

    def __init__(self, settings) -> None:
        self.settings = settings
        self.spark = SparkSession.builder.getOrCreate()

    def table_fqn(self, table_name: str) -> str:
        """Build a fully qualified Unity Catalog table name."""
        return f"{self.settings.uc_catalog}.{self.settings.uc_schema}.{table_name}"

    def read_table(self, table_name: str) -> DataFrame:
        """Read a table from Unity Catalog."""
        return self.spark.table(self.table_fqn(table_name))
