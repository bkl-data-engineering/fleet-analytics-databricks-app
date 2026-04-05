from pyspark.sql import SparkSession, DataFrame


class DatabricksClient:
    def __init__(self, settings) -> None:
        self.settings = settings
        self.spark = SparkSession.builder.getOrCreate()

    def table_fqn(self, table_name: str) -> str:
        return f"{self.settings.uc_catalog}.{self.settings.uc_schema}.{table_name}"

    def read_table(self, table_name: str) -> DataFrame:
        return self.spark.table(self.table_fqn(table_name))
