from pyspark.sql import SparkSession, DataFrame


def get_taxis(spark: SparkSession) -> DataFrame:
    return spark.read.table("samples.nyctaxi.trips")


# Create a new Databricks Connect session. If this fails,
# check that you have configured Databricks Connect correctly.
# See https://docs.databricks.com/dev-tools/databricks-connect.html.
def get_spark() -> SparkSession:
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()

def create_catalog(spark: SparkSession, catalog_name: str):
    spark.sql(f"CREATE CATALOG IF NOT EXISTS my_test_catalog")
    print(f"Catalog {catalog_name} created successfully.")

def main():
    get_taxis(get_spark()).show(5)


if __name__ == "__main__":
    main()
