from pyspark.sql import SparkSession, DataFrame


def get_taxis(spark: SparkSession) -> DataFrame:
    return spark.read.table("samples.nyctaxi.trips")


def get_spark() -> SparkSession:
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()

def create_catalog(spark: SparkSession, catalog_name: str):
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
    print(f"Catalog {catalog_name} created successfully.")

def main():
    spark = get_spark()
    get_taxis(spark).show(5)
    create_catalog(spark, "my_catalog")


if __name__ == "__main__":
    main()