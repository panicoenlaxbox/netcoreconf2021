from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("dbfs:/mnt/netcoreconf/daily_stock")
df = (
    df.groupBy(F.col("PointOfSaleId"))
    .agg(
        F.count("*").alias("count"),
        F.min(F.col("Date")).alias("min_date"),
        F.max(F.col("Date")).alias("max_date"),
    )
    .orderBy("count", ascending=False)
    .withColumnRenamed("PointOfSaleId", "point_of_sale_id")
)
df.show()