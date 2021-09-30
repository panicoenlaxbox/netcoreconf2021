from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet(r"C:\Users\azureuser\netcoreconf\daily_stock")
df.createOrReplaceTempView("daily_stock")
df = spark.sql("""
SELECT 
    PointOfSaleId AS point_of_sale_id,
    COUNT(*) AS count,
    MIN(Date) AS min_date,
    MAX(Date) AS max_date
FROM
    daily_stock
GROUP BY
    PointOfSaleId
ORDER BY
    count DESC
""")
df.show()
