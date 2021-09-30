using Microsoft.Spark.Sql;
using F = Microsoft.Spark.Sql.Functions;

namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();
            var df = spark.Read().Parquet(args[0]);
            df = df.GroupBy((F.Col("PointOfSaleId")))
                .Agg(
                    F.Count("*").Alias("count"),
                    F.Min(F.Col("Date")).Alias("min_date"),
                    F.Max(F.Col("Date")).Alias("max_date"))
                .OrderBy(F.Col("count").Desc())
                .WithColumnRenamed("PointOfSaleId", "point_of_sale_id");
            df.Show();
        }
    }
}
