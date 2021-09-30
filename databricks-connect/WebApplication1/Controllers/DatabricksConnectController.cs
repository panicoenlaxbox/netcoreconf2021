using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Spark.Sql;
using WebApplication1.Models;
using F = Microsoft.Spark.Sql.Functions;

namespace WebApplication1.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class DatabricksConnectController : ControllerBase
    {
        private readonly ILogger<DatabricksConnectController> _logger;

        public DatabricksConnectController(ILogger<DatabricksConnectController> logger)
        {
            _logger = logger;
        }

        [HttpGet]
        public IEnumerable<DailyStock> Get()
        {
            var spark = SparkSession.Builder().GetOrCreate();
            var df = spark.Read().Parquet("/mnt/netcoreconf/daily_stock");
            df = df.GroupBy((F.Col("PointOfSaleId")))
                .Agg(
                    F.Count("*").Alias("count"),
                    F.Min(F.Col("Date")).Alias("min_date"),
                    F.Max(F.Col("Date")).Alias("max_date"))
                .OrderBy(F.Col("count").Desc())
                .WithColumnRenamed("PointOfSaleId", "point_of_sale_id");
            var rows = df.Collect();
            var result = new List<DailyStock>();
            foreach (var row in rows)
            {
                result.Add(new DailyStock()
                {
                    PointOfSaleId = row.GetAs<int>("point_of_sale_id"),
                    Count = row.GetAs<int>("count"),
                });
            }
            return result;
        }
    }
}
