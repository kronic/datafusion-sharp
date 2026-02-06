using System.Reflection;

namespace DataFusionSharp.Tests;

internal static class DataSet
{
    private static readonly string DataDir = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location)!, "Data");
    
    public static string CustomersCsvPath => Path.Combine(DataDir, "customers.csv");
    public static string OrdersCsvPath => Path.Combine(DataDir, "orders.csv");
    
    public static string CustomersJsonPath => Path.Combine(DataDir, "customers.json");
    public static string OrdersJsonPath => Path.Combine(DataDir, "orders.json");
    
    public static string CustomersParquetPath => Path.Combine(DataDir, "customers.parquet");
    public static string OrdersParquetPath => Path.Combine(DataDir, "orders.parquet");
}