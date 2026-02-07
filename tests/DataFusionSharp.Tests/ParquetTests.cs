using Xunit.Abstractions;

namespace DataFusionSharp.Tests;

public sealed class ParquetTests : FileFormatTests
{
    protected override string FileExtension => ".parquet";
    
    public ParquetTests(ITestOutputHelper testOutputHelper)
        : base(testOutputHelper)
    {
    }

    protected override Task RegisterCustomersTableAsync()
    {
        return Context.RegisterParquetAsync("customers", DataSet.CustomersParquetPath);
    }

    protected override Task RegisterOrdersTableAsync()
    {
        return Context.RegisterParquetAsync("orders", DataSet.OrdersParquetPath);
    }

    protected override Task WriteTableAsync(DataFrame dataFrame, string path)
    {
        return dataFrame.WriteParquetAsync(path);
    }
}