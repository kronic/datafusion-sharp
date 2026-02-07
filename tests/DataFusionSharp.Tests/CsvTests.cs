using Xunit.Abstractions;

namespace DataFusionSharp.Tests;

public sealed class CsvTests : FileFormatTests
{
    protected override string FileExtension => ".csv";

    public CsvTests(ITestOutputHelper testOutputHelper)
        : base(testOutputHelper)
    {
    }

    protected override Task RegisterCustomersTableAsync(string tableName = "customers")
    {
        return Context.RegisterCsvAsync(tableName, DataSet.CustomersCsvPath);
    }

    protected override Task RegisterOrdersTableAsync(string tableName = "orders")
    {
        return Context.RegisterCsvAsync(tableName, DataSet.OrdersCsvPath);
    }

    protected override Task WriteTableAsync(DataFrame dataFrame, string path)
    {
        return dataFrame.WriteCsvAsync(path);
    }
}