using Xunit.Abstractions;

namespace DataFusionSharp.Tests;

public sealed class CsvTests : FileFormatTests
{
    protected override string FileExtension => ".csv";

    public CsvTests(ITestOutputHelper testOutputHelper)
        : base(testOutputHelper)
    {
    }

    protected override Task RegisterCustomersTableAsync()
    {
        return Context.RegisterCsvAsync("customers", DataSet.CustomersCsvPath);
    }

    protected override Task RegisterOrdersTableAsync()
    {
        return Context.RegisterCsvAsync("orders", DataSet.OrdersCsvPath);
    }

    protected override Task WriteTableAsync(DataFrame dataFrame, string path)
    {
        return dataFrame.WriteCsvAsync(path);
    }
}