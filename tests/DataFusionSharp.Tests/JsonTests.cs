using Xunit.Abstractions;

namespace DataFusionSharp.Tests;

public sealed class JsonTests : FileFormatTests
{
    protected override string FileExtension => ".json";
    
    public JsonTests(ITestOutputHelper testOutputHelper)
        : base(testOutputHelper)
    {
    }

    protected override Task RegisterCustomersTableAsync()
    {
        return Context.RegisterJsonAsync("customers", DataSet.CustomersJsonPath);
    }

    protected override Task RegisterOrdersTableAsync()
    {
        return Context.RegisterJsonAsync("orders", DataSet.OrdersJsonPath);
    }

    protected override Task WriteTableAsync(DataFrame dataFrame, string path)
    {
        return dataFrame.WriteJsonAsync(path);
    }
}
