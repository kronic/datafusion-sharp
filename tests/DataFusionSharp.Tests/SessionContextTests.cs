namespace DataFusionSharp.Tests;

public sealed class SessionContextTests : IDisposable
{
    private static string OrdersCsvFilePath => Path.Combine("Data", "orders.csv");
    
    private readonly DataFusionRuntime _runtime = DataFusionRuntime.Create();

    [Fact]
    public void CreateSessionContext_ReturnsContext()
    {
        using var context = _runtime.CreateSessionContext();
        
        Assert.NotNull(context);
    }
    
    [Fact]
    public async Task RegisterCsvAsync_CompletesSuccessfully()
    {
        using var context = _runtime.CreateSessionContext();
        
        var ordersCsvFilePath = Path.Combine("Data", "orders.csv");
        
        await context.RegisterCsvAsync("orders", ordersCsvFilePath);
    }

    [Fact]
    public async Task SqlAsync_ReturnsDataFrame()
    {
        using var context = _runtime.CreateSessionContext();
        
        using var df = await context.SqlAsync("SELECT 1");
        
        Assert.NotNull(df);
    }
    
    [Fact]
    public async Task ToStringAsync_ReturnsString()
    {
        using var context = _runtime.CreateSessionContext();

        using var df = await context.SqlAsync("SELECT 1");

        var str = await df.ToStringAsync();

        Assert.NotNull(str);
        Assert.NotEmpty(str);
    }

    [Fact]
    public async Task SqlAsync_InvalidQuery_ThrowsDataFusionException()
    {
        using var context = _runtime.CreateSessionContext();

        await Assert.ThrowsAsync<DataFusionException>(async () =>
        {
            using var df = await context.SqlAsync("SELECT customer_id FROM orders");
        });
    }

    public void Dispose()
    {
        _runtime.Dispose();
    }
}