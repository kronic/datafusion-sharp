namespace DataFusionSharp.Tests;

public class SessionContextTests
{
    [Fact]
    public void CreateSessionContext_Returns()
    {
        using var runtime = DataFusionRuntime.Create();
        using var context = runtime.CreateSessionContext();
        
        Assert.NotNull(context);
    }
    
    [Fact]
    public async Task RegisterCsvAsync_WithValidPath_Completes()
    {
        await using var runtime = DataFusionRuntime.Create();
        using var context = runtime.CreateSessionContext();
        
        var ordersCsvFilePath = Path.Combine("Data", "orders.csv");
        
        await context.RegisterCsvAsync("orders", ordersCsvFilePath);
    }

    [Fact]
    public async Task RegisterCsvAsync_WithInvalidPath_ThrowsDataFusionException()
    {
        await using var runtime = DataFusionRuntime.Create();
        using var context = runtime.CreateSessionContext();
        var invalidFilePath = Path.Combine("Data", "non_existent_file.csv");
        var exception = await Assert.ThrowsAsync<DataFusionException>(async () =>
        {
            await context.RegisterCsvAsync("invalid_table", invalidFilePath);
        });
        Assert.Equal(DataFusionErrorCode.TableRegistrationFailed, exception.ErrorCode);
    }
}