namespace DataFusionSharp.Tests;

public sealed class CsvTests : IDisposable
{
    private readonly DataFusionRuntime _runtime;
    private readonly SessionContext _context;

    public CsvTests()
    {
        _runtime = DataFusionRuntime.Create();
        _context = _runtime.CreateSessionContext();
    }

    [Fact]
    public async Task RegisterCsvAsync_CompletesSuccessfully()
    {
        // Arrange

        // Act & Assert
        await _context.RegisterCsvAsync("customers", DataSet.CustomersCsvPath);
    }

    [Fact]
    public async Task RegisterCsvAsync_QueryRegisteredTable_ReturnsData()
    {
        // Arrange
        await _context.RegisterCsvAsync("customers", DataSet.CustomersCsvPath);

        // Act
        using var df = await _context.SqlAsync("SELECT * FROM customers");
        var count = await df.CountAsync();

        // Assert
        Assert.True(count > 0);
    }

    public void Dispose()
    {
        _context.Dispose();
        _runtime.Dispose();
    }
}