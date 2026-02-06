namespace DataFusionSharp.Tests;

public sealed class SessionContextTests : IDisposable
{
    private readonly DataFusionRuntime _runtime = DataFusionRuntime.Create();

    [Fact]
    public void CreateSessionContext_ReturnsContext()
    {
        // Act
        using var context = _runtime.CreateSessionContext();

        // Assert
        Assert.NotNull(context);
    }

    [Fact]
    public async Task SqlAsync_ReturnsDataFrame()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();

        // Act
        using var df = await context.SqlAsync("SELECT 1");

        // Assert
        Assert.NotNull(df);
    }

    [Fact]
    public async Task SqlAsync_InvalidQuery_ThrowsDataFusionException()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();

        // Act & Assert
        await Assert.ThrowsAsync<DataFusionException>(async () =>
        {
            using var df = await context.SqlAsync("SELECT * FROM customers");
        });
    }

    public void Dispose()
    {
        _runtime.Dispose();
    }
}