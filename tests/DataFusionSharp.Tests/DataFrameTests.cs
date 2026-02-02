namespace DataFusionSharp.Tests;

public sealed class DataFrameTests : IDisposable
{
    private readonly DataFusionRuntime _runtime = DataFusionRuntime.Create();

    [Fact]
    public async Task CountAsync_ReturnsRowCount()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var df = await context.SqlAsync("SELECT * FROM (VALUES (1), (2), (3)) AS t(x)");

        // Act
        var count = await df.CountAsync();

        // Assert
        Assert.Equal(3UL, count);
    }

    [Fact]
    public async Task CountAsync_EmptyResult_ReturnsZero()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var df = await context.SqlAsync("SELECT 1 WHERE false");

        // Act
        var count = await df.CountAsync();

        // Assert
        Assert.Equal(0UL, count);
    }

    [Fact]
    public async Task ShowAsync_CompletesSuccessfully()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var df = await context.SqlAsync("SELECT 1 as value");

        // Act & Assert
        await df.ShowAsync();
    }

    [Fact]
    public async Task ShowAsync_WithLimit_CompletesSuccessfully()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var df = await context.SqlAsync("SELECT * FROM (VALUES (1), (2), (3)) AS t(x)");

        // Act & Assert
        await df.ShowAsync(limit: 2);
    }

    [Fact]
    public async Task GetSchemaAsync_ReturnsSchema()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var df = await context.SqlAsync("SELECT 1 as id, 'hello' as name");

        // Act
        var schema = await df.GetSchemaAsync();

        // Assert
        Assert.NotNull(schema);
        Assert.Equal(2, schema.FieldsList.Count);
        Assert.Equal("id", schema.FieldsList[0].Name);
        Assert.Equal("name", schema.FieldsList[1].Name);
    }

    [Fact]
    public async Task CollectAsync_ReturnsData()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var df = await context.SqlAsync("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)");

        // Act
        var collected = await df.CollectAsync();

        // Assert
        Assert.NotNull(collected);
        Assert.NotNull(collected.Schema);
        Assert.Equal(2, collected.Schema.FieldsList.Count);
        Assert.NotEmpty(collected.Batches);
        Assert.Equal(2, collected.Batches.Sum(b => b.Length));
    }

    [Fact]
    public async Task CollectAsync_EmptyResult_ReturnsEmptyBatches()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var df = await context.SqlAsync("SELECT 1 as id WHERE false");

        // Act
        var collected = await df.CollectAsync();

        // Assert
        Assert.NotNull(collected);
        Assert.NotNull(collected.Schema);
        Assert.Equal(0, collected.Batches.Sum(b => b.Length));
    }

    [Fact]
    public async Task ToStringAsync_ReturnsString()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var df = await context.SqlAsync("SELECT 1 as value");

        // Act
        var str = await df.ToStringAsync();

        // Assert
        Assert.NotNull(str);
        Assert.NotEmpty(str);
        Assert.Contains("value", str);
    }

    public void Dispose()
    {
        _runtime.Dispose();
    }
}