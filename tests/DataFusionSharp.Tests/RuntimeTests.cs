namespace DataFusionSharp.Tests;

public sealed class RuntimeTests
{
    [Fact]
    public void Create_ReturnsRuntime()
    {
        // Act
        using var runtime = DataFusionRuntime.Create();

        // Assert
        Assert.NotNull(runtime);
    }

    [Fact]
    public void Create_MultipleRuntimes_AllValid()
    {
        // Act
        using var runtime1 = DataFusionRuntime.Create();
        using var runtime2 = DataFusionRuntime.Create();

        // Assert
        Assert.NotNull(runtime1);
        Assert.NotNull(runtime2);
    }
}