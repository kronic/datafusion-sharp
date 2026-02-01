namespace DataFusionSharp.Tests;

public sealed class RuntimeTests
{
    [Fact]
    public void Create_Returns()
    {
        using var runtime = DataFusionRuntime.Create();
        
        Assert.NotNull(runtime);
    }
}