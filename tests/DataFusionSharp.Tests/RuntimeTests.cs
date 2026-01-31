namespace DataFusionSharp.Tests;

public class RuntimeTests
{
    [Fact]
    public void Create_Returns()
    {
        using var runtime = DataFusionRuntime.Create();
        
        Assert.NotNull(runtime);
    }
}