namespace DataFusionSharp.Tests;

public class InteropTests
{
    [Fact]
    public void Add_ReturnsCorrectSum()
    {
        var result = DataFusion.Add(2, 3);
        Assert.Equal(5, result);
    }
}