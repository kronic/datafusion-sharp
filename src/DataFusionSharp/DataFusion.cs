namespace DataFusionSharp;

public static class DataFusion
{
    public static int Add(int left, int right)
    {
        return Interop.NativeMethods.Add(left, right);
    }
}