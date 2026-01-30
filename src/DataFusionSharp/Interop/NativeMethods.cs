using System.Runtime.InteropServices;

namespace DataFusionSharp.Interop;

internal static partial class NativeMethods
{
    private const string LibraryName = "libdatafusion_sharp_native";

    [LibraryImport(LibraryName, EntryPoint = "add")]
    public static partial int Add(int left, int right);
}