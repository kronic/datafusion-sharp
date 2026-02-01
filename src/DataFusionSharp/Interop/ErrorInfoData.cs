using System.Runtime.InteropServices;

namespace DataFusionSharp.Interop;

[StructLayout(LayoutKind.Sequential)]
internal struct ErrorInfoData
{
    /// <summary>
    /// Error code
    /// </summary>
    public DataFusionErrorCode Code;
    
    /// <summary>
    /// Error message
    /// </summary>
    public StringData Message;
    
    public static ErrorInfoData FromIntPtr(IntPtr ptr)
    {
        var data = Marshal.PtrToStructure<ErrorInfoData>(ptr);
        return data;
    }
    
    public Exception ToException()
    {
        var message = Message.GetMessage();
        return new DataFusionException(Code, message);
    }
}