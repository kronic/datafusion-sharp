using System.Runtime.InteropServices;

namespace DataFusionSharp.Interop;

[StructLayout(LayoutKind.Sequential)]
internal struct StringData
{
    /// <summary>
    /// String data pointer, UTF-8 encoded, *const u8
    /// </summary>
    public IntPtr DataPtr;
    
    /// <summary>
    /// String data length, u32
    /// </summary>
    public int Length;
    
    /// <summary>
    /// Gets the message as a managed string.
    /// </summary>
    /// <returns></returns>
    public string GetMessage()
    {
        if (DataPtr == IntPtr.Zero || Length == 0)
            return string.Empty;
            
        var message = Marshal.PtrToStringUTF8(DataPtr, Length);
        return message;
    }
    
    public static StringData FromIntPtr(IntPtr ptr)
    {
        var data = Marshal.PtrToStructure<StringData>(ptr);
        return data;
    }
}