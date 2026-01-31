using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using System.Text;

namespace DataFusionSharp.Interop;

internal static partial class NativeMethods
{
    private const string LibraryName = "datafusion_sharp_native";

    // Runtime

    [LibraryImport(LibraryName, EntryPoint = "datafusion_runtime_new")]
    public static partial DataFusionErrorCode RuntimeNew(uint workerThreads, uint maxBlockingThreads, out IntPtr runtimeHandle);

    [LibraryImport(LibraryName, EntryPoint = "datafusion_runtime_destroy")]
    public static partial DataFusionErrorCode RuntimeShutdown(IntPtr runtimeHandle, ulong timeoutMillis);
    
    // Context
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_new")]
    public static partial DataFusionErrorCode ContextNew(IntPtr runtimeHandle, out IntPtr contextHandle);
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_destroy")]
    public static partial DataFusionErrorCode ContextDestroy(IntPtr contextHandle);
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_register_csv")]
    public static partial DataFusionErrorCode ContextRegisterCsv(IntPtr contextHandle, [MarshalAs(UnmanagedType.LPStr)] string tableName, [MarshalAs(UnmanagedType.LPStr)] string filePath, AsyncCallback callback, ulong callbackUserData);
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_sql")]
    public static partial DataFusionErrorCode ContextSql(IntPtr contextHandle, [MarshalAs(UnmanagedType.LPStr)] string sql, AsyncCallback callback, ulong callbackUserData);
    
    // DataFrame
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_destroy")]
    public static partial DataFusionErrorCode DataFrameDestroy(IntPtr dataFrameHandle);
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_count")]
    public static partial DataFusionErrorCode DataFrameCount(IntPtr dataFrameHandle, AsyncCallback callback, ulong callbackUserData);
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_show")]
    public static partial DataFusionErrorCode DataFrameShow(IntPtr dataFrameHandle, AsyncCallback callback, ulong callbackUserData);
}

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
internal delegate void AsyncCallback(IntPtr result, IntPtr error, ulong userData);

internal class AsyncOperations
{
    public static AsyncOperations Instance { get; } = new();
    
    private readonly ConcurrentDictionary<ulong, object> _operations = new();
    private ulong _nextId = 0;
    
    public (ulong Id, TaskCompletionSource TaskCompletionSource) Create()
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        
        var id = Interlocked.Increment(ref _nextId);
        while (!_operations.TryAdd(id, tcs))
            id = Interlocked.Increment(ref _nextId);
        
        return (id, tcs);
    }
    
    public (ulong Id, TaskCompletionSource<T> Source) Create<T>()
    {
        var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        
        var id = Interlocked.Increment(ref _nextId);
        while (!_operations.TryAdd(id, tcs))
            id = Interlocked.Increment(ref _nextId);
        
        return (id, tcs);
    }

    public void Abort(ulong id)
    {
        _operations.TryRemove(id, out _);
    }
    
    public void CompleteVoid(ulong id, Exception? exception = null)
    {
        if (!_operations.TryRemove(id, out var t))
            return;
        
        if (t is not TaskCompletionSource tcs)
            return;
        
        if (exception == null)
            tcs.TrySetResult();
        else
            tcs.TrySetException(exception);
    }
    
    public void CompleteWithError<T>(ulong id, Exception exception)
    {
        if (!_operations.TryRemove(id, out var t))
            return;
        
        if (t is not TaskCompletionSource<T> tcs)
            return;
        
        tcs.TrySetException(exception);
    }
    
    public void CompleteWithResult<T>(ulong id, T result)
    {
        if (!_operations.TryRemove(id, out var t))
            return;
        
        if (t is not TaskCompletionSource<T> tcs)
            return;
        
        tcs.TrySetResult(result);
    }
}

internal static class AsyncVoidOperations
{
    public static void Callback(IntPtr _, IntPtr error, ulong handle)
    {
        var exception = error != IntPtr.Zero ? ErrorInfo.FromIntPtr(error) : null;
        AsyncOperations.Instance.CompleteVoid(handle, exception);
    }
}

internal static class AsyncIntPtrOperations
{
    public static void Callback(IntPtr result, IntPtr error, ulong handle)
    {
        if (error == IntPtr.Zero)
            AsyncOperations.Instance.CompleteWithResult(handle, Marshal.ReadIntPtr(result));
        else
            AsyncOperations.Instance.CompleteWithError<IntPtr>(handle, ErrorInfo.FromIntPtr(error));
    }
}

internal static class AsyncUInt64Operations
{
    public static void Callback(IntPtr result, IntPtr error, ulong handle)
    {
        if (error == IntPtr.Zero)
            AsyncOperations.Instance.CompleteWithResult(handle, (ulong)Marshal.ReadInt64(result));
        else
            AsyncOperations.Instance.CompleteWithError<ulong>(handle, ErrorInfo.FromIntPtr(error));
    }
}

[StructLayout(LayoutKind.Sequential)]
public struct ErrorInfo
{
    public DataFusionErrorCode Code;
    public IntPtr Message;      // *const u8
    public uint MessageLen;     // u32
    
    public string GetMessage()
    {
        if (Message == IntPtr.Zero || MessageLen == 0)
            return string.Empty;
            
        var message = Marshal.PtrToStringUTF8(Message, (int)MessageLen);
        return message;
    }
    
    public static Exception FromIntPtr(IntPtr ptr)
    {
        var errorInfo = Marshal.PtrToStructure<ErrorInfo>(ptr);
        var message = errorInfo.GetMessage();
        return new DataFusionException(errorInfo.Code, message);
    }
}