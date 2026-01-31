using DataFusionSharp.Interop;

namespace DataFusionSharp;

public sealed class SessionContext : IDisposable
{
    private readonly DataFusionRuntime _runtime;
    
    private IntPtr _handle;

    internal SessionContext(DataFusionRuntime runtime, IntPtr handle)
    {
        _runtime = runtime;
        _handle = handle;
    }
    
    ~SessionContext()
    {
        DestroyContext();
    }
    
    public Task RegisterCsvAsync(string tableName, string filePath)
    {
        var (id, tcs) = AsyncOperations.Instance.Create();
        var result = NativeMethods.ContextRegisterCsv(_handle, tableName, filePath, AsyncVoidOperations.Callback, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start registering CSV file");
        }
        return tcs.Task;
    }
    
    public async Task<DataFrame> SqlAsync(string sql)
    {
        var (id, tcs) = AsyncOperations.Instance.Create<IntPtr>();
        var result = NativeMethods.ContextSql(_handle, sql, AsyncIntPtrOperations.Callback, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start executing SQL query");
        }
        
        var dataFrameHandle = await tcs.Task;

        return new DataFrame(this, dataFrameHandle);
    }
    
    public void Dispose()
    {
        DestroyContext();
        GC.SuppressFinalize(this);
    }
    
    private void DestroyContext()
    {
        var handle = _handle;
        if (handle == IntPtr.Zero)
            return;
        
        _handle = IntPtr.Zero;
        
        NativeMethods.ContextDestroy(handle);
    }
}