using DataFusionSharp.Interop;

namespace DataFusionSharp;

public class DataFrame : IDisposable
{
    private readonly SessionContext _sessionContext;
    private IntPtr _handle;
    
    internal DataFrame(SessionContext sessionContext, IntPtr handle)
    {
        _sessionContext = sessionContext;
        _handle = handle;
    }
    
    ~DataFrame()
    {
        DestroyDataFrame();
    }
    
    public Task<ulong> CountAsync()
    {
        var (id, tcs) = AsyncOperations.Instance.Create<ulong>();
        var result = NativeMethods.DataFrameCount(_handle, AsyncUInt64Operations.Callback, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start counting rows in DataFrame");
        }
        return tcs.Task;
    }

    public Task ShowAsync()
    {
        var (id, tcs) = AsyncOperations.Instance.Create();
        var result = NativeMethods.DataFrameShow(_handle, AsyncVoidOperations.Callback, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start showing DataFrame");
        }
        return tcs.Task;
    }
    
    public void Dispose()
    {
        DestroyDataFrame();
        GC.SuppressFinalize(this);
    }
    
    private void DestroyDataFrame()
    {
        var handle = _handle;
        if (handle == IntPtr.Zero)
            return;
        
        _handle = IntPtr.Zero;
        
        NativeMethods.DataFrameDestroy(handle);
    }
}