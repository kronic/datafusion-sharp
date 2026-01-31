using DataFusionSharp.Interop;

namespace DataFusionSharp;

public sealed class DataFusionRuntime : IAsyncDisposable, IDisposable
{
    private static readonly TimeSpan DefaultShutdownTimeout = TimeSpan.FromMilliseconds(500);
    
    private IntPtr _handle;

    private DataFusionRuntime(IntPtr handle)
    {
        _handle = handle;
    }
    
    ~DataFusionRuntime()
    {
        ShutdownRuntime(DefaultShutdownTimeout);
    }

    public static DataFusionRuntime Create(uint? workerThreads = null, uint? maxBlockingThreads = null)
    {
        if (workerThreads.HasValue)
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(workerThreads.Value, nameof(workerThreads));
        if (maxBlockingThreads.HasValue)
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxBlockingThreads.Value, nameof(maxBlockingThreads));
        
        var result = NativeMethods.RuntimeNew(workerThreads ?? 0, maxBlockingThreads ?? 0, out var handle);
        if (result != DataFusionErrorCode.Ok)
            throw new DataFusionException(result, "Failed to create DataFusion runtime");

        return new DataFusionRuntime(handle);
    }
    
    public SessionContext CreateSessionContext()
    {
        var result = NativeMethods.ContextNew(_handle, out var contextHandle);
        if (result != DataFusionErrorCode.Ok)
            throw new DataFusionException(result, "Failed to create DataFusion session context");
        
        return new SessionContext(this, contextHandle);
    }
    
    public void Dispose()
    {
        ShutdownRuntime(DefaultShutdownTimeout);
        GC.SuppressFinalize(this);
    }
    
    public ValueTask DisposeAsync()
    {
        return new ValueTask(Task.Run(Dispose));
    }

    private void ShutdownRuntime(TimeSpan timeout)
    {
        var handle = _handle;
        if (handle == IntPtr.Zero)
            return;
        
        _handle = IntPtr.Zero;
        
        NativeMethods.RuntimeShutdown(handle, (ulong) timeout.TotalMilliseconds);
    }
}