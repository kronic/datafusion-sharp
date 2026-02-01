namespace DataFusionSharp;

public class DataFusionException : Exception
{
    public DataFusionErrorCode ErrorCode { get; }

    public DataFusionException(DataFusionErrorCode errorCode)
        : base($"DataFusion error occurred: {errorCode}")
    {
        ErrorCode = errorCode;
    }
    
    public DataFusionException(DataFusionErrorCode errorCode, string message)
        : base(message)
    {
        ErrorCode = errorCode;
    }

    public DataFusionException(DataFusionErrorCode errorCode, string message, Exception innerException)
        : base(message, innerException)
    {
        ErrorCode = errorCode;
    }
}
