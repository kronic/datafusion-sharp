namespace DataFusionSharp;

public enum DataFusionErrorCode
{
    Ok = 0,
    Panic = 1,
    InvalidArgument = 2,
    RuntimeInitializationFailed = 3,
    RuntimeShutdownFailed = 4,
    TableRegistrationFailed = 5,
    SqlError = 6,
    DataFrameError = 7
}
