#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    Ok = 0,
    Panic = 1,
    InvalidArgument = 2,
    RuntimeInitializationFailed = 3,
    TableRegistrationFailed = 4,
    SqlError = 5,
}

#[derive(Debug)]
pub(crate) struct Error {
    pub code: ErrorCode,
    pub message: String,
}
