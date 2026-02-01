pub type Callback = extern "C" fn(
    result: *const std::ffi::c_void,
    error: *const ErrorInfoData,
    user_data: u64
);

#[repr(C)]
pub struct StringData {
    pub data: *const u8,
    pub len: u32,
}

impl StringData {
    fn new(s: &str) -> Self {
        StringData {
            data: s.as_ptr(),
            len: s.len() as u32,
        }
    }
}

#[repr(C)]
pub struct ErrorInfoData {
    pub code: crate::ErrorCode,
    pub message: StringData
}

impl ErrorInfoData {
    fn new(err: &crate::ErrorInfo) -> Self {
        ErrorInfoData {
            code: err.code(),
            message: StringData::new(err.message()),
        }
    }
}

pub(crate) fn invoke_callback<T>(result: Result<T, crate::ErrorInfo>, callback: Callback, user_data: u64) {
    match result {
        Ok(value) => {
            let value_ptr = &value as *const T as *const std::ffi::c_void;
            callback(value_ptr, std::ptr::null(), user_data);
        }
        Err(error) => {
            let err_info = ErrorInfoData::new(&error);
            let err_into_ptr = &err_info as *const ErrorInfoData;
            callback(std::ptr::null(), err_into_ptr, user_data);
        }
    }
}
