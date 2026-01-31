pub type Callback = extern "C" fn(
    result: *const std::ffi::c_void,
    error: *const ErrorInfo,
    user_data: u64
);

#[repr(C)]
pub struct ErrorInfo {
    pub code: crate::ErrorCode,
    pub message: *const u8,
    pub message_len: u32,
}

pub(crate) fn invoke_callback_void(
    result: Result<(), crate::Error>,
    callback: Callback,
    user_data: u64
) {
    match result {
        Ok(_) => {
            callback(std::ptr::null(), std::ptr::null(), user_data);
        }
        Err(error) => {
            let error_info = ErrorInfo {
                code: error.code,
                message: error.message.as_ptr(),
                message_len: error.message.len() as u32,
            };
            let error_ptr = &error_info as *const ErrorInfo;
            callback(std::ptr::null(), error_ptr, user_data);
        }
    }
}

pub(crate) fn invoke_callback<T>(
    result: Result<T, crate::Error>,
    callback: Callback,
    user_data: u64
) {
    match result {
        Ok(value) => {
            let value_ref = &value;
            let result_ptr = value_ref as *const T as *const std::ffi::c_void;
            callback(result_ptr, std::ptr::null(), user_data);
        }
        Err(error) => {
            let error_info = ErrorInfo {
                code: error.code,
                message: error.message.as_ptr(),
                message_len: error.message.len() as u32,
            };
            let error_ptr = &error_info as *const ErrorInfo;
            callback(std::ptr::null(), error_ptr, user_data);
        }
    }
}
