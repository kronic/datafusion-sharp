pub struct SessionContextWrapper {
    runtime: crate::RuntimeHandle,
    inner: std::sync::Arc<datafusion::prelude::SessionContext>
}

impl SessionContextWrapper {
    fn new(runtime: crate::RuntimeHandle) -> Self {
        Self {
            runtime,
            inner: std::sync::Arc::new(datafusion::prelude::SessionContext::new())
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn datafusion_context_new(runtime_ptr: *mut crate::RuntimeHandle, context_ptr: *mut *mut SessionContextWrapper) -> crate::ErrorCode {
    if runtime_ptr.is_null() || context_ptr.is_null() {
        return crate::ErrorCode::InvalidArgument;
    }

    let runtime_handle = unsafe { &*runtime_ptr };

    let ctx = Box::new(SessionContextWrapper::new(std::sync::Arc::clone(&*runtime_handle)));
    unsafe { *context_ptr = Box::into_raw(ctx); }

    dev_msg!("Successfully created context: {:p}", unsafe { *context_ptr });

    crate::ErrorCode::Ok
}

#[unsafe(no_mangle)]
pub extern "C" fn datafusion_context_destroy(context_ptr: *mut SessionContextWrapper) -> crate::ErrorCode {
    dev_msg!("Destroying context: {:p}", context_ptr);

    if !context_ptr.is_null() {
        unsafe { drop(Box::from_raw(context_ptr)) };
    }

    crate::ErrorCode::Ok
}

#[unsafe(no_mangle)]
pub extern "C" fn datafusion_context_register_csv(
    context_ptr: *mut SessionContextWrapper,
    table_ref_ptr: *const std::ffi::c_char,
    table_path_ptr: *const std::ffi::c_char,
    callback: Option<crate::Callback>,
    callback_ud: u64
) -> crate::ErrorCode {
    if context_ptr.is_null() || table_ref_ptr.is_null() || table_path_ptr.is_null() {
        return crate::ErrorCode::InvalidArgument;
    }

    let Some(callback) = callback else {
        return crate::ErrorCode::InvalidArgument;
    };

    let Ok(table_ref) = unsafe { std::ffi::CStr::from_ptr(table_ref_ptr) }
        .to_str()
        .map(|s| s.to_string()) else {
        return crate::ErrorCode::InvalidArgument;
    };

    let Ok(table_path) = unsafe { std::ffi::CStr::from_ptr(table_path_ptr) }
        .to_str()
        .map(|s| s.to_string()) else {
        return crate::ErrorCode::InvalidArgument;
    };

    let context = unsafe { &*context_ptr };
    let runtime = std::sync::Arc::clone(&context.runtime);
    let inner = std::sync::Arc::clone(&context.inner);

    dev_msg!("Registering CSV table '{}' from path '{}'", table_ref, table_path);

    runtime.spawn(async move {
        let result = inner
            .register_csv(&table_ref, &table_path, datafusion::prelude::CsvReadOptions::new())
            .await
            .map_err(|e| crate::ErrorInfo::new(crate::ErrorCode::TableRegistrationFailed, e));

        dev_msg!("Finished registering CSV table '{}' from path '{}'", table_ref, table_path);

        crate::invoke_callback(result, callback, callback_ud);
    });

    crate::ErrorCode::Ok
}

#[unsafe(no_mangle)]
pub extern "C" fn datafusion_context_sql(
    context_ptr: *mut SessionContextWrapper,
    sql_ptr: *const std::ffi::c_char,
    callback: Option<crate::Callback>,
    callback_user_data: u64
) -> crate::ErrorCode {
    if context_ptr.is_null() || sql_ptr.is_null() {
        return crate::ErrorCode::InvalidArgument;
    }

    let Some(callback) = callback else {
        return crate::ErrorCode::InvalidArgument;
    };

    let Ok(sql) = unsafe { std::ffi::CStr::from_ptr(sql_ptr) }
        .to_str()
        .map(|s| s.to_string()) else {
        return crate::ErrorCode::InvalidArgument;
    };

    let context = unsafe { &*context_ptr };
    let runtime = std::sync::Arc::clone(&context.runtime);
    let inner = std::sync::Arc::clone(&context.inner);

    dev_msg!("Executing SQL query: {}", sql);

    runtime.spawn(async move {
        let result = inner
            .sql(&sql)
            .await
            .map(|dataframe| {
                let data_frame = Box::new(crate::DataFrameWrapper::new(std::sync::Arc::clone(&context.runtime), dataframe));
                Box::into_raw(data_frame)
            })
            .map_err(|e| crate::ErrorInfo::new(crate::ErrorCode::SqlError, e));

        dev_msg!("Finished executing SQL query: {}, dataframe ptr: {:p}", sql, result.as_ref().ok().map_or(std::ptr::null(), |ptr| *ptr));

        crate::invoke_callback(result, callback, callback_user_data);
    });

    crate::ErrorCode::Ok
}
