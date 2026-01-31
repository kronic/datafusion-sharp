pub struct Context {
    runtime: *const tokio::runtime::Runtime,
    inner: std::sync::Arc<datafusion::prelude::SessionContext>,
}

impl Context {
    fn new(runtime: *const tokio::runtime::Runtime) -> Self {
        Self {
            runtime,
            inner: std::sync::Arc::new(datafusion::prelude::SessionContext::new())
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn datafusion_context_new(runtime: *mut tokio::runtime::Runtime, context: *mut *mut Context) -> crate::ErrorCode {
    if runtime.is_null() || context.is_null() {
        return crate::ErrorCode::InvalidArgument;
    }

    let ctx = Box::new(Context::new(runtime));
    unsafe { *context = Box::into_raw(ctx); }

    crate::ErrorCode::Ok
}

#[unsafe(no_mangle)]
pub extern "C" fn datafusion_context_destroy(context: *mut Context) -> crate::ErrorCode {
    if !context.is_null() {
        unsafe { drop(Box::from_raw(context)) };
    }

    crate::ErrorCode::Ok
}

#[unsafe(no_mangle)]
pub extern "C" fn datafusion_context_register_csv(
    context_ptr: *mut Context,
    table_ref_ptr: *const std::ffi::c_char,
    table_path_ptr: *const std::ffi::c_char,
    callback: Option<crate::Callback>,
    callback_user_data: u64
) -> crate::ErrorCode {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
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
        let runtime = unsafe { &*context.runtime };
        let inner = std::sync::Arc::clone(&context.inner);

        runtime.spawn(async move {
            let result = inner
                .register_csv(&table_ref, &table_path, datafusion::prelude::CsvReadOptions::new())
                .await
                .map_err(|e| crate::Error {
                    code: crate::ErrorCode::TableRegistrationFailed,
                    message: e.to_string(),
                });

            crate::invoke_callback_void(result, callback, callback_user_data);
        });

        crate::ErrorCode::Ok
    }));

    result.unwrap_or_else(|err| {
        eprintln!("[DataFusionSharp] Panic in datafusion_context_register_csv: {:?}", err);
        crate::ErrorCode::Panic
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn datafusion_context_sql(
    ctx: *mut Context,
    sql: *const std::ffi::c_char,
    callback: Option<crate::Callback>,
    callback_user_data: u64
) -> crate::ErrorCode {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if ctx.is_null() || sql.is_null() {
            return crate::ErrorCode::InvalidArgument;
        }

        let Some(callback) = callback else {
            return crate::ErrorCode::InvalidArgument;
        };

        let Ok(sql_str) = unsafe { std::ffi::CStr::from_ptr(sql) }
            .to_str()
            .map(|s| s.to_string()) else {
            return crate::ErrorCode::InvalidArgument;
        };

        let context = unsafe { &*ctx };
        let runtime = unsafe { &*context.runtime };
        let inner = std::sync::Arc::clone(&context.inner);

        runtime.spawn(async move {
            let result = inner
                .sql(&sql_str)
                .await
                .map(|df| {
                    let data_frame = Box::new(crate::DataFrame::new(runtime, df));
                    Box::into_raw(data_frame)
                })
                .map_err(|e| crate::Error {
                    code: crate::ErrorCode::SqlError,
                    message: e.to_string(),
                });

            crate::invoke_callback(result, callback, callback_user_data);
        });

        crate::ErrorCode::Ok
    }));

    result.unwrap_or_else(|err| {
        eprintln!("[DataFusionSharp] Panic in datafusion_context_sql: {:?}", err);
        crate::ErrorCode::Panic
    })
}
