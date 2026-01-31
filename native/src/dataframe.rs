pub struct DataFrame {
    runtime: *const tokio::runtime::Runtime,
    inner: datafusion::prelude::DataFrame,
}

impl DataFrame {
    pub fn new(runtime: *const tokio::runtime::Runtime, inner: datafusion::prelude::DataFrame) -> Self {
        Self {
            runtime,
            inner,
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn datafusion_dataframe_destroy(df: *mut DataFrame) -> crate::ErrorCode {
    if !df.is_null() {
        unsafe { drop(Box::from_raw(df)) };
    }

    crate::ErrorCode::Ok
}

#[unsafe(no_mangle)]
pub extern "C" fn datafusion_dataframe_count(
    df: *mut DataFrame,
    callback: Option<crate::Callback>,
    callback_user_data: u64
) -> crate::ErrorCode {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if df.is_null() {
            return crate::ErrorCode::InvalidArgument;
        }

        let Some(callback) = callback else {
            return crate::ErrorCode::InvalidArgument;
        };

        let dataframe = unsafe { &*df };
        let runtime = unsafe { &*dataframe.runtime };
        let inner = dataframe.inner.clone();

        runtime.spawn(async move {
            let result = inner
                .count()
                .await
                .map_err(|e| crate::Error {
                    code: crate::ErrorCode::SqlError,
                    message: e.to_string(),
                })
                .map(|s| s as u64);

            crate::invoke_callback(result, callback, callback_user_data);
        });

        crate::ErrorCode::Ok
    }));

    result.unwrap_or_else(|err| {
        eprintln!("[DataFusionSharp] Panic in datafusion_dataframe_count: {:?}", err);
        crate::ErrorCode::Panic
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn datafusion_dataframe_show(
    df: *mut DataFrame,
    callback: Option<crate::Callback>,
    callback_user_data: u64
) -> crate::ErrorCode {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if df.is_null() {
            return crate::ErrorCode::InvalidArgument;
        }

        let Some(callback) = callback else {
            return crate::ErrorCode::InvalidArgument;
        };

        let dataframe = unsafe { &*df };
        let runtime = unsafe { &*dataframe.runtime };
        let inner = dataframe.inner.clone();

        runtime.spawn(async move {
            let result = inner
                .show()
                .await
                .map_err(|e| crate::Error {
                    code: crate::ErrorCode::SqlError,
                    message: e.to_string(),
                });

            crate::invoke_callback_void(result, callback, callback_user_data);
        });

        crate::ErrorCode::Ok
    }));

    result.unwrap_or_else(|err| {
        eprintln!("[DataFusionSharp] Panic in datafusion_dataframe_show: {:?}", err);
        crate::ErrorCode::Panic
    })
}