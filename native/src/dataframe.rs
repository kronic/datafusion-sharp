pub struct DataFrameWrapper {
    runtime: crate::RuntimeHandle,
    inner: datafusion::prelude::DataFrame,
}

impl DataFrameWrapper {
    pub fn new(runtime: crate::RuntimeHandle, inner: datafusion::prelude::DataFrame) -> Self {
        Self {
            runtime,
            inner,
        }
    }
}

/// Destroys a `DataFrame` and frees its resources.
///
/// # Safety
/// - `df_ptr` must be a valid pointer returned by other public functions, or null
/// - Caller must not use `df_ptr` after this call
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_dataframe_destroy(df_ptr: *mut DataFrameWrapper) -> crate::ErrorCode {
    if !df_ptr.is_null() {
        unsafe { drop(Box::from_raw(df_ptr)) };
    }

    crate::ErrorCode::Ok
}

/// Counts the number of rows in the `DataFrame`.
///
/// This is an async operation. The callback is invoked on completion with the row count as u64.
///
/// # Safety
/// - `df_ptr` must be a valid pointer returned by other public functions
/// - `callback` must be valid to call from any thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_dataframe_count(
    df_ptr: *mut DataFrameWrapper,
    callback: crate::Callback,
    user_data: u64
) -> crate::ErrorCode {
    let df_wrapper = ffi_ref!(df_ptr);

    let runtime = std::sync::Arc::clone(&df_wrapper.runtime);
    let df = df_wrapper.inner.clone();

    dev_msg!("Executing count on DataFrame: {:p}", df_ptr);

    runtime.spawn(async move {
        let result = df
            .count()
            .await
            .map_err(|e| crate::ErrorInfo::new(crate::ErrorCode::DataFrameError, e))
            .map(|s| s as u64);

        crate::invoke_callback(result, callback, user_data);
    });

    crate::ErrorCode::Ok
}

/// Prints the `DataFrame` contents to stdout.
///
/// This is an async operation. The callback is invoked on completion with no result data.
///
/// # Safety
/// - `df_ptr` must be a valid pointer returned by other public functions
/// - `callback` must be valid to call from any thread
///
/// # Parameters
/// - `limit`: Maximum number of rows to display (0 = no limit)
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_dataframe_show(
    df_ptr: *mut DataFrameWrapper,
    limit: u64,
    callback: crate::Callback,
    user_data: u64
) -> crate::ErrorCode {
    let df_wrapper = ffi_ref!(df_ptr);

    let runtime = std::sync::Arc::clone(&df_wrapper.runtime);
    let df = df_wrapper.inner.clone();

    dev_msg!("Executing show on DataFrame: {:p}", df_ptr);

    runtime.spawn(async move {
        let result = if limit > 0 {
            #[allow(clippy::cast_possible_truncation)]
            df.show_limit(limit as usize).await
        } else {
            df.show().await
        }.map_err(|e| crate::ErrorInfo::new(crate::ErrorCode::DataFrameError, e));

        crate::invoke_callback(result, callback, user_data);
    });

    crate::ErrorCode::Ok
}

/// Converts the `DataFrame` to a string representation.
///
/// This is an async operation. The callback is invoked on completion with the string as bytes.
///
/// # Safety
/// - `df_ptr` must be a valid pointer returned by other public functions
/// - `callback` must be valid to call from any thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_dataframe_to_string(
    df_ptr: *mut DataFrameWrapper,
    callback: crate::Callback,
    user_data: u64
) -> crate::ErrorCode {
    let df_wrapper = ffi_ref!(df_ptr);

    let runtime = std::sync::Arc::clone(&df_wrapper.runtime);
    let df = df_wrapper.inner.clone();

    dev_msg!("Executing to_string on DataFrame: {:p}", df_ptr);

    runtime.spawn(async move {
        let result = df
            .to_string()
            .await;

        match result {
            Ok(s) => {
                let data = crate::callback::BytesData::new(s.as_bytes());
                crate::invoke_callback(Ok(data), callback, user_data);
            }
            Err(err) => {
                let err_info = crate::ErrorInfo::new(crate::ErrorCode::DataFrameError, err);
                crate::invoke_callback(Err::<crate::callback::BytesData, _>(err_info), callback, user_data);
            }
        }
    });

    crate::ErrorCode::Ok
}

/// Returns the `DataFrame` schema as a serialized Arrow IPC stream.
///
/// This is a synchronous operation. The callback is invoked immediately with the schema bytes.
///
/// # Safety
/// - `df_ptr` must be a valid pointer returned by other public functions
/// - `callback` must be valid to call from the current thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_dataframe_schema(
    df_ptr: *mut DataFrameWrapper,
    callback: crate::Callback,
    user_data: u64
) -> crate::ErrorCode {
    let df_wrapper = ffi_ref!(df_ptr);

    dev_msg!("Executing schema on DataFrame: {:p}", df_ptr);

    let df = &df_wrapper.inner;
    let schema = df.schema().as_arrow();

    let mut serialized_data = Vec::new();

    let result = datafusion::arrow::ipc::writer::StreamWriter::try_new(&mut serialized_data, schema)
        .and_then(|mut s| s.flush())
        .map(|()| crate::callback::BytesData::new(serialized_data.as_slice()))
        .map_err(|e| crate::ErrorInfo::new(crate::ErrorCode::DataFrameError, e));

    dev_msg!("Finished executing schema on DataFrame: {:p}, schema size: {}", df_ptr, serialized_data.len());

    crate::invoke_callback(result, callback, user_data);

    crate::ErrorCode::Ok
}

/// Materializes all records as a serialized Arrow IPC stream.
///
/// This is an async operation. The callback is invoked on completion with the serialized bytes.
///
/// # Safety
/// - `df_ptr` must be a valid pointer returned by other public functions
/// - `callback` must be valid to call from any thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_dataframe_collect(
    df_ptr: *mut DataFrameWrapper,
    callback: crate::Callback,
    user_data: u64
) -> crate::ErrorCode {
    let df_wrapper = ffi_ref!(df_ptr);

    let runtime = std::sync::Arc::clone(&df_wrapper.runtime);
    let df = df_wrapper.inner.clone();

    dev_msg!("Executing collect on DataFrame: {:p}", df_ptr);

    runtime.spawn(async move {
        let mut serialized_data = Vec::new();

        let schema = df.schema().as_arrow();
        let result = match datafusion::arrow::ipc::writer::StreamWriter::try_new(&mut serialized_data, schema) {
            Ok(mut s) => {
                df
                    .collect()
                    .await
                    .map(|batches| -> datafusion::error::Result<()> {
                        for batch in batches {
                            s.write(&batch)?;
                        }
                        Ok(())
                    })
                    .map(|_| s.flush())
                    .map(|_| crate::callback::BytesData::new(serialized_data.as_slice()))
                    .map_err(|e| crate::ErrorInfo::new(crate::ErrorCode::DataFrameError, e))
            }
            Err(e) => {
                Err(crate::ErrorInfo::new(crate::ErrorCode::DataFrameError, e))
            }
        };

        dev_msg!("Finished executing collect, serialized size: {}", serialized_data.len());

        crate::invoke_callback(result, callback, user_data);
    });

    crate::ErrorCode::Ok
}