use crate::ErrorCode;

/// Creates a new Tokio multi-threaded runtime for DataFusion.
///
/// # Safety
/// - `runtime` must be a valid, aligned, non-null pointer to writable memory
/// - Caller must call `datafusion_runtime_destroy` exactly once with the returned pointer
///
/// # Parameters
/// - `worker_threads`: Number of worker threads (0 = automatic)
/// - `max_blocking_threads`: Max blocking threads (0 = automatic)
/// - `runtime`: Output pointer to receive the runtime pointer
#[unsafe(no_mangle)]
pub extern "C" fn datafusion_runtime_new(
    worker_threads: u32,
    max_blocking_threads: u32,
    runtime: *mut *mut tokio::runtime::Runtime) -> ErrorCode {
    if runtime.is_null() {
        return ErrorCode::InvalidArgument;
    }

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut builder = tokio::runtime::Builder::new_multi_thread();

        if worker_threads > 0 {
            builder.worker_threads(worker_threads as usize);
        }

        if max_blocking_threads > 0 {
            builder.max_blocking_threads(max_blocking_threads as usize);
        }

        builder.enable_all();

        match builder.build() {
            Ok(rt) => {
                unsafe { *runtime = Box::into_raw(Box::new(rt)); }
                ErrorCode::Ok
            }
            Err(err) => {
                eprintln!("[DataFusionSharp] Failed to initialize Tokio runtime: {}", err);
                ErrorCode::RuntimeInitializationFailed
            },
        }
    }));

    result.unwrap_or_else(|err| {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            eprintln!("[DataFusionSharp] Panic during runtime initialization: {:?}", err);
        }));
        ErrorCode::Panic
    })
}

/// Destroys a Tokio runtime created by `datafusion_runtime_new`.
///
/// Shuts down the runtime, waiting up to `timeout_millis` milliseconds for tasks to complete.
///
/// # Safety
/// - `runtime` must be a valid pointer returned by `datafusion_runtime_new`
/// - Caller must not use `runtime` after this call
///
/// # Parameters
/// - `runtime`: Pointer to the runtime to destroy
/// - `timeout_millis`: Maximum time to wait for shutdown
#[unsafe(no_mangle)]
pub extern "C" fn datafusion_runtime_destroy(runtime: *mut tokio::runtime::Runtime, timeout_millis: u64) -> ErrorCode {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if runtime.is_null() {
            return ErrorCode::Ok;
        }

        let rt = unsafe { Box::from_raw(runtime) };
        rt.shutdown_timeout(std::time::Duration::from_millis(timeout_millis));

        ErrorCode::Ok
    }));

    result.unwrap_or_else(|err| {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            eprintln!("[DataFusionSharp] Panic during runtime shutdown: {:?}", err);
        }));
        ErrorCode::Panic
    })
}