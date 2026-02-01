#[cfg(debug_assertions)]
#[macro_export]
macro_rules! dev_msg {
    ($($arg:tt)*) => {
        eprintln!("[datafusion-sharp-native]({}) ({}:{}) {}",
            chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, false),
            file!(),
            line!(),
            format!($($arg)*));
    };
}

#[cfg(not(debug_assertions))]
macro_rules! dev_msg {
    ($($arg:tt)*) => {};
}
