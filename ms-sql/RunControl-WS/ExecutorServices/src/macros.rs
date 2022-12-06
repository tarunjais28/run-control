use std::cell::RefCell;

pub struct PerfDiagParams {
    // We use RefCells here because the variable that uses this struct is `static` and needs to be mutable.
    // Using a RefCell gives us internal mutability.
    is_value_set: RefCell<bool>,
    is_perf_diagnostics_enabled: RefCell<bool>,
}

impl PerfDiagParams {
    // pub fn is_perf_diagnostics_enabled(&self) -> bool {
    //     *self.is_perf_diagnostics_enabled.borrow()
    // }

    /// This method should only be set once during the execution of the program. The value should only be set from the main thread, when you are *sure* no other thread is trying to read or write to the `is_perf_diagnostics_enabled` flag at the same time.
    ///
    /// This method includes a basic checking mechanism to ensure you can't use it more than once.
    pub fn set_once_perf_diagnostics_enabled(&self, value: bool) {
        if *self.is_value_set.borrow() == true {
            panic!("Changing the value of the `is_perf_diagnostics_enabled` is allowed only once!");
        }
        *self.is_value_set.borrow_mut() = true;
        *self.is_perf_diagnostics_enabled.borrow_mut() = value;
    }
}

// We can ask the compiler to unsafely treat this struct as `Send` and `Sync` so we're allowed to read values from multiple thread contexts.
// As long as we don't write to and read from the underlying RefCell data at the same time, the execution of the program will work correctly.
// The restrictions placed on the `set_once_perf_diagnostics_enabled` method (see its docs) ensure that multiple thread contexts only ever read values, never write to it.
unsafe impl Send for PerfDiagParams {}
unsafe impl Sync for PerfDiagParams {}

pub static PERF_PARAMS: PerfDiagParams = PerfDiagParams {
    is_value_set: RefCell::new(false),
    is_perf_diagnostics_enabled: RefCell::new(false),
};

pub struct LoggingParams {
    // We use RefCells here because the variable that uses this struct is `static` and needs to be mutable.
    // Using a RefCell gives us internal mutability.
    is_value_set: RefCell<bool>,
    is_error_enabled: RefCell<bool>,
    is_warn_enabled: RefCell<bool>,
    is_info_enabled: RefCell<bool>,
    is_debug_enabled: RefCell<bool>,
    is_trace_enabled: RefCell<bool>,
}

#[allow(dead_code)]
impl LoggingParams {
    pub fn is_error_enabled(&self) -> bool {
        *self.is_error_enabled.borrow()
    }

    pub fn is_warn_enabled(&self) -> bool {
        *self.is_warn_enabled.borrow()
    }

    pub fn is_info_enabled(&self) -> bool {
        *self.is_info_enabled.borrow()
    }

    pub fn is_debug_enabled(&self) -> bool {
        *self.is_debug_enabled.borrow()
    }

    pub fn is_trace_enabled(&self) -> bool {
        *self.is_trace_enabled.borrow()
    }

    /// This method should only be set once during the execution of the program. The value should only be set from the main thread, when you are *sure* no other thread is trying to read or write to the log level at the same time.
    ///
    /// This method includes a basic checking mechanism to ensure you can't use it more than once.
    pub fn set_once_diagnostic_level(&self, value: String) {
        if *self.is_value_set.borrow() == true {
            panic!("Changing logging level is allowed only once.");
        }

        match value.as_ref() {
            "error" => {
                *self.is_error_enabled.borrow_mut() = true;
                *self.is_warn_enabled.borrow_mut() = false;
                *self.is_info_enabled.borrow_mut() = false;
                *self.is_debug_enabled.borrow_mut() = false;
                *self.is_trace_enabled.borrow_mut() = false;
            }
            "warn" => {
                *self.is_error_enabled.borrow_mut() = true;
                *self.is_warn_enabled.borrow_mut() = true;
                *self.is_info_enabled.borrow_mut() = false;
                *self.is_debug_enabled.borrow_mut() = false;
                *self.is_trace_enabled.borrow_mut() = false;
            }
            "info" => {
                *self.is_error_enabled.borrow_mut() = true;
                *self.is_warn_enabled.borrow_mut() = true;
                *self.is_info_enabled.borrow_mut() = true;
                *self.is_debug_enabled.borrow_mut() = false;
                *self.is_trace_enabled.borrow_mut() = false;
            }
            "debug" => {
                *self.is_error_enabled.borrow_mut() = true;
                *self.is_warn_enabled.borrow_mut() = true;
                *self.is_info_enabled.borrow_mut() = true;
                *self.is_debug_enabled.borrow_mut() = true;
                *self.is_trace_enabled.borrow_mut() = false;
            }
            "trace" => {
                *self.is_error_enabled.borrow_mut() = true;
                *self.is_warn_enabled.borrow_mut() = true;
                *self.is_info_enabled.borrow_mut() = true;
                *self.is_debug_enabled.borrow_mut() = true;
                *self.is_trace_enabled.borrow_mut() = true;
            }
            "none" => {
                *self.is_error_enabled.borrow_mut() = false;
                *self.is_warn_enabled.borrow_mut() = false;
                *self.is_info_enabled.borrow_mut() = false;
                *self.is_debug_enabled.borrow_mut() = false;
                *self.is_trace_enabled.borrow_mut() = false;
            }
            _ => {
                panic!("'{}' is not an accepted diagnostic level.", value);
            }
        }

        *self.is_value_set.borrow_mut() = true;
    }
}

// We can ask the compiler to unsafely treat this struct as `Send` and `Sync` so we're allowed to read values from multiple thread contexts.
// As long as we don't write to and read from the underlying RefCell data at the same time, the execution of the program will work correctly.
// The restrictions placed on the `set_once_diagnostic_level` method (see its docs) ensure that multiple thread contexts only ever read values, never write to it.
unsafe impl Send for LoggingParams {}
unsafe impl Sync for LoggingParams {}

pub static LOG_PARAMS: LoggingParams = LoggingParams {
    is_value_set: RefCell::new(false),
    is_error_enabled: RefCell::new(true),
    is_warn_enabled: RefCell::new(true),
    is_info_enabled: RefCell::new(true),
    is_debug_enabled: RefCell::new(true),
    is_trace_enabled: RefCell::new(true),
};

#[macro_export]
macro_rules! log_measurements {
    ($l:expr, $tags:expr, $f:expr) => {
        if macros::PERF_PARAMS.is_perf_diagnostics_enabled() {
            let mut tags = String::new();

            for tag in $tags.iter() {
                tags.push_str(" ");
                tags.push_str(tag);
            }

            let start = SystemTime::now();
            let res = $f;
            let duration = SystemTime::now().duration_since(start).unwrap();

            info!($l, "{}: {}", tags, duration.as_nanos());
            res
        } else {
            $f
        }
    };
}

#[macro_export]
macro_rules! accumulate_time_duration {
    ($duration:expr, $f:expr) => {{
        let start = SystemTime::now();
        let res = $f;
        let end = SystemTime::now();
        let duration = end
            .duration_since(start)
            .expect("Couldn't calculate time between start and end times");

        $duration += duration;

        res
    }};
}

#[macro_export]
macro_rules! log_error {
    ($l:expr, #$tag:expr, $($args:tt)+) => {
        if macros::LOG_PARAMS.is_error_enabled() {
            error!($l, $tag, $($args)+);
        }
    };
    ($l:expr, $($args:tt)+) => {
        if macros::LOG_PARAMS.is_error_enabled() {
            error!($l, $($args)+);
        }
    };
}

#[macro_export]
macro_rules! log_warn {
    ($l:expr, #$tag:expr, $($args:tt)+) => {
        if macros::LOG_PARAMS.is_warn_enabled() {
            warn!($l, $tag, $($args)+);
        }
    };
    ($l:expr, $($args:tt)+) => {
        if macros::LOG_PARAMS.is_warn_enabled() {
            warn!($l, $($args)+);
        }
    };
}

#[macro_export]
macro_rules! log_info {
    ($l:expr, #$tag:expr, $($args:tt)+) => {
        if macros::LOG_PARAMS.is_info_enabled() {
            info!($l, $tag, $($args)+);
        }
    };
    ($l:expr, $($args:tt)+) => {
        if macros::LOG_PARAMS.is_info_enabled() {
            info!($l, $($args)+);
        }
    };
}

#[macro_export]
macro_rules! log_debug {
    ($l:expr, #$tag:expr, $($args:tt)+) => {
        if macros::LOG_PARAMS.is_debug_enabled() {
            debug!($l, $tag, $($args)+);
        }
    };
    ($l:expr, $($args:tt)+) => {
        if macros::LOG_PARAMS.is_debug_enabled() {
            debug!($l, $($args)+);
        }
    };
}

#[macro_export]
macro_rules! log_trace {
    ($l:expr, #$tag:expr, $($args:tt)+) => {
        if macros::LOG_PARAMS.is_trace_enabled() {
            trace!($l, $tag, $($args)+);
        }
    };
    ($l:expr, $($args:tt)+) => {
        if macros::LOG_PARAMS.is_trace_enabled() {
            trace!($l, $($args)+);
        }
    };
}
