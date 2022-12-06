use slog::Logger;

pub struct Params {
    pub con_str: String,
    pub log: Logger,
    pub diag_log: Logger,
}
