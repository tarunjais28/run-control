use slog::Logger;

pub struct Params {
    pub orch_url: String,
    pub log: Logger,
    pub diag_log: Logger,
}
