use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default, Debug, RustcDecodable, RustcEncodable, Clone)]
pub struct TriggerInfo {
    pub run_id: i64,
    pub asondate: String,
    pub batch_id: i64,
    pub stream_id: i64,
    pub flow_id: i64,
    pub executor_id: i64,
    pub process_id: i64,
    pub process_name: String,
    pub process_binary: String,
    pub process_args: Vec<String>,
    pub process_report: String,
}

#[derive(Serialize, Deserialize, Debug, RustcDecodable, RustcEncodable, Clone)]
pub struct UpdateInfo {
    pub run_id: i64,
    pub as_on_date: String,
    pub batch_id: i64,
    pub stream_id: i64,
    pub flow_id: i64,
    pub executor_id: i64,
    pub process_id: i64,
    pub process_status: String,
    pub end_time: String,
    pub process_report: String,
}

#[derive(Serialize, Deserialize, Debug, RustcDecodable, RustcEncodable, Clone)]
struct ServerMapTable {
    pub exec_id: i64,
    pub socket_addr: String,
}
