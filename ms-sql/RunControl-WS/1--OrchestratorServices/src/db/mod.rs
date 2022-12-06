use super::*;
use crate::{
    handlers::{update::HealthReport, TriggerInfo},
    trigger::{Bullet, FlowDef, ProcDef, StreamDef},
};
use actix_web::{
    get, post,
    web::{Data, Json, Path},
    Error, HttpResponse, Result,
};
use chrono::Local;
use chrono::{DateTime, NaiveDateTime, Utc};
use odbc::*;
use rustc_serialize::json;
use serde::{Deserialize, Serialize};
use serde_json;
use std::str;

#[derive(Serialize, Deserialize, Debug, RustcDecodable, RustcEncodable, Clone)]
struct FlowFullStatus {
    flow_id: i64,
    flow_name: String,
    flow_start_time: String,
    flow_end_time: String,
    flow_status: String,
    tt_by_flow_secs: i64,
    flow_error_msg: String,
    process: Vec<ProcessFullStatus>,
}

#[derive(Serialize, Deserialize, Debug, RustcDecodable, RustcEncodable, Clone)]
struct ProcessFullStatus {
    process_id: i64,
    process_name: String,
    process_start_time: String,
    process_end_time: String,
    process_status: String,
    process_error_msg: String,
    tt_by_process_secs: i64,
    total_num_recs: i64,
    success_process_recs: i64,
    failed_process_recs: i64,
    principal_input_amt: f64,
    principal_output_amt: f64,
    interest_output_amt: f64,
}

#[derive(Serialize, Deserialize, Debug, RustcDecodable, RustcEncodable, Clone)]
struct BatchFullStatus {
    batch_id: i64,
    as_on_date: String,
    streams: Vec<StreamFullStatus>,
}

#[derive(Serialize, Deserialize, Debug, RustcDecodable, RustcEncodable, Clone)]
struct StreamFullStatus {
    stream_id: i64,
    stream_name: String,
    stream_start_time: String,
    stream_end_time: String,
    stream_status: String,
    stream_lock_status: String,
    tt_by_stream_secs: i64,
    stream_error_msg: String,
    flows: Vec<FlowFullStatus>,
}

#[derive(Serialize, Deserialize, Debug, RustcDecodable, RustcEncodable, Clone)]
pub struct Dep {
    dependencies: Vec<i64>,
}

#[derive(Serialize, Deserialize, Debug, RustcDecodable, RustcEncodable, Clone)]
pub struct AsOn {
    as_on_date: String,
}

#[derive(Serialize, Deserialize, Debug, RustcDecodable, RustcEncodable, Clone)]
pub struct Batch {
    batch_id: i64,
    batch_name: String,
}

#[derive(Serialize, Deserialize, Debug, RustcDecodable, RustcEncodable, Clone)]
pub struct StatusReq {
    batch_id: i64,
}

#[derive(Serialize, Deserialize, Debug, RustcDecodable, RustcEncodable, Clone)]
pub struct Stream {
    stream_id: i64,
    stream_name: String,
}

#[derive(Serialize, Deserialize, Debug, RustcDecodable, RustcEncodable, Clone)]
pub struct RunStatus {
    pub batch_id: i64,
    pub as_on_date: String,
    pub streams: Vec<StreamStatus>,
}

#[derive(Serialize, Deserialize, Debug, RustcDecodable, RustcEncodable, Clone)]
pub struct StreamStatus {
    pub stream_id: i64,
    pub status: String,
}

#[derive(Serialize, Deserialize, Debug, RustcDecodable, RustcEncodable, Clone)]
pub struct RunReqStatus {
    pub batch_id: i64,
    pub as_on_date: String,
}

#[derive(Serialize, Deserialize, Debug, RustcDecodable, RustcEncodable, Clone)]
pub struct Fail {
    pub run_id: i64,
    pub asondate: String,
    pub batch_id: i64,
    pub stream_id: i64,
    pub flow_id: i64,
    pub process_id: i64,
}
// TODO: create a method for getting connection
/*
let env = create_environment_v3().map_err(|e| e.unwrap()).expect("Cannot create DB environment.");
let conn = env.connect_with_connection_string(&con_str).expect("Cannot establish a DB connection.");
let stmt = Statement::with_parent(&conn).expect("Cannot create a statement instance to run queries.");
*/
// Getting invalid lifetime error: Omiting now to due to time constraint

pub fn get_last_assigned_run_id(pool_id: i64, pool: Data<Params>) -> Result<i64, &'static str> {
    let env = create_environment_v3()
        .map_err(|e| e.unwrap())
        .expect("Cannot create DB environment.");
    let conn = env
        .connect_with_connection_string(&pool.con_str)
        .expect("Cannot establish a DB connection.");
    let stmt =
        Statement::with_parent(&conn).expect("Cannot create a statement instance to run queries.");
    let sql_cmd = format!(
        "select max(RunID) as RunID from RunControl where PoolID = {};",
        pool_id
    );
    let mut result = 0;
    match stmt
        .exec_direct(&sql_cmd)
        .expect("Failed to fetch max(RunID) from RunControl table!!")
    {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch().expect("Cannot read output of query.") {
                match cursor
                    .get_data::<&str>(1)
                    .expect("Cannot read row data from query.")
                {
                    Some(val) => {
                        result = val.parse::<i64>().unwrap_or(0);
                    }
                    None => {
                        log_debug!(
                            pool.diag_log,
                            "No runs initiated till now or Run details are not available."
                        );
                        // Default value 0
                        result = "0".parse::<i64>().unwrap_or(0);
                    }
                }
            }
        }
        NoData(_) => {
            log_debug!(
                pool.diag_log,
                "11. Query \"{}\" executed, no data returned",
                sql_cmd
            );
        }
    }
    return Ok(result);
}

pub fn clear_last_run_det(trigger_info: TriggerInfo, poolid: i64, pool: Data<Params>) {
    let env = create_environment_v3()
        .map_err(|e| e.unwrap())
        .expect("Cannot create DB environment.");
    let conn = env
        .connect_with_connection_string(&pool.con_str)
        .expect("Cannot establish a DB connection.");

    let as_on_date: chrono::NaiveDateTime =
        chrono::NaiveDate::parse_from_str(&trigger_info.trigger.as_on_date, "%d-%m-%Y")
            .expect("Cannot parse as on date as NaiveDate")
            .and_hms(0, 0, 0);
    for id in trigger_info.trigger.stream_ids {
        let stmt = Statement::with_parent(&conn)
            .expect("Cannot create a statement instance to run queries.");
        let sql_cmd = format!("DELETE from RunControl where AsOnDate = {} AND BatchID = {} AND StreamID = {} AND PoolID = {};", as_on_date.timestamp(), trigger_info.trigger.batch_id, id, poolid);
        match stmt
            .exec_direct(&sql_cmd)
            .expect("Failed to execute a sql cmd!!")
        {
            Data(_) => {}
            NoData(_) => {
                log_debug!(
                    pool.diag_log,
                    "12. Query \"{}\" executed, no data returned",
                    sql_cmd
                );
            }
        }
    }
}

// returns id of stream which can be triggered immediately
pub fn get_triggerable_stream_ids(
    pool_id: i64,
    trigger_info: TriggerInfo,
    pool: Data<Params>,
) -> Vec<i64> {
    let mut triggerable_stream: Vec<i64> = Vec::new();
    for id in trigger_info.trigger.stream_ids.clone() {
        let env = create_environment_v3()
            .map_err(|e| e.unwrap())
            .expect("Cannot create DB environment.");
        let conn = env
            .connect_with_connection_string(&pool.con_str)
            .expect("Cannot establish a DB connection.");
        let stmt = Statement::with_parent(&conn)
            .expect("Cannot create a statement instance to run queries.");
        let sql_cmd = format!(
                "select cast(StreamDep as varchar(max)) from BATCHSTREAM where BatchID = {} AND StreamID = {} AND IsActive = 'Y';",
                trigger_info.trigger.batch_id, id
            );
        match stmt
            .exec_direct(&sql_cmd)
            .expect("Failed to execute a sql cmd!!")
        {
            Data(mut stmt) => {
                while let Some(mut cursor) = stmt.fetch().expect("Cannot read output of query.") {
                    match cursor
                        .get_data::<&str>(1)
                        .expect("Cannot read row data from query.")
                    {
                        Some(val) => {
                            let dep: Dep = serde_json::from_str(val)
                                .expect("Cannot deserialize StreamDep Json.");
                            if dep.dependencies.len() == 0 {
                                triggerable_stream.push(id)
                            } else {
                                let mut status = true;
                                for dep in dep.dependencies {
                                    // if "true" means the status is "SUCCESS"
                                    if get_stream_status(
                                        pool_id,
                                        trigger_info.clone(),
                                        dep,
                                        "SUCCESS".to_string(),
                                        pool.clone(),
                                    ) {
                                        continue;
                                    } else {
                                        status = false;
                                    }
                                }
                                if status {
                                    triggerable_stream.push(id)
                                }
                            }
                        }
                        None => {
                            panic!("Cannot Fetch Column details from row result.");
                        }
                    }
                }
            }
            NoData(_) => {
                log_debug!(
                    pool.diag_log,
                    "9. Query \"{}\" executed, no data returned",
                    sql_cmd
                );
            }
        };
    }
    triggerable_stream
}

// returns stream desc from db
pub fn get_stream_desc(asondate: String, id: i64, pool: Data<Params>) -> Option<StreamDef> {
    let ason = chrono::NaiveDate::parse_from_str(&asondate, "%d-%m-%Y")
        .expect("Cannot parse ason date as NaiveDate.");
    let ason_dd_mm_yyyy = asondate.clone();
    let mut ason_ddmmyyyy = asondate;
    let ason_ddmonyy = ason.format("%d%b%y").to_string();
    let ason_yyyy_mon_dd = ason.format("%Y_%b_%d").to_string();
    let ason_mon_yyyy = ason.format("%b_%Y").to_string();
    let ason_yyyymmdd = ason.format("%Y%m%d").to_string();
    let ason_mmyyyy = ason.format("%m%Y").to_string();
    let timestamp = Local::now()
        .naive_local()
        .format("%d%m%Y_%H%M%S")
        .to_string();
    ason_ddmmyyyy.retain(|v| v != '-');
    let mut desc: Option<StreamDef> = None;
    let env = create_environment_v3()
        .map_err(|e| e.unwrap())
        .expect("Cannot create DB environment.");
    let conn = env
        .connect_with_connection_string(&pool.con_str)
        .expect("Cannot establish a DB connection.");
    let stmt =
        Statement::with_parent(&conn).expect("Cannot create a statement instance to run queries.");
    let sql_cmd = format!(
        "select cast(StreamDesc as varchar(max)) from StreamDef where StreamID = {}",
        id
    );
    match stmt
        .exec_direct(&sql_cmd)
        .expect("Failed to execute a sql cmd!!")
    {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch().expect("Cannot read output of query.") {
                match cursor
                    .get_data::<&str>(1)
                    .expect("Cannot read row data from query.")
                {
                    Some(val) => {
                        let stream_desc_ason = &val
                            .replace("{AsOnDate}", &ason_dd_mm_yyyy)
                            .replace("{root}", "/")
                            .replace("{ddmmyyyy}", &ason_ddmmyyyy)
                            .replace("{ddmonyy}", &ason_ddmonyy)
                            .replace("{yyyy_mon_dd}", &ason_yyyy_mon_dd)
                            .replace("{mon_yyyy}", &ason_mon_yyyy)
                            .replace("{mmyyyy}", &ason_mmyyyy)
                            .replace("{yyyymmdd}", &ason_yyyymmdd)
                            .replace("{ddmmyyyy_hhmmss}", &timestamp);

                        desc = Some(
                            serde_json::from_str(stream_desc_ason)
                                .expect("Cannot deserialize fetched stream desc."),
                        )
                    }
                    None => {
                        panic!("Cannot Fetch Column details from row result.");
                    }
                }
            }
        }
        NoData(_) => {
            log_debug!(
                pool.diag_log,
                "13. Query \"{}\" executed, no data returned",
                sql_cmd
            );
        }
    }
    desc
}

// returns id of flows which can be triggered immediately
pub fn get_triggerable_flow_ids(
    pool_id: i64,
    stream: StreamDef,
    trigger: TriggerInfo,
    pool: Data<Params>,
) -> Vec<i64> {
    let mut triggerable_flows: Vec<i64> = Vec::new();
    let s_id: i64 = stream
        .streamId
        .parse::<i64>()
        .expect("Cannot parse StreamID to integer.");
    for flow in &stream.flows {
        let mut status = true;
        for dep in &flow.flowDependencies {
            let id: i64 = dep
                .parse::<i64>()
                .expect("Cannot parse dep in flow description.");
            let dep_flow_desc: FlowDef = get_flow_desc_using_id(stream.clone(), id)
                .expect("Cannot fetch flow description using passed flow id.");
            if !get_flow_status(
                pool_id,
                trigger.clone(),
                dep_flow_desc,
                s_id,
                "SUCCESS".to_string(),
                pool.clone(),
            ) {
                status = false;
            }
        }
        if status {
            let flow_id: i64 = flow
                .flowId
                .parse::<i64>()
                .expect("Cannot parse flow id in flow description.");
            triggerable_flows.push(flow_id);
        }
    }
    triggerable_flows
}

// returns id of process which can be triggered immediately
pub fn get_triggerable_process_ids(
    pool_id: i64,
    s_id: i64,
    flow: FlowDef,
    trigger: TriggerInfo,
    pool: Data<Params>,
) -> Vec<i64> {
    let mut triggerable_procs: Vec<i64> = Vec::new();
    let f_id: i64 = flow
        .flowId
        .parse::<i64>()
        .expect("Cannot parse flowId to integer.");
    for process in &flow.process {
        let mut status = true;
        for dep in &process.processDependencies {
            let id: i64 = dep
                .parse::<i64>()
                .expect("Cannot parse dep in process description.");
            let dep_proc_desc: ProcDef = get_proc_desc_using_id(flow.clone(), id)
                .expect("Cannot fetch process description using passed process id.");
            let p_id: i64 = dep_proc_desc
                .processId
                .parse::<i64>()
                .expect("Cannot parse processId in process description.");
            if !get_process_status(
                pool_id,
                trigger.clone(),
                p_id,
                f_id,
                s_id,
                "SUCCESS".to_string(),
                pool.clone(),
            ) {
                status = false;
            }
        }
        if status {
            let proc_id: i64 = process
                .processId
                .parse::<i64>()
                .expect("Cannot parse processId in process description.");
            triggerable_procs.push(proc_id);
        }
    }
    triggerable_procs
}

// returns status of a stream "true" if "SUCCESS" else "false"
pub fn get_stream_status(
    pool_id: i64,
    trigger_info: TriggerInfo,
    s_id: i64,
    key_status: String,
    pool: Data<Params>,
) -> bool {
    let mut status = true;
    let stream: StreamDef =
        get_stream_desc(trigger_info.trigger.as_on_date.clone(), s_id, pool.clone())
            .expect("Cannot fetch stream description from db.");
    let s_id: i64 = stream
        .streamId
        .parse::<i64>()
        .expect("Cannot parse process id in flow description.");
    for flow in stream.flows {
        if !get_flow_status(
            pool_id,
            trigger_info.clone(),
            flow,
            s_id,
            key_status.clone(),
            pool.clone(),
        ) {
            status = false;
        }
    }
    status
}

// returns status of a flow "true" if "SUCCESS" else "false"
pub fn get_flow_status(
    pool_id: i64,
    trigger_info: TriggerInfo,
    flow: FlowDef,
    s_id: i64,
    key_status: String,
    pool: Data<Params>,
) -> bool {
    let mut status = true;
    let flow_id: i64 = flow
        .flowId
        .parse::<i64>()
        .expect("Cannot parse process id in flow description.");
    for process in flow.process {
        let process_id: i64 = process
            .processId
            .parse::<i64>()
            .expect("Cannot parse process id in process description.");
        if !get_process_status(
            pool_id,
            trigger_info.clone(),
            process_id,
            flow_id,
            s_id,
            key_status.clone(),
            pool.clone(),
        ) {
            status = false;
        }
    }
    status
}

// returns status of a process "true" if "SUCCESS" else "false"
pub fn get_process_status(
    pool_id: i64,
    trigger_info: TriggerInfo,
    p_id: i64,
    f_id: i64,
    s_id: i64,
    key_status: String,
    pool: Data<Params>,
) -> bool {
    let env = create_environment_v3()
        .map_err(|e| e.unwrap())
        .expect("Cannot create DB environment.");
    let conn = env
        .connect_with_connection_string(&pool.con_str)
        .expect("Cannot establish a DB connection.");
    let stmt =
        Statement::with_parent(&conn).expect("Cannot create a statement instance to run queries.");
    let mut status = false;
    let as_on_date =
        chrono::NaiveDate::parse_from_str(&trigger_info.trigger.as_on_date, "%d-%m-%Y")
            .expect("Cannot parse string as NaiveDate")
            .and_hms(0, 0, 0);
    let sql_cmd = format!("select ProcessStatus from RunControl where BatchID = {} AND StreamID = {} AND FlowID = {} AND ProcessID = {} AND AsOnDate = {} AND PoolId = {};", 
            trigger_info.trigger.batch_id.to_string(),
            s_id.to_string(),
            f_id.to_string(),
            p_id.to_string(),
            as_on_date.timestamp(),
            pool_id,
        );
    match stmt
        .exec_direct(&sql_cmd)
        .expect("Failed to execute a sql cmd!!")
    {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch().expect("Cannot read output of query.") {
                match cursor
                    .get_data::<&str>(1)
                    .expect("Cannot read row data from query.")
                {
                    Some(val) => {
                        if val == key_status {
                            status = true;
                        }
                    }
                    None => {}
                }
            }
        }
        NoData(_) => {
            log_debug!(
                pool.diag_log,
                "14. Query \"{}\" executed, no data returned",
                sql_cmd
            );
        }
    }
    status
}

// return flow desc using flow id from stream desc
pub fn get_flow_desc_using_id(stream: StreamDef, id: i64) -> Option<FlowDef> {
    let mut desc: Option<FlowDef> = None;
    for flow in stream.flows {
        let f_id: i64 = flow
            .flowId
            .parse::<i64>()
            .expect("Cannot parse flowId in stream description.");
        if f_id == id {
            desc = Some(flow);
        }
    }
    desc
}

// return process desc using process id from flow desc
pub fn get_proc_desc_using_id(flow: FlowDef, id: i64) -> Option<ProcDef> {
    let mut desc: Option<ProcDef> = None;
    for process in flow.process {
        let p_id: i64 = process
            .processId
            .parse::<i64>()
            .expect("Cannot parse processId in flow description.");
        if p_id == id {
            desc = Some(process);
        }
    }
    desc
}

// return status of a Batch
pub fn get_batch_status(pool_id: i64, trigger: TriggerInfo, pool: Data<Params>) -> bool {
    let mut status = true;
    for id in &trigger.trigger.stream_ids {
        let stream: StreamDef =
            get_stream_desc(trigger.trigger.as_on_date.clone(), *id, pool.clone())
                .expect("Cannot fetch stream description from db.");
        for flow in stream.flows {
            for process in flow.process {
                let bullet = Bullet {
                    run_id: trigger.run_id,
                    asondate: trigger.trigger.as_on_date.clone(),
                    batch_id: trigger.trigger.batch_id,
                    stream_id: *id,
                    flow_id: flow.flowId.parse::<i64>().unwrap(),
                    executor_id: 0,
                    process_id: process.processId.parse::<i64>().unwrap(),
                    process_name: process.processName,
                    process_binary: process.processBinary,
                    process_args: process.processArguments,
                    process_report: process.processReport,
                };
                if !process_executed(pool_id, bullet, pool.clone()) {
                    status = false;
                }
            }
        }
    }
    status
}

// returns status of a worker
pub fn check_worker_status(exe_id: i64, pool_id: i64, pool: Data<Params>) -> bool {
    let env = create_environment_v3()
        .map_err(|e| e.unwrap())
        .expect("Cannot create DB environment.");
    let conn = env
        .connect_with_connection_string(&pool.con_str)
        .expect("Cannot establish a DB connection.");
    let stmt =
        Statement::with_parent(&conn).expect("Cannot create a statement instance to run queries.");
    let sql_cmd = format!(
        "select ProcessStatus from RunControl where ExecutorID = {} AND PoolID = {} ORDER BY RunID DESC;",
        exe_id,
        pool_id
    );
    let mut status = true;
    match stmt
        .exec_direct(&sql_cmd)
        .expect("Failed to execute a sql cmd!!")
    {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch().expect("Cannot read output of query.") {
                match cursor
                    .get_data::<&str>(1)
                    .expect("Cannot read row data from query.")
                {
                    Some(val) => {
                        let ProcessStatus = val;
                        if ProcessStatus == "PROCESSING".to_string() {
                            status = false;
                        }
                    }
                    None => {
                        log_debug!(
                            pool.diag_log,
                            "19. Query \"{}\" executed, no data returned",
                            sql_cmd
                        );
                    }
                }
                break;
            }
        }
        NoData(_) => {
            log_debug!(
                pool.diag_log,
                "20. Query \"{}\" executed, no data returned",
                sql_cmd
            );
        }
    }
    status
}

// returns connection url of the worker
pub fn get_executor_connection_url(exe_id: i64, pool_id: i64, pool: Data<Params>) -> String {
    let env = create_environment_v3()
        .map_err(|e| e.unwrap())
        .expect("Cannot create DB environment.");
    let conn = env
        .connect_with_connection_string(&pool.con_str)
        .expect("Cannot establish a DB connection.");
    let stmt =
        Statement::with_parent(&conn).expect("Cannot create a statement instance to run queries.");
    let sql_cmd = format!(
        "select ExecutorURL from ExecutorDef where ExecutorID = {} and PoolID = {};",
        exe_id, pool_id,
    );
    let mut url: String = String::new();
    match stmt
        .exec_direct(&sql_cmd)
        .expect("Failed to read ExecutorURL!!")
    {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch().expect("Cannot read output of query.") {
                match cursor
                    .get_data::<&str>(1)
                    .expect("Cannot read row data from query.")
                {
                    Some(val) => {
                        url = val.to_string();
                    }
                    None => {
                        log_debug!(
                            pool.diag_log,
                            "21. Query \"{}\" executed, no data returned",
                            sql_cmd
                        );
                    }
                }
            }
        }
        NoData(_) => {
            log_debug!(
                pool.diag_log,
                "22. Query \"{}\" executed, no data returned",
                sql_cmd
            );
        }
    }
    url
}

// add detail of queued process
pub fn add_to_run_control(
    pool_id: i64,
    status: &str,
    worker_id: i64,
    info: Bullet,
    pool: Data<Params>,
) {
    let asondate_timestamp: i64 = chrono::NaiveDate::parse_from_str(&info.asondate, "%d-%m-%Y")
        .expect("Cannot parse string as NaiveDate.")
        .and_hms(0, 0, 0)
        .timestamp();
    let env = create_environment_v3()
        .map_err(|e| e.unwrap())
        .expect("Cannot create DB environment.");
    let conn = env
        .connect_with_connection_string(&pool.con_str)
        .expect("Cannot establish a DB connection.");
    let stmt =
        Statement::with_parent(&conn).expect("Cannot create a statement instance to run queries.");
    let sql_cmd = format!("INSERT INTO RunControl (RunID, AsOnDate, BatchID, StreamID, FlowID, ExecutorID, ProcessID, ProcessStartTime, ProcessStatus, PoolID) values ({}, {}, {}, {}, {}, {}, {}, {}, '{}', {})", info.run_id,asondate_timestamp,info.batch_id,info.stream_id,info.flow_id,worker_id,info.process_id,Utc::now().timestamp(),status,pool_id);
    log_debug!(pool.diag_log, "Insert Query: {}", sql_cmd);
    match stmt
        .exec_direct(&sql_cmd)
        .expect("Failed to execute insert query sql cmd!!")
    {
        Data(_) => {}
        NoData(_) => {
            log_debug!(
                pool.diag_log,
                "23. Query \"{}\" executed, no data returned",
                sql_cmd
            );
        }
    }
    conn.disconnect()
        .expect("Error while connecting to server.");
}

#[get("/batches/{pool_id}")]
async fn get_batches(_: Path<(i64,)>, pool: Data<Params>) -> Result<HttpResponse, Error> {
    let env = create_environment_v3()
        .map_err(|e| e.unwrap())
        .expect("Cannot create DB environment.");
    let conn = env
        .connect_with_connection_string(&pool.con_str)
        .expect("Cannot establish a DB connection.");
    let stmt =
        Statement::with_parent(&conn).expect("Cannot create a statement instance to run queries.");
    let sql_cmd = format!("select BatchID,BatchName from BatchDef;");
    let mut batches: Vec<Batch> = Vec::new();
    let mut batch_id = 0;
    let mut batch_name = String::new();
    match stmt
        .exec_direct(&sql_cmd)
        .expect("Failed to execute a sql cmd!!")
    {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch().expect("Cannot read output of query.") {
                match cursor
                    .get_data::<&str>(1)
                    .expect("Cannot get batch id from db.")
                {
                    Some(val) => {
                        batch_id = val.parse::<i64>().unwrap_or(0);
                    }
                    None => {
                        log_debug!(
                            pool.diag_log,
                            "24. Query \"{}\" executed, no data returned",
                            sql_cmd
                        );
                    }
                };
                match cursor
                    .get_data::<&str>(2)
                    .expect("Cannot get batch id from db.")
                {
                    Some(val) => {
                        batch_name = val.to_string();
                    }
                    None => {
                        log_debug!(
                            pool.diag_log,
                            "25. Query \"{}\" executed, no data returned",
                            sql_cmd
                        );
                    }
                };
                let batch = Batch {
                    batch_id: batch_id,
                    batch_name: batch_name.to_string(),
                };
                batches.push(batch);
            }
        }
        NoData(_) => {
            log_debug!(
                pool.diag_log,
                "26. Query \"{}\" executed, no data returned",
                sql_cmd
            );
        }
    }
    // Serialize using `json::encode`
    let batches_info = json::encode(&batches).expect("Cannot serialize as JSON.");

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(batches_info))
}

#[post("/streams/{pool_id}")]
async fn get_streams(
    _path: Path<(i64,)>,
    info: Json<StatusReq>,
    pool: Data<Params>,
) -> Result<HttpResponse, Error> {
    let env = create_environment_v3()
        .map_err(|e| e.unwrap())
        .expect("Cannot create DB environment.");
    let conn = env
        .connect_with_connection_string(&pool.con_str)
        .expect("Cannot establish a DB connection.");
    let mut streams: Vec<Stream> = Vec::new();
    let mut id: i64 = info.batch_id;
    let mut stream_id: Vec<i64> = Vec::new();
    let mut stream_name = String::new();
    let sql_cmd = format!(
        "select StreamID from BatchStream where BatchID = {} AND IsActive = 'Y';",
        id
    );
    let stmt =
        Statement::with_parent(&conn).expect("Cannot create a statement instance to run queries.");
    match stmt
        .exec_direct(&sql_cmd)
        .expect("Failed to execute a sql cmd!!")
    {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch().expect("Cannot read output of query.") {
                match cursor
                    .get_data::<&str>(1)
                    .expect("Cannot get StreamID from db.")
                {
                    Some(val) => {
                        id = val.parse::<i64>().expect("Cannot parse string as integer.");
                        stream_id.push(id);
                    }
                    None => {
                        log_debug!(
                            pool.diag_log,
                            "1. Query \"{}\" executed, no data returned",
                            sql_cmd
                        );
                    }
                }
            }
        }
        NoData(_) => {
            log_debug!(
                pool.diag_log,
                "2. Query \"{}\" executed, no data returned",
                sql_cmd
            );
        }
    }
    for id in stream_id {
        let stmt_2 = Statement::with_parent(&conn)
            .expect("Cannot create a statement instance to run queries.");
        let sql_cmd_2 = format!("select StreamName from StreamDef where StreamID = {};", id);
        match stmt_2
            .exec_direct(&sql_cmd_2)
            .expect("Failed to execute a sql cmd!!")
        {
            Data(mut stmt) => {
                while let Some(mut cursor) = stmt.fetch().expect("Cannot read output of query.") {
                    match cursor
                        .get_data::<&str>(1)
                        .expect("Cannot get StreamName from db.")
                    {
                        Some(val) => {
                            stream_name = val.to_string();
                        }
                        None => {
                            log_debug!(
                                pool.diag_log,
                                "27. Query \"{}\" executed, no data returned",
                                sql_cmd
                            );
                        }
                    }
                }
            }
            NoData(_) => {
                log_debug!(
                    pool.diag_log,
                    "28. Query \"{}\" executed, no data returned",
                    sql_cmd
                );
            }
        }
        let stream = Stream {
            stream_id: id,
            stream_name: stream_name.clone(),
        };
        streams.push(stream);
    }
    // Serialize using `json::encode`
    let streams_info = json::encode(&streams).expect("Cannot serialize as JSON.");

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(streams_info))
}

#[post("/batch_status/{pool_id}")]
async fn get_batch_status_by_id(
    path: Path<(i64,)>,
    info: Json<RunReqStatus>,
    pool: Data<Params>,
) -> Result<HttpResponse, Error> {
    let as_on_date = chrono::NaiveDate::parse_from_str(&info.as_on_date, "%Y-%m-%d")
        .expect("Cannot parse string as NaiveDate.");
    let resp = status_by_id(
        path.0 .0,
        info.batch_id,
        as_on_date.format("%d-%m-%Y").to_string(),
        pool,
    );
    let batches_info = json::encode(&resp).expect("Cannot serialize as JSON.");

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(batches_info))
}

#[get("/last_batch_status/{pool_id}")]
async fn get_batch_status_latest(
    path: Path<(i64,)>,
    pool: Data<Params>,
) -> Result<HttpResponse, Error> {
    let run_id = get_last_assigned_run_id(path.0 .0, pool.clone()).unwrap_or(0);
    let batch_id = get_batch_id_latest(path.0 .0, run_id, pool.clone()).unwrap_or(0);
    let ason = fetch_latest_ason(path.0 .0, pool.clone())
        .date()
        .format("%d-%m-%Y")
        .to_string();
    let resp = status_by_id(path.0 .0, batch_id, ason, pool.clone());
    let batches_info = json::encode(&resp).expect("Cannot serialize as JSON.");

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(batches_info))
}

#[post("/batch_full_status/{pool_id}")]
async fn get_batch_full_status(
    path: Path<(i64,)>,
    info: Json<RunReqStatus>,
    pool: Data<Params>,
) -> Result<HttpResponse, Error> {
    let mut streams: Vec<i64> = Vec::new();
    let asondate = chrono::NaiveDate::parse_from_str(&info.as_on_date, "%Y-%m-%d")
        .expect("Cannot Parse ason date to a valid date type.")
        .and_hms(0, 0, 0);
    let env = create_environment_v3()
        .map_err(|e| e.unwrap())
        .expect("Cannot create DB environment.");
    let conn = env
        .connect_with_connection_string(&pool.con_str.clone())
        .expect("Cannot establish a DB connection.");
    let stmt =
        Statement::with_parent(&conn).expect("Cannot create a statement instance to run queries.");
    let sql_cmd = format!(
        "select StreamID from BatchStream where BatchID = {} AND IsActive = 'Y';",
        info.batch_id
    );
    match stmt
        .exec_direct(&sql_cmd)
        .expect("Failed to execute a sql cmd!!")
    {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch().expect("Cannot read output of query.") {
                match cursor
                    .get_data::<&str>(1)
                    .expect("Cannot get StreamID from db.")
                {
                    Some(val) => {
                        let stream_id = val.parse::<i64>().unwrap_or(0);
                        streams.push(stream_id);
                    }
                    None => {
                        log_debug!(
                            pool.diag_log,
                            "3. Query \"{}\" executed, no data returned",
                            sql_cmd
                        );
                    }
                }
            }
        }
        NoData(_) => {
            log_debug!(
                pool.diag_log,
                "4. Query \"{}\" executed, no data returned",
                sql_cmd
            );
        }
    }
    let batch_lock_status = get_lock_status(&info.batch_id, &asondate, pool.clone());
    let mut stream_status: Vec<StreamFullStatus> = Vec::new();
    for stream_id in streams {
        let stream: StreamDef = get_stream_desc(
            asondate.format("%d-%m-%Y").to_string(),
            stream_id,
            pool.clone(),
        )
        .expect("Cannot fetch stream description from db.");
        let mut stream_start_time = NaiveDateTime::from_timestamp(0, 0);
        let mut stream_end_time = NaiveDateTime::from_timestamp(0, 0);
        let mut _tt_by_stream_secs = 0;
        let mut flow_full_status: Vec<FlowFullStatus> = Vec::new();
        for flow in stream.flows {
            let mut flow_final_status = "NOT STARTED";
            let mut tt_by_flow_secs = 0;
            let mut process_full_status: Vec<ProcessFullStatus> = Vec::new();
            let mut proc_count = 0;
            let mut succ_count = 0;
            let mut flow_start_time = NaiveDateTime::from_timestamp(0, 0);
            let mut flow_end_time = NaiveDateTime::from_timestamp(0, 0);
            for process in flow.process {
                proc_count += 1;
                let mut proc_start_time: NaiveDateTime = NaiveDateTime::from_timestamp(0, 0);
                let mut proc_end_time: NaiveDateTime = NaiveDateTime::from_timestamp(0, 0);
                let mut proc_final_status: String = "NOT STARTED".to_string();
                let mut process_report: Vec<u8> = Vec::new();
                let mut total_num_recs = 0;
                let mut success_process_recs = 0;
                let mut failed_process_recs = 0;
                let mut prin_in_ip = 0.0;
                let mut prin_in_op = 0.0;
                let int_amt_in_op = 0.0;
                let mut _total_cfs = 0;
                let stmt_2 = Statement::with_parent(&conn)
                    .expect("Cannot create a statement instance to run queries.");
                let sql_cmd_2 = format!("select ProcessStartTime,ProcessEndTime,cast(ProcessSummary as varchar(max)),ProcessStatus from RunControl where BatchID = {} AND StreamID = {} AND PoolID = {} AND AsOnDate = {} AND FlowID = {} AND ProcessID = {};", info.batch_id,stream_id,path.0.0,asondate.timestamp(),flow.flowId,process.processId);
                match stmt_2
                    .exec_direct(&sql_cmd_2)
                    .expect("Failed to execute a sql cmd!!")
                {
                    Data(mut stmt) => {
                        while let Some(mut cursor) =
                            stmt.fetch().expect("Cannot read output of query.")
                        {
                            match cursor
                                .get_data::<&str>(1)
                                .expect("Cannot get batch id from db.")
                            {
                                Some(val) => {
                                    proc_start_time = NaiveDateTime::from_timestamp(
                                        val.parse::<i64>().unwrap_or(0),
                                        0,
                                    );
                                }
                                None => {
                                    log_debug!(
                                        pool.diag_log,
                                        "29. Query \"{}\" executed, no data returned",
                                        sql_cmd_2
                                    );
                                }
                            };
                            match cursor
                                .get_data::<&str>(2)
                                .expect("Cannot get batch id from db.")
                            {
                                Some(val) => {
                                    proc_end_time = NaiveDateTime::from_timestamp(
                                        val.parse::<i64>().unwrap_or(0),
                                        0,
                                    );
                                }
                                None => {
                                    log_debug!(
                                        pool.diag_log,
                                        "30. Query \"{}\" executed, no data returned",
                                        sql_cmd_2
                                    );
                                }
                            };
                            match cursor
                                .get_data::<&str>(3)
                                .expect("Cannot get batch id from db.")
                            {
                                Some(val) => {
                                    process_report = val.to_string().into_bytes();
                                }
                                None => {
                                    log_debug!(
                                        pool.diag_log,
                                        "31. Query \"{}\" executed, no data returned",
                                        sql_cmd_2
                                    );
                                }
                            };
                            match cursor
                                .get_data::<&str>(4)
                                .expect("Cannot get batch id from db.")
                            {
                                Some(val) => {
                                    proc_final_status = val.to_string();
                                }
                                None => {
                                    log_debug!(
                                        pool.diag_log,
                                        "32. Query \"{}\" executed, no data returned",
                                        sql_cmd
                                    );
                                }
                            };
                            if proc_final_status == "FAIL".to_string() {
                                flow_final_status = "FAIL";
                            } else if proc_final_status == "PROCESSING".to_string() {
                                flow_final_status = "PROCESSING";
                            } else {
                                succ_count += 1;
                            }
                            if flow_start_time.timestamp() == 0 {
                                flow_start_time = proc_start_time;
                            }
                            if proc_start_time < flow_start_time {
                                flow_start_time = proc_start_time;
                            }
                            if proc_end_time > flow_end_time {
                                flow_end_time = proc_end_time;
                            }
                            if process.processReport != "".to_string() && process_report.len() > 0 {
                                match str::from_utf8(&process_report) {
                                    Ok(val) => {
                                        let report: HealthReport = serde_json::from_str(val)
                                            .expect("Cannot deserialize HealthReport Json.");
                                        total_num_recs = report.tot_accounts;
                                        success_process_recs = report.acc_read_succ;
                                        failed_process_recs = report.acc_read_fail;
                                        prin_in_ip = report.tot_amt_ip;
                                        prin_in_op = report.tot_amt_op;
                                        _total_cfs = report.tot_no_cf;
                                    }
                                    Err(_e) => {}
                                };
                            }
                        }
                    }
                    NoData(_) => {
                        log_debug!(
                            pool.diag_log,
                            "33. Query \"{}\" executed, no data returned",
                            sql_cmd
                        );
                    }
                }
                let mut tt_by_proc = proc_end_time
                    .signed_duration_since(proc_start_time)
                    .num_seconds();
                if tt_by_proc < 0 {
                    tt_by_proc = 0;
                }
                let process_status = ProcessFullStatus {
                    process_id: process.processId.parse::<i64>().unwrap(),
                    process_name: process.processName,
                    process_start_time: proc_start_time.format("%Y-%m-%dT%T%.6f").to_string(),
                    process_end_time: proc_end_time.format("%Y-%m-%dT%T%.6f").to_string(),
                    process_status: proc_final_status,
                    process_error_msg: "".to_string(),
                    tt_by_process_secs: tt_by_proc,
                    total_num_recs: total_num_recs,
                    success_process_recs: success_process_recs,
                    failed_process_recs: failed_process_recs,
                    principal_input_amt: prin_in_ip,
                    principal_output_amt: prin_in_op,
                    interest_output_amt: int_amt_in_op,
                };
                process_full_status.push(process_status);
                tt_by_flow_secs += tt_by_proc;
            }
            if proc_count == succ_count {
                flow_final_status = "SUCCESS";
            }
            if stream_start_time.timestamp() == 0 {
                stream_start_time = flow_start_time;
            }
            if flow_start_time < stream_start_time {
                stream_start_time = flow_start_time;
            }
            if flow_end_time > stream_end_time {
                stream_end_time = flow_end_time;
            }
            _tt_by_stream_secs += tt_by_flow_secs;
            let mut succ_proc = 0;
            for status in &process_full_status {
                if status.process_status == "FAIL".to_string() {
                    flow_final_status = "FAIL";
                    break;
                }
                if status.process_status == "PROCESSING".to_string() {
                    flow_final_status = "PROCESSING";
                    break;
                }
                if status.process_status == "NOT STARTED".to_string() {
                    continue;
                }
                succ_proc += 1;
            }
            if succ_proc == process_full_status.len() {
                flow_final_status = "SUCCESS";
            }
            let tt_by_flow_secs_act = flow_end_time
                .signed_duration_since(flow_start_time)
                .num_seconds();
            let flow_status = FlowFullStatus {
                flow_id: flow.flowId.parse::<i64>().unwrap_or(0),
                flow_name: flow.name,
                flow_start_time: flow_start_time.format("%Y-%m-%dT%T%.6f").to_string(),
                flow_end_time: flow_end_time.format("%Y-%m-%dT%T%.6f").to_string(),
                flow_status: flow_final_status.to_string(),
                tt_by_flow_secs: tt_by_flow_secs_act,
                flow_error_msg: "".to_string(),
                process: process_full_status,
            };
            flow_full_status.push(flow_status);
        }
        let tt_by_stream_secs_act = stream_end_time
            .signed_duration_since(stream_start_time)
            .num_seconds();
        let stream_full_status = StreamFullStatus {
            stream_id: stream_id,
            stream_name: get_stream_name(stream_id, pool.clone()),
            stream_start_time: stream_start_time.format("%Y-%m-%dT%T%.6f").to_string(),
            stream_end_time: stream_end_time.format("%Y-%m-%dT%T%.6f").to_string(),
            stream_status: get_stream_status_by_batch(
                path.0 .0,
                info.batch_id,
                asondate.format("%d-%m-%Y").to_string(),
                stream_id,
                pool.clone(),
            ),
            stream_lock_status: batch_lock_status.to_string(),
            tt_by_stream_secs: tt_by_stream_secs_act,
            stream_error_msg: "".to_string(),
            flows: flow_full_status,
        };
        stream_status.push(stream_full_status);
    }
    let batch_status = BatchFullStatus {
        batch_id: info.batch_id,
        as_on_date: asondate.date().format("%Y-%m-%d").to_string(),
        streams: stream_status,
    };
    let batch_status_info = json::encode(&batch_status).expect("Cannot serialize as JSON.");

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(batch_status_info))
}

pub fn get_batch_id_latest(
    pool_id: i64,
    run_id: i64,
    pool: Data<Params>,
) -> Result<i64, &'static str> {
    let env = create_environment_v3()
        .map_err(|e| e.unwrap())
        .expect("Cannot create DB environment.");
    let conn = env
        .connect_with_connection_string(&pool.con_str)
        .expect("Cannot establish a DB connection.");
    let stmt =
        Statement::with_parent(&conn).expect("Cannot create a statement instance to run queries.");
    let sql_cmd = format!(
        "select BatchID from RunControl where RunID = {} AND PoolID = {};",
        run_id, pool_id
    );
    match stmt
        .exec_direct(&sql_cmd)
        .expect("Failed to execute a sql cmd!!")
    {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch().expect("Cannot read output of query.") {
                match cursor
                    .get_data::<&str>(1)
                    .expect("Cannot get batch id from db.")
                {
                    Some(val) => {
                        let batch_id = val.parse::<i64>().unwrap_or(0);
                        return Ok(batch_id);
                    }
                    None => {
                        return Err("No runs initiated till now.");
                    }
                }
            }
            return Err("No runs initiated till now.");
        }
        NoData(_) => {
            return Err("No runs initiated till now.");
        }
    };
}

fn status_by_id(pool_id: i64, batchid: i64, ason: String, pool: Data<Params>) -> RunStatus {
    let env = create_environment_v3()
        .map_err(|e| e.unwrap())
        .expect("Cannot create DB environment.");
    let conn = env
        .connect_with_connection_string(&pool.con_str)
        .expect("Cannot establish a DB connection.");
    let stmt =
        Statement::with_parent(&conn).expect("Cannot create a statement instance to run queries.");
    let mut run_id: i64 = 0;
    let mut streams_status: Vec<StreamStatus> = Vec::new();
    let asondate = chrono::NaiveDate::parse_from_str(&ason, "%d-%m-%Y")
        .expect("Cannot Parse ason date to a valid date type.")
        .and_hms(0, 0, 0);
    let mut streams: Vec<i64> = Vec::new();
    let sql_cmd = format!(
        "select StreamID from BatchStream where BatchID = {} AND IsActive = 'Y';",
        batchid
    );
    match stmt
        .exec_direct(&sql_cmd)
        .expect("Failed to execute a sql cmd!!")
    {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch().expect("Cannot read output of query.") {
                match cursor
                    .get_data::<&str>(1)
                    .expect("Cannot get StreamID from query.")
                {
                    Some(val) => {
                        let stream_id = val
                            .parse::<i64>()
                            .expect("Cannot get stream ID from Batch ID.");
                        streams.push(stream_id);
                    }
                    None => {
                        log_debug!(
                            pool.diag_log,
                            "5. Query \"{}\" executed, no data returned",
                            sql_cmd
                        );
                    }
                }
            }
        }
        NoData(_) => {
            log_debug!(
                pool.diag_log,
                "6. Query \"{}\" executed, no data returned",
                sql_cmd
            );
        }
    }
    for stream_id in streams {
        let stmt_2 = Statement::with_parent(&conn)
            .expect("Cannot create a statement instance to run queries.");
        let sql_cmd_2 = format!(
            "select max(RunID) as RunID from RunControl where BatchID = {} AND StreamID = {} AND PoolID = {} AND AsOnDate = {};",
            batchid,
            stream_id,
            pool_id,
            asondate.timestamp(),
        );
        match stmt_2
            .exec_direct(&sql_cmd_2)
            .expect("Failed to execute a sql cmd 2!!")
        {
            Data(mut stmt) => {
                while let Some(mut cursor) = stmt.fetch().expect("Cannot read output of query.") {
                    match cursor
                        .get_data::<&str>(1)
                        .expect("Cannot get RunID from query.")
                    {
                        Some(val) => {
                            run_id = val.parse::<i64>().expect("Cannot get Run ID from Query.");
                        }
                        None => {
                            log_debug!(
                                pool.diag_log,
                                "35. Query \"{}\" executed, no data returned",
                                sql_cmd_2
                            );
                        }
                    }
                }
            }
            NoData(_) => {
                log_debug!(
                    pool.diag_log,
                    "34. Query \"{}\" executed, no data returned",
                    sql_cmd_2
                );
            }
        }
        let stream: StreamDef = get_stream_desc(ason.clone(), stream_id, pool.clone())
            .expect("Cannot fetch stream description from db.");
        let mut stream_status = "NOT STARTED";
        let mut success_count = 0;
        let sql_cmd_3 = format!(
            "select ProcessStatus from RunControl where AsOnDate = {} AND BatchID = {} AND StreamID = {} AND RunID = {} AND PoolID = {};",
            asondate.timestamp(),
            batchid,
            stream_id,
            run_id,
            pool_id,
        );
        let stmt_3 = Statement::with_parent(&conn)
            .expect("Cannot create a statement instance to run queries.");
        match stmt_3
            .exec_direct(&sql_cmd_3)
            .expect("Failed to execute a sql cmd 3!!")
        {
            Data(mut stmt) => {
                while let Some(mut cursor) = stmt.fetch().expect("Cannot read output of query.") {
                    match cursor
                        .get_data::<&str>(1)
                        .expect("Cannot get ProcessStatus from query.")
                    {
                        Some(val) => {
                            let status: String = val.to_string();
                            if status == "PROCESSING".to_string() {
                                stream_status = "PROCESSING";
                                break;
                            } else if status == "FAIL".to_string() {
                                stream_status = "FAIL";
                                break;
                            } else {
                                stream_status = "SUCCESS";
                                success_count += 1;
                            }
                        }
                        None => {
                            log_debug!(
                                pool.diag_log,
                                "36. Query \"{}\" executed, no data returned",
                                sql_cmd
                            );
                        }
                    }
                }
            }
            NoData(_) => {
                log_debug!(
                    pool.diag_log,
                    "37. Query \"{}\" executed, no data returned",
                    sql_cmd
                );
            }
        }
        let mut process_count = 0;
        if stream_status == "SUCCESS".to_string() {
            for flows in stream.flows {
                for _ in flows.process {
                    process_count += 1;
                }
            }
            if process_count != success_count {
                stream_status = "PROCESSING";
            } else {
                stream_status = "SUCCESS";
            }
        }
        let stream_final_status = StreamStatus {
            stream_id: stream_id,
            status: stream_status.to_string(),
        };
        streams_status.push(stream_final_status);
    }
    // Serialize using `json::encode`
    let batch_status = RunStatus {
        batch_id: batchid,
        as_on_date: asondate.date().format("%Y-%m-%d").to_string(),
        streams: streams_status,
    };

    batch_status
}

#[get("/latest_ason/{pool_id}")]
async fn get_lastest_ason(path: Path<(i64,)>, pool: Data<Params>) -> Result<HttpResponse, Error> {
    let asondate = fetch_latest_ason(path.0 .0, pool.clone());
    let latest_ason = asondate.date().format("%Y-%m-%d").to_string();
    let ason = AsOn {
        as_on_date: latest_ason,
    };
    let resp = json::encode(&ason).expect("Cannot serialize as JSON.");

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(resp))
}

#[post("/batch_latest_ason/{pool_id}")]
async fn get_batch_lastest_ason(
    path: Path<(i64,)>,
    body: Json<StatusReq>,
    pool: Data<Params>,
) -> Result<HttpResponse, Error> {
    let asondate = fetch_latest_ason_by_batch(path.0 .0, body.batch_id, pool.clone());
    let latest_ason = asondate.date().format("%Y-%m-%d").to_string();
    let ason = AsOn {
        as_on_date: latest_ason,
    };
    let resp = json::encode(&ason).expect("Cannot serialize as JSON.");

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(resp))
}

#[post("/batch_last_success_ason/{pool_id}")]
async fn get_batch_last_success_ason(
    path: Path<(i64,)>,
    body: Json<StatusReq>,
    pool: Data<Params>,
) -> Result<HttpResponse, Error> {
    let asondate = fetch_batch_last_success_ason(path.0 .0, body.batch_id, pool.clone());
    let latest_ason = asondate.date().format("%Y-%m-%d").to_string();
    Ok(HttpResponse::Ok()
        .content_type("text/plain")
        .body(latest_ason))
}

fn fetch_latest_ason(pool_id: i64, pool: Data<Params>) -> DateTime<Utc> {
    let mut asondate: DateTime<Utc> = Utc::now();
    let env = create_environment_v3()
        .map_err(|e| e.unwrap())
        .expect("Cannot create DB environment.");
    let conn = env
        .connect_with_connection_string(&pool.con_str)
        .expect("Cannot establish a DB connection.");
    let stmt =
        Statement::with_parent(&conn).expect("Cannot create a statement instance to run queries.");
    let sql_cmd = format!(
        "select max(AsOnDate) from RunControl where PoolID = {};",
        pool_id
    );
    match stmt
        .exec_direct(&sql_cmd)
        .expect("Failed to execute a sql cmd!!")
    {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch().expect("Cannot read output of query.") {
                match cursor
                    .get_data::<&str>(1)
                    .expect("Cannot get max(AsOnDate) from db.")
                {
                    Some(val) => {
                        let asondate_ndt =
                            NaiveDateTime::from_timestamp(val.parse::<i64>().unwrap_or(0), 0);
                        asondate = DateTime::<Utc>::from_utc(asondate_ndt, Utc);
                    }
                    None => {
                        log_debug!(
                            pool.diag_log,
                            "38. Query \"{}\" executed, no data returned",
                            sql_cmd
                        );
                    }
                }
            }
        }
        NoData(_) => {
            log_debug!(
                pool.diag_log,
                "39. Query \"{}\" executed, no data returned",
                sql_cmd
            );
        }
    }

    asondate
}

fn fetch_latest_ason_by_batch(pool_id: i64, b_id: i64, pool: Data<Params>) -> DateTime<Utc> {
    let mut asondate: DateTime<Utc> = Utc::now();
    let env = create_environment_v3()
        .map_err(|e| e.unwrap())
        .expect("Cannot create DB environment.");
    let conn = env
        .connect_with_connection_string(&pool.con_str)
        .expect("Cannot establish a DB connection.");
    let stmt =
        Statement::with_parent(&conn).expect("Cannot create a statement instance to run queries.");
    let sql_cmd = format!(
        "select max(AsOnDate) from RunControl where BatchID = {} AND PoolID = {};",
        b_id, pool_id
    );
    match stmt
        .exec_direct(&sql_cmd)
        .expect("Failed to execute a sql cmd!!")
    {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch().expect("Cannot read output of query.") {
                match cursor
                    .get_data::<&str>(1)
                    .expect("Cannot get max(AsOnDate) from db.")
                {
                    Some(val) => {
                        let asondate_ndt =
                            NaiveDateTime::from_timestamp(val.parse::<i64>().unwrap_or(0), 0);
                        asondate = DateTime::<Utc>::from_utc(asondate_ndt, Utc);
                    }
                    None => {
                        log_debug!(
                            pool.diag_log,
                            "40. Query \"{}\" executed, no data returned",
                            sql_cmd
                        );
                    }
                }
            }
        }
        NoData(_) => {
            log_debug!(
                pool.diag_log,
                "41. Query \"{}\" executed, no data returned",
                sql_cmd
            );
        }
    }

    asondate
}

fn fetch_batch_last_success_ason(pool_id: i64, b_id: i64, pool: Data<Params>) -> DateTime<Utc> {
    let mut asondate: DateTime<Utc> = Utc::now() - chrono::Duration::days(1);
    let env = create_environment_v3()
        .map_err(|e| e.unwrap())
        .expect("Cannot create DB environment.");
    let conn = env
        .connect_with_connection_string(&pool.con_str)
        .expect("Cannot establish a DB connection.");
    let stmt =
        Statement::with_parent(&conn).expect("Cannot create a statement instance to run queries.");
    let sql_cmd = format!(
        "select AsOnDate from RunControl where BatchID = {} AND PoolID = {} ORDER BY RunID DESC;",
        b_id, pool_id
    );
    match stmt
        .exec_direct(&sql_cmd)
        .expect("Failed to execute a sql cmd!!")
    {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch().expect("Cannot read output of query.") {
                let mut is_executed = true;
                match cursor
                    .get_data::<&str>(1)
                    .expect("Cannot get AsOnDate from db.")
                {
                    Some(val) => {
                        let asondate_ndt =
                            NaiveDateTime::from_timestamp(val.parse::<i64>().unwrap_or(0), 0);
                        asondate = DateTime::<Utc>::from_utc(asondate_ndt, Utc);
                    }
                    None => {
                        log_debug!(
                            pool.diag_log,
                            "42. Query \"{}\" executed, no data returned",
                            sql_cmd
                        );
                    }
                }
                let batch_status = status_by_id(
                    pool_id,
                    b_id,
                    asondate.date().format("%d-%m-%Y").to_string(),
                    pool.clone(),
                );
                for status in batch_status.streams {
                    if status.status != "SUCCESS" {
                        is_executed = false;
                    }
                }
                if is_executed {
                    break;
                }
            }
        }
        NoData(_) => {
            log_debug!(
                pool.diag_log,
                "43. Query \"{}\" executed, no data returned",
                sql_cmd
            );
        }
    }

    asondate
}

pub fn process_executed(pool_id: i64, bullet: Bullet, pool: Data<Params>) -> bool {
    let mut status = false;
    let asondate = chrono::NaiveDate::parse_from_str(&bullet.asondate, "%d-%m-%Y")
        .expect("Cannot parse string as NaiveDate")
        .and_hms(0, 0, 0);

    let env = create_environment_v3()
        .map_err(|e| e.unwrap())
        .expect("Cannot create DB environment.");
    let conn = env
        .connect_with_connection_string(&pool.con_str)
        .expect("Cannot establish a DB connection.");
    let stmt =
        Statement::with_parent(&conn).expect("Cannot create a statement instance to run queries.");
    let sql_cmd = format!(
            "select ProcessStatus from RunControl where BatchID = {} AND StreamID = {} AND FlowID = {} AND ProcessID = {} AND AsOnDate = {} AND RunID = {} AND PoolID = {};",
            bullet.batch_id,
            bullet.stream_id,
            bullet.flow_id,
            bullet.process_id,
            asondate.timestamp(),
            bullet.run_id,
            pool_id,
        );
    let mut ProcessStatus: String = String::new();
    match stmt
        .exec_direct(&sql_cmd)
        .expect("Failed to execute a sql cmd!!")
    {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch().expect("Cannot read output of query.") {
                match cursor
                    .get_data::<&str>(1)
                    .expect("Cannot get max(AsOnDate) from db.")
                {
                    Some(val) => {
                        ProcessStatus = val.to_string();
                    }
                    None => {
                        log_debug!(
                            pool.diag_log,
                            "44. Query \"{}\" executed, no data returned",
                            sql_cmd
                        );
                    }
                }
                if ProcessStatus == "FAIL".to_string()
                    || ProcessStatus == "PROCESSING".to_string()
                    || ProcessStatus == "SUCCESS".to_string()
                {
                    status = true;
                }
            }
        }
        NoData(_) => {
            log_debug!(
                pool.diag_log,
                "45. Query \"{}\" executed, no data returned",
                sql_cmd
            );
        }
    }

    status
}

pub fn check_additional_failed_processes(
    pool_id: i64,
    trigger_info: TriggerInfo,
    pool: Data<Params>,
) {
    let mut fail_set: Vec<Fail> = Vec::new();
    let mut failed_stream_ids: Vec<i64> = Vec::new();
    let mut failed_flow_ids: Vec<String> = Vec::new();
    let mut failed_process_ids: Vec<String> = Vec::new();
    let ason = trigger_info.trigger.as_on_date.clone();
    let env = create_environment_v3()
        .map_err(|e| e.unwrap())
        .expect("Cannot create DB environment.");
    let conn = env
        .connect_with_connection_string(&pool.con_str)
        .expect("Cannot establish a DB connection.");
    let stmt =
        Statement::with_parent(&conn).expect("Cannot create a statement instance to run queries.");
    let sql_cmd = format!(
            "select ProcessStatus,StreamID,FlowID,ProcessID from RunControl where RunID = {} and PoolID = {};",
            trigger_info.run_id,
            pool_id
        );
    let mut status: String = String::new();
    let mut stream_id: i64 = 0;
    let mut flow_id: i64 = 0;
    let mut process_id: i64 = 0;
    match stmt
        .exec_direct(&sql_cmd)
        .expect("Failed to execute a sql cmd!!")
    {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch().expect("Cannot read output of query.") {
                match cursor
                    .get_data::<&str>(1)
                    .expect("Cannot get ProcessStatus from db.")
                {
                    Some(val) => {
                        status = val.to_string();
                    }
                    None => {
                        log_debug!(
                            pool.diag_log,
                            "46. Query \"{}\" executed, no data returned",
                            sql_cmd
                        );
                    }
                };
                match cursor
                    .get_data::<&str>(2)
                    .expect("Cannot get StreamID from db.")
                {
                    Some(val) => {
                        stream_id = val.parse::<i64>().expect("Cannot parse string as i64.");
                    }
                    None => {
                        log_debug!(
                            pool.diag_log,
                            "47. Query \"{}\" executed, no data returned",
                            sql_cmd
                        );
                    }
                };
                match cursor
                    .get_data::<&str>(3)
                    .expect("Cannot get FlowID from db.")
                {
                    Some(val) => {
                        flow_id = val.parse::<i64>().expect("Cannot parse string as i64.");
                    }
                    None => {
                        log_debug!(
                            pool.diag_log,
                            "48. Query \"{}\" executed, no data returned",
                            sql_cmd
                        );
                    }
                };
                match cursor
                    .get_data::<&str>(4)
                    .expect("Cannot get ProcessID from db.")
                {
                    Some(val) => {
                        process_id = val.parse::<i64>().expect("Cannot parse string as i64.");
                    }
                    None => {
                        log_debug!(
                            pool.diag_log,
                            "49. Query \"{}\" executed, no data returned",
                            sql_cmd
                        );
                    }
                };
                if status == "FAIL".to_string() {
                    failed_stream_ids.push(stream_id);
                    failed_flow_ids.push(format!("{}-{}", stream_id, flow_id));
                    failed_process_ids.push(format!("{}-{}-{}", stream_id, flow_id, process_id));
                }
            }
        }
        NoData(_) => {
            log_debug!(
                pool.diag_log,
                "50. Query \"{}\" executed, no data returned",
                sql_cmd
            );
        }
    };
    let mut init_set_len = fail_set.len();
    loop {
        for id in trigger_info.trigger.stream_ids.clone() {
            let dep_stream_ids: Vec<i64> =
                get_stream_dep(trigger_info.trigger.batch_id, id, pool.clone());
            for dep_id in dep_stream_ids {
                if failed_stream_ids.contains(&dep_id) {
                    let stream: StreamDef = get_stream_desc(ason.clone(), id, pool.clone())
                        .expect("Cannot fetch stream description from db.");
                    for flow in stream.flows {
                        for process in flow.process {
                            let fail_element = Fail {
                                run_id: trigger_info.run_id,
                                asondate: ason.clone(),
                                batch_id: trigger_info.trigger.batch_id,
                                stream_id: id,
                                flow_id: flow.flowId.parse::<i64>().unwrap(),
                                process_id: process.processId.parse::<i64>().unwrap(),
                            };
                            let mut failed_ele_present = false;
                            for ele in &fail_set {
                                if fail_element.process_id == ele.process_id
                                    && fail_element.flow_id == ele.flow_id
                                    && fail_element.stream_id == ele.stream_id
                                {
                                    failed_ele_present = true;
                                }
                            }
                            if !failed_ele_present {
                                fail_set.push(fail_element);
                            }
                        }
                    }
                }
            }
        }
        for id in &failed_stream_ids {
            let stream: StreamDef = get_stream_desc(ason.clone(), *id, pool.clone())
                .expect("Cannot fetch stream description from db.");
            for flow in &stream.flows {
                let dep_flow_ids: Vec<i64> = get_flow_dep(flow);
                for f_id in dep_flow_ids {
                    let flow_key = format!("{}-{}", id, f_id);
                    if failed_flow_ids.contains(&flow_key) {
                        for process in &flow.process {
                            let fail_element = Fail {
                                run_id: trigger_info.run_id,
                                asondate: ason.clone(),
                                batch_id: trigger_info.trigger.batch_id,
                                stream_id: *id,
                                flow_id: flow.flowId.parse::<i64>().unwrap_or(0),
                                process_id: process.processId.parse::<i64>().unwrap_or(0),
                            };
                            let mut failed_ele_present = false;
                            for ele in &fail_set {
                                if fail_element.process_id == ele.process_id
                                    && fail_element.flow_id == ele.flow_id
                                    && fail_element.stream_id == ele.stream_id
                                {
                                    failed_ele_present = true;
                                }
                            }
                            if !failed_ele_present {
                                fail_set.push(fail_element);
                            }
                        }
                    }
                }
            }
        }
        for id in &failed_stream_ids {
            let stream: StreamDef = get_stream_desc(ason.clone(), *id, pool.clone())
                .expect("Cannot fetch stream description from db.");
            log_debug!(pool.diag_log, "Failed Flow ID's: {:#?}", failed_flow_ids);
            for flow in stream.flows {
                let flow_id = flow.flowId.parse::<i64>().unwrap_or(0);
                for process in flow.process {
                    let dep_proc_ids: Vec<i64> = get_proc_dep(process.clone());
                    for p_id in dep_proc_ids {
                        let process_key = format!("{}-{}-{}", id, flow_id, p_id);
                        if failed_process_ids.contains(&process_key) {
                            let fail_element = Fail {
                                run_id: trigger_info.run_id,
                                asondate: ason.clone(),
                                batch_id: trigger_info.trigger.batch_id,
                                stream_id: *id,
                                flow_id: flow_id,
                                process_id: process.processId.parse::<i64>().unwrap_or(0),
                            };
                            let mut failed_ele_present = false;
                            for ele in &fail_set {
                                if fail_element.process_id == ele.process_id
                                    && fail_element.flow_id == ele.flow_id
                                    && fail_element.stream_id == ele.stream_id
                                {
                                    failed_ele_present = true;
                                }
                            }
                            if !failed_ele_present {
                                fail_set.push(fail_element);
                            }
                            failed_process_ids.push(format!("{}-{}-{}", id, flow_id, p_id));
                        }
                    }
                }
            }
        }
        let new_set_len = fail_set.len();
        if new_set_len == init_set_len {
            break;
        }
        init_set_len = new_set_len;
    }
    for ele in fail_set {
        let status = "FAIL";
        let info = Bullet {
            run_id: ele.run_id,
            asondate: ele.asondate,
            batch_id: ele.batch_id,
            stream_id: ele.stream_id,
            flow_id: ele.flow_id,
            executor_id: 0,
            process_id: ele.process_id,
            process_name: "".to_string(),
            process_binary: "".to_string(),
            process_args: Vec::new(),
            process_report: "".to_string(),
        };
        let is_fail = get_process_status(
            pool_id,
            trigger_info.clone(),
            info.process_id,
            info.flow_id,
            info.stream_id,
            "FAIL".to_string(),
            pool.clone(),
        );
        let is_success = get_process_status(
            pool_id,
            trigger_info.clone(),
            info.process_id,
            info.flow_id,
            info.stream_id,
            "SUCCESS".to_string(),
            pool.clone(),
        );
        if !is_fail && !is_success {
            add_to_run_control(pool_id, status, 0, info, pool.clone());
        }
    }
}

fn get_stream_dep(batchid: i64, stream_id: i64, pool: Data<Params>) -> Vec<i64> {
    let env = create_environment_v3()
        .map_err(|e| e.unwrap())
        .expect("Cannot create DB environment.");
    let conn = env
        .connect_with_connection_string(&pool.con_str)
        .expect("Cannot establish a DB connection.");
    let stmt =
        Statement::with_parent(&conn).expect("Cannot create a statement instance to run queries.");
    let sql_cmd = format!(
            "select cast(StreamDep as varchar(max)) from BATCHSTREAM where BatchID = {} AND StreamID = {} AND IsActive = 'Y'",
            batchid, stream_id
        );
    let mut res: Vec<i64> = Vec::new();
    match stmt
        .exec_direct(&sql_cmd)
        .expect("Failed to execute a sql cmd!!")
    {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch().expect("Cannot read output of query.") {
                match cursor
                    .get_data::<&str>(1)
                    .expect("Cannot read row data from query.")
                {
                    Some(val) => {
                        let stream_dep: Vec<u8> = val.to_string().into_bytes();
                        match str::from_utf8(&stream_dep) {
                            Ok(val) => {
                                let dep: Dep = serde_json::from_str(val)
                                    .expect("Cannot deserialize StreamDep Json.");
                                res = dep.dependencies;
                            }
                            Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
                        };
                    }
                    None => {
                        panic!("Cannot Fetch Column details from row result.");
                    }
                }
            }
        }
        NoData(_) => {
            log_debug!(
                pool.diag_log,
                "10. Query \"{}\" executed, no data returned",
                sql_cmd
            );
        }
    }

    res
}

fn get_flow_dep(flow: &FlowDef) -> Vec<i64> {
    let mut res: Vec<i64> = Vec::new();
    for dep in &flow.flowDependencies {
        let dep_as_i64 = dep.parse::<i64>().expect("Cannot parse as i64.");
        res.push(dep_as_i64);
    }

    res
}

fn get_proc_dep(process: ProcDef) -> Vec<i64> {
    let mut res: Vec<i64> = Vec::new();
    for dep in process.processDependencies {
        let dep_as_i64 = dep.parse::<i64>().expect("Cannot parse as i64.");
        res.push(dep_as_i64);
    }

    res
}

fn get_stream_name(stream_id: i64, pool: Data<Params>) -> String {
    let env = create_environment_v3()
        .map_err(|e| e.unwrap())
        .expect("Cannot create DB environment.");
    let conn = env
        .connect_with_connection_string(&pool.con_str)
        .expect("Cannot establish a DB connection.");
    let stmt =
        Statement::with_parent(&conn).expect("Cannot create a statement instance to run queries.");
    let sql_cmd = format!(
        "select StreamName from StreamDef where StreamID = {};",
        stream_id
    );
    let mut stream_name: String = String::new();
    match stmt
        .exec_direct(&sql_cmd)
        .expect("Failed to execute a sql cmd!!")
    {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch().expect("Cannot read output of query.") {
                match cursor
                    .get_data::<&str>(1)
                    .expect("Cannot get Stream Name from db.")
                {
                    Some(val) => {
                        stream_name = val.to_string();
                    }
                    None => {
                        log_debug!(
                            pool.diag_log,
                            "51. Query \"{}\" executed, no data returned",
                            sql_cmd
                        );
                    }
                }
            }
        }
        NoData(_) => {
            log_debug!(
                pool.diag_log,
                "52. Query \"{}\" executed, no data returned",
                sql_cmd
            );
        }
    }

    stream_name
}

fn get_stream_status_by_batch(
    pool_id: i64,
    batch_id: i64,
    ason: String,
    stream_id: i64,
    pool: Data<Params>,
) -> String {
    let batch_status = status_by_id(pool_id, batch_id, ason, pool.clone());
    let mut stream_status: String = String::new();
    for stream in batch_status.streams {
        if stream.stream_id == stream_id {
            stream_status = stream.status;
        }
    }

    stream_status
}

pub fn get_lock_status(batch_id: &i64, ason: &NaiveDateTime, pool: Data<Params>) -> String {
    let env = create_environment_v3()
        .map_err(|e| e.unwrap())
        .expect("Cannot create DB environment.");
    let conn = env
        .connect_with_connection_string(&pool.con_str)
        .expect("Cannot establish a DB connection.");
    let stmt =
        Statement::with_parent(&conn).expect("Cannot create a statement instance to run queries.");
    let sql_cmd = format!(
        "select ApplicationId from BatchApplicationDef where BatchId = {};",
        batch_id
    );
    let mut application_id: String;
    let mut is_locked_status: String;
    match stmt
        .exec_direct(&sql_cmd)
        .expect("Failed to execute a sql cmd!!")
    {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch().expect("Cannot read output of query.") {
                match cursor
                    .get_data::<&str>(1)
                    .expect("Cannot get Stream Name from db.")
                {
                    Some(val) => {
                        application_id = val.to_string();
                        let stmt = Statement::with_parent(&conn)
                            .expect("Cannot create a statement instance to run queries.");
                        let sql_cmd = format!(
                            "select IsLocked from ReportsLockInfoAsOn where ApplicationID = {} and AsOnDate = {};",
                            application_id,
                            ason.timestamp(),
                        );
                        match stmt
                            .exec_direct(&sql_cmd)
                            .expect("Failed to execute a sql cmd!!")
                        {
                            Data(mut stmt) => {
                                while let Some(mut cursor) =
                                    stmt.fetch().expect("Cannot read output of query.")
                                {
                                    match cursor
                                        .get_data::<&str>(1)
                                        .expect("Cannot get Stream Name from db.")
                                    {
                                        Some(val) => {
                                            is_locked_status = val.to_string();
                                            if is_locked_status == "Y" {
                                                return "Y".to_string();
                                            }
                                        }
                                        None => {
                                            log_debug!(
                                                pool.diag_log,
                                                "Query \"{}\" executed, no data returned",
                                                sql_cmd
                                            );
                                        }
                                    }
                                }
                            }
                            NoData(_) => {
                                log_debug!(
                                    pool.diag_log,
                                    "Query \"{}\" executed, no data returned",
                                    sql_cmd
                                );
                            }
                        }
                    }
                    None => {
                        log_debug!(
                            pool.diag_log,
                            "Query \"{}\" executed, no data returned",
                            sql_cmd
                        );
                    }
                }
            }
        }
        NoData(_) => {
            log_debug!(
                pool.diag_log,
                "Query \"{}\" executed, no data returned",
                sql_cmd
            );
        }
    }

    "N".to_string()
}
