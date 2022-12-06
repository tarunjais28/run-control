use actix_web::web;
use actix_web::web::{Json, Path};
use actix_web::{Error, HttpResponse, Result};
use chrono::Local;
use chrono::{DateTime, NaiveDateTime, Utc};
use dbpool::OracleConnectionManager;
use handlers::update::HealthReport;
use handlers::TriggerInfo;
use r2d2::Pool;
use rustc_serialize::json;
use serde::{Deserialize, Serialize};
use serde_json;
use std::str;
use trigger::{Bullet, FlowDef, ProcDef, StreamDef};

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

pub fn get_last_assigned_run_id(
    pool: web::Data<Pool<OracleConnectionManager>>,
) -> Result<i64, &'static str> {
    let conn = &pool.get().unwrap().conn;
    match conn {
        Some(db) => {
            let sql = "select max(RunID) as RunID from RunControl";
            let rows = db
                .conn
                .query(sql, &[])
                .expect("Query Failed to Fetch max(RunID) Data from DB.");
            for row_result in &rows {
                let row = row_result.expect("Failed to read query output.");
                let run_id: String = row.get("RunID").unwrap_or("0".to_string());
                return Ok(run_id.parse::<i64>().unwrap_or(0));
            }
        }
        None => return Err("No runs initiated till now."),
    };
    return Err("No runs initiated till now.");
}

pub fn clear_last_run_det(
    trigger_info: TriggerInfo,
    pool: web::Data<Pool<OracleConnectionManager>>,
    poolid: i64,
) {
    let conn = &pool.get().unwrap().conn;
    let as_on_date: chrono::NaiveDate =
        chrono::NaiveDate::parse_from_str(&trigger_info.trigger.as_on_date, "%d-%m-%Y")
            .expect("Cannot parse as on date as NaiveDate");
    for id in trigger_info.trigger.stream_ids {
        match conn {
            Some(db) => match db.conn.execute(
                "delete from RunControl where AsOnDate = :1 AND BatchID = :2 AND StreamID = :3 AND PoolID = :4",
                &[
                    &as_on_date.format("%d-%b-%Y").to_string(),
                    &trigger_info.trigger.batch_id,
                    &id,
                    &poolid
                ],
            ) {
                Ok(_val) => {
                    println!("Delete from table : {} Successful.", "RunControl");
                    match db.conn.commit() {
                        Ok(_) => println!("Commit Successful."),
                        Err(err) => println!("Error: {}", err),
                    }
                }
                Err(err) => {
                    println!(
                        "Cannot Delete from table : {}\n Error: {}",
                        "RunControl", err
                    );
                }
            },
            None => {}
        };
    }
}

// returns id of stream which can be triggered immediately
pub fn get_triggerable_stream_ids(
    trigger_info: TriggerInfo,
    pool: web::Data<Pool<OracleConnectionManager>>,
) -> Vec<i64> {
    let mut triggerable_stream: Vec<i64> = Vec::new();
    for id in trigger_info.trigger.stream_ids.clone() {
        let conn = &pool.get().unwrap().conn;
        match conn {
            Some(db) => {
                let sql = "select * from BATCHSTREAM where BatchID = :1 AND StreamID = :2 AND IsActive = 'Y'";
                let rows = db
                    .conn
                    .query(sql, &[&trigger_info.trigger.batch_id, &id])
                    .expect("Query Failed to Fetch Data from BATCHSTREAM Table DB.");
                for row_result in &rows {
                    let row = row_result.expect("Failed to read query output.");
                    let stream_dep: Vec<u8> = row
                        .get("StreamDep")
                        .expect("Cannot Fetch stream_dep Column details from row result.");
                    match str::from_utf8(&stream_dep) {
                        Ok(val) => {
                            let dep: Dep = serde_json::from_str(val)
                                .expect("Cannot deserialize StreamDep Json.");
                            if dep.dependencies.len() == 0 {
                                triggerable_stream.push(id)
                            } else {
                                let mut status = true;
                                for dep in dep.dependencies {
                                    // if "true" means the status is "SUCCESS"
                                    if get_stream_status(
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
                        Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
                    };
                }
            }
            None => {}
        };
    }
    triggerable_stream
}

// returns stream desc from db
pub fn get_stream_desc(
    asondate: String,
    id: i64,
    pool: web::Data<Pool<OracleConnectionManager>>,
) -> Option<StreamDef> {
    let ason = chrono::NaiveDate::parse_from_str(&asondate, "%d-%m-%Y")
        .expect("Cannot parse ason date as NaiveDate.");
    let ason_dd_mm_yyyy = asondate.clone();
    let mut ason_ddmmyyyy = asondate;
    let ason_ddmonyy = ason.format("%d%b%y").to_string();
    let ason_yyyy_mon_dd = ason.format("%Y_%b_%d").to_string();
    let ason_mon_yyyy = ason.format("%b_%Y").to_string();
    let ason_yyyymmdd = ason.format("%Y%m%d").to_string();
    let ason_mmyyyy = ason.format("%m%Y").to_string();
    let ason_mm_yyyy = ason.format("%m-%Y").to_string();
    let ason_ddmonyyyy = ason.format("%d%b%Y").to_string();
    let timestamp = Local::now()
        .naive_local()
        .format("%d%m%Y_%H%M%S")
        .to_string();
    ason_ddmmyyyy.retain(|v| v != '-');
    let conn = &pool.get().unwrap().conn;
    let mut desc: Option<StreamDef> = None;
    match conn {
        Some(db) => {
            let sql = "select * from StreamDef where StreamID = :1";
            let rows = db
                .conn
                .query(sql, &[&id])
                .expect("Query Failed to Fetch Data from StreamDef DB.");
            for row_result in &rows {
                let row = row_result.expect("Failed to read query output.");
                let stream_desc: Vec<u8> = row
                    .get("StreamDesc")
                    .expect("Cannot Fetch stream_desc Column details from row result.");
                match str::from_utf8(&stream_desc) {
                    Ok(val) => {
                        let stream_desc_ason = &val
                            .replace("{AsOnDate}", &ason_dd_mm_yyyy)
                            .replace("{root}", "/")
                            .replace("{ddmmyyyy}", &ason_ddmmyyyy)
                            .replace("{ddmonyy}", &ason_ddmonyy)
                            .replace("{ddmonyyyy}", &ason_ddmonyyyy)
                            .replace("{yyyy_mon_dd}", &ason_yyyy_mon_dd)
                            .replace("{mon_yyyy}", &ason_mon_yyyy)
                            .replace("{mmyyyy}", &ason_mmyyyy)
                            .replace("{mm-yyyy}", &ason_mm_yyyy)
                            .replace("{yyyymmdd}", &ason_yyyymmdd)
                            .replace("{ddmmyyyy_hhmmss}", &timestamp);

                        desc = Some(
                            serde_json::from_str(stream_desc_ason)
                                .expect("Cannot deserialize fetched stream desc."),
                        )
                    }
                    Err(_) => {}
                }
            }
        }
        None => {}
    };
    desc
}

// returns id of flows which can be triggered immediately
pub fn get_triggerable_flow_ids(
    stream: StreamDef,
    trigger: TriggerInfo,
    pool: web::Data<Pool<OracleConnectionManager>>,
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
    s_id: i64,
    flow: FlowDef,
    trigger: TriggerInfo,
    pool: web::Data<Pool<OracleConnectionManager>>,
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
    trigger_info: TriggerInfo,
    s_id: i64,
    key_status: String,
    pool: web::Data<Pool<OracleConnectionManager>>,
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
    trigger_info: TriggerInfo,
    flow: FlowDef,
    s_id: i64,
    key_status: String,
    pool: web::Data<Pool<OracleConnectionManager>>,
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
    trigger_info: TriggerInfo,
    p_id: i64,
    f_id: i64,
    s_id: i64,
    key_status: String,
    pool: web::Data<Pool<OracleConnectionManager>>,
) -> bool {
    let conn = &pool.get().unwrap().conn;
    let mut status = false;
    let as_on_date =
        chrono::NaiveDate::parse_from_str(&trigger_info.trigger.as_on_date, "%d-%m-%Y")
            .expect("Cannot parse string as NaiveDate")
            .and_hms(0, 0, 0);
    match conn {
        Some(db) => {
            let sql = "select * from RunControl where BatchID = :1 AND StreamID = :2 AND FlowID = :3 AND ProcessID = :4 AND AsOnDate = :5";
            let rows = db
                .conn
                .query(
                    sql,
                    &[
                        &trigger_info.trigger.batch_id,
                        &s_id,
                        &f_id,
                        &p_id,
                        &as_on_date.date().format("%d-%b-%y").to_string(),
                    ],
                )
                .expect("Query Failed to Fetch RunControl data from DB.");
            for row_result in &rows {
                let row = row_result.expect("Failed to read get process status query output.");
                let ProcessStatus: String = row
                    .get("ProcessStatus")
                    .expect("Cannot Fetch ProcessStatus Column details from row result.");
                if ProcessStatus == key_status {
                    status = true;
                }
            }
        }
        None => {}
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
pub fn get_batch_status(
    trigger: TriggerInfo,
    pool: web::Data<Pool<OracleConnectionManager>>,
) -> bool {
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
                if !process_executed(bullet, pool.clone()) {
                    status = false;
                }
            }
        }
    }
    status
}

// returns the pool id for batch
pub fn get_pool_id(id: i64, pool: web::Data<Pool<OracleConnectionManager>>) -> i64 {
    let conn = &pool.get().unwrap().conn;
    let mut pool_id: i64 = 0;
    match conn {
        Some(db) => {
            let sql = "select PoolID from BatchDef where BatchID = :1";
            let rows = db
                .conn
                .query(sql, &[&id])
                .expect("Query Failed to Fetch PoolID from RunControl from DB.");
            for row_result in &rows {
                let row = row_result.expect("Failed to read query output.");
                pool_id = row.get("PoolID").unwrap_or(0);
            }
        }
        None => {
            println!("Could Not connect to DB.");
        }
    }
    pool_id
}

// returns collection of all the workers assigned to a pool
pub fn get_actual_worker_id(
    pool_id: i64,
    logical_worker_id: i64,
    pool: web::Data<Pool<OracleConnectionManager>>,
) -> i64 {
    let conn = &pool.get().unwrap().conn;
    let mut worker_id: i64 = 0;
    match conn {
        Some(db) => {
            let sql = "select ExecutorID from ExecutorPool where PoolID = :1";
            let rows = db
                .conn
                .query(sql, &[&pool_id])
                .expect("Query Failed to Fetch ExecutorID from ExecutorPool from DB.");
            for row_result in &rows {
                let row = row_result.expect("Failed to read query output.");
                let exe_id: i64 = row
                    .get("ExecutorID")
                    .expect("Cannot Fetch ExecutorID Column details from row result.");
                if logical_worker_id == exe_id {
                    worker_id = exe_id;
                    break;
                } else {
                    worker_id = exe_id;
                }
            }
        }
        None => {}
    }
    worker_id
}

// returns status of a worker
pub fn check_worker_status(id: i64, pool: web::Data<Pool<OracleConnectionManager>>) -> bool {
    let conn = &pool.get().unwrap().conn;
    let mut status = true;
    match conn {
        Some(db) => {
            let sql = "select ProcessStatus from RunControl where ExecutorID = :1 AND rownum = 1
                        ORDER BY RunID DESC";
            let rows = db
                .conn
                .query(sql, &[&id])
                .expect("Query Failed to Fetch ProcessStatus from RunControl from DB.");
            for row_result in &rows {
                let row = row_result.expect("Failed to read query output.");
                let ProcessStatus: String = row
                    .get("ProcessStatus")
                    .expect("Cannot Fetch ProcessStatus Column details from row result.");
                if ProcessStatus == "PROCESSING".to_string() {
                    status = false;
                }
            }
        }
        None => {}
    }
    status
}

// returns connection url of the worker
pub fn get_executor_connection_url(
    id: i64,
    pool: web::Data<Pool<OracleConnectionManager>>,
) -> String {
    let conn = &pool.get().unwrap().conn;
    let mut url: String = String::new();
    match conn {
        Some(db) => {
            let sql = "select ExecutorURL from ExecutorDef where ExecutorID = :1";
            let rows = db
                .conn
                .query(sql, &[&id])
                .expect("Query Failed to Fetch ExecutorURL from ExecutorDef from DB.");
            for row_result in &rows {
                let row = row_result.expect("Failed to read query output.");
                url = row
                    .get("ExecutorURL")
                    .expect("Cannot Fetch ExecutorID Column details from row result.");
            }
        }
        None => {}
    }
    url
}

// add detail of queued process
pub fn add_to_run_control(
    pool_id: i64,
    status: &str,
    worker_id: i64,
    info: Bullet,
    pool: web::Data<Pool<OracleConnectionManager>>,
) {
    let asondate_timestamp: i64 = chrono::NaiveDate::parse_from_str(&info.asondate, "%d-%m-%Y")
        .expect("Cannot parse string as NaiveDate.")
        .and_hms(0, 0, 0)
        .timestamp();
    let conn = &pool.get().unwrap().conn;
    match conn {
        Some(db) => {
            // Insert Query
            match db.conn.execute(
        "INSERT INTO RUNCONTROL (RunID, AsOnDate, BatchID, StreamID, FlowID, ExecutorID, ProcessID, ProcessStartTime, ProcessStatus, PoolID) values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)",
        &[
            &info.run_id,
            &DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(asondate_timestamp, 0), Utc),
            &info.batch_id,
            &info.stream_id,
            &info.flow_id,
            &worker_id,
            &info.process_id,
            &Utc::now(),
            &status,
            &pool_id
        ],
    )   {
        Ok(_) => {
            println!("Insert into table : {} Successful.", "RunControl");
        },
        Err(err) => {
            println!("Cannot insert into table : {}\n Error: {}", "RunControl", err);
        }
    }
            match db.conn.commit() {
                Ok(_) => println!("Commit Successful."),
                Err(err) => println!("Error: {}", err),
            }
        }
        None => {}
    }
}

pub fn get_batches(
    path: Path<(i64,)>,
    pool: web::Data<Pool<OracleConnectionManager>>,
) -> Result<HttpResponse, Error> {
    let conn = &pool.get().unwrap().conn;
    let mut batches: Vec<Batch> = Vec::new();
    match conn {
        Some(db) => {
            let sql = "select * from BatchDef where PoolID = :1";
            let rows = db
                .conn
                .query(sql, &[&path.0])
                .expect("Query Failed to Fetch BatchDef from DB.");
            for row_result in &rows {
                let row = row_result.expect("Failed to read query output.");
                let batch_id: i64 = row.get("BatchID").expect("Cannot get batch id from db.");
                let batch_name: String = row
                    .get("BatchName")
                    .expect("Cannot get batch name from db.");
                let batch = Batch {
                    batch_id: batch_id,
                    batch_name: batch_name,
                };
                batches.push(batch);
            }
        }
        None => {}
    };
    // Serialize using `json::encode`
    let batches_info = json::encode(&batches).expect("Cannot serialize as JSON.");
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(batches_info))
}

pub fn get_streams(
    _path: Path<(i64,)>,
    info: Json<StatusReq>,
    pool: web::Data<Pool<OracleConnectionManager>>,
) -> Result<HttpResponse, Error> {
    let conn = &pool.get().unwrap().conn;
    let mut streams: Vec<Stream> = Vec::new();
    let id: i64 = info.batch_id;
    let mut stream_id: Vec<i64> = Vec::new();
    let mut stream_name = String::new();
    match conn {
        Some(db) => {
            let sql = "select * from BatchStream where BatchID = :1 AND IsActive = 'Y'";
            let rows = db
                .conn
                .query(sql, &[&id])
                .expect("Query Failed to Fetch BatchStream from DB.");
            for row_result in &rows {
                let row = row_result.expect("Failed to read query output.");
                let id = row.get("StreamID").expect("Cannot get StreamID from db.");
                stream_id.push(id);
            }
        }
        None => {}
    };
    for id in stream_id {
        match conn {
            Some(db) => {
                let sql = "select * from StreamDef where StreamID = :1";
                let rows = db
                    .conn
                    .query(sql, &[&id])
                    .expect("Query Failed to Fetch StreamDef from DB.");
                for row_result in &rows {
                    let row = row_result.expect("Failed to read query output.");
                    stream_name = row
                        .get("StreamName")
                        .expect("Cannot get StreamName from db.");
                }
            }
            None => {}
        };
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

pub fn get_batch_status_by_id(
    path: Path<(i64,)>,
    info: Json<RunReqStatus>,
    pool: web::Data<Pool<OracleConnectionManager>>,
) -> Result<HttpResponse, Error> {
    let as_on_date = chrono::NaiveDate::parse_from_str(&info.as_on_date, "%Y-%m-%d")
        .expect("Cannot parse string as NaiveDate.");
    let resp = status_by_id(
        path.0,
        info.batch_id,
        as_on_date.format("%d-%m-%Y").to_string(),
        pool,
    );
    let batches_info = json::encode(&resp).expect("Cannot serialize as JSON.");
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(batches_info))
}

pub fn get_batch_status_latest(
    path: Path<(i64,)>,
    pool: web::Data<Pool<OracleConnectionManager>>,
) -> Result<HttpResponse, Error> {
    let run_id = get_last_assigned_run_id(pool.clone()).unwrap_or(0);
    let batch_id = get_batch_id_latest(run_id, pool.clone()).unwrap_or(0);
    let ason = fetch_latest_ason(pool.clone())
        .date()
        .format("%d-%m-%Y")
        .to_string();
    let resp = status_by_id(path.0, batch_id, ason, pool);
    let batches_info = json::encode(&resp).expect("Cannot serialize as JSON.");
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(batches_info))
}

pub fn get_batch_full_status(
    path: Path<(i64,)>,
    info: Json<RunReqStatus>,
    pool: web::Data<Pool<OracleConnectionManager>>,
) -> Result<HttpResponse, Error> {
    let mut streams: Vec<i64> = Vec::new();
    let asondate = chrono::NaiveDate::parse_from_str(&info.as_on_date, "%Y-%m-%d")
        .expect("Cannot Parse ason date to a valid date type.")
        .and_hms(0, 0, 0);
    let conn = &pool.get().unwrap().conn;
    match conn {
        Some(db) => {
            let sql = "select * from BatchStream where BatchID = :1 AND IsActive = 'Y'";
            let rows = db
                .conn
                .query(sql, &[&info.batch_id])
                .expect("Query Failed to Fetch BatchStream from DB.");
            for row_result in &rows {
                let row = row_result.expect("Failed to read query output.");
                let stream_id = row.get("StreamID").expect("Cannot get StreamID from db.");
                streams.push(stream_id);
            }
        }
        None => {}
    };
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
        let mut tt_by_stream_secs = 0;
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
                let mut total_cfs = 0;
                match conn {
                    Some(db) => {
                        let sql = "select * from RunControl where BatchID = :1 AND StreamID = :2 AND PoolID = :3 AND AsOnDate = :4 AND FlowID = :5 AND ProcessID = :6";
                        let rows = db
                            .conn
                            .query(
                                sql,
                                &[
                                    &info.batch_id,
                                    &stream_id,
                                    &path.0,
                                    &asondate.date().format("%d-%b-%y").to_string(),
                                    &flow.flowId.parse::<i64>().unwrap_or(0),
                                    &process.processId.parse::<i64>().unwrap_or(0),
                                ],
                            )
                            .expect("Query Failed to Fetch RunControl Data from DB.");
                        for row_result in &rows {
                            let row = row_result.expect("Failed to read query output.");
                            proc_start_time = row
                                .get("ProcessStartTime")
                                .unwrap_or(NaiveDateTime::from_timestamp(0, 0));
                            proc_end_time = row
                                .get("ProcessEndTime")
                                .unwrap_or(NaiveDateTime::from_timestamp(0, 0));
                            process_report = match row.get("ProcessSummary") {
                                Ok(val) => val,
                                Err(_e) => process_report,
                            };
                            proc_final_status = row.get("ProcessStatus").unwrap_or("-".to_string());
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
                                        total_cfs = report.tot_no_cf;
                                    }
                                    Err(_e) => {}
                                };
                            }
                        }
                    }
                    None => {}
                };
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
            tt_by_stream_secs += tt_by_flow_secs;
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
                path.0,
                info.batch_id,
                asondate.format("%d-%m-%Y").to_string(),
                stream_id,
                pool.clone(),
            ),
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
    run_id: i64,
    pool: web::Data<Pool<OracleConnectionManager>>,
) -> Result<i64, &'static str> {
    let conn = &pool.get().unwrap().conn;
    match conn {
        Some(db) => {
            let sql = "select BatchID from RunControl where RunID = :1";
            let rows = db
                .conn
                .query(sql, &[&run_id])
                .expect("Query Failed to Fetch BatchID Data from DB.");
            for row_result in &rows {
                let row = row_result.expect("Failed to read query output.");
                let batch_id: String = row.get("BatchID").unwrap_or("0".to_string());
                return Ok(batch_id.parse::<i64>().unwrap_or(0));
            }
        }
        None => return Err("No runs initiated till now."),
    };
    return Err("No runs initiated till now.");
}

fn status_by_id(
    pool_id: i64,
    batchid: i64,
    ason: String,
    pool: web::Data<Pool<OracleConnectionManager>>,
) -> RunStatus {
    let conn = &pool.get().unwrap().conn;
    let mut run_id: i64 = 0;
    let mut streams_status: Vec<StreamStatus> = Vec::new();
    let asondate = chrono::NaiveDate::parse_from_str(&ason, "%d-%m-%Y")
        .expect("Cannot Parse ason date to a valid date type.")
        .and_hms(0, 0, 0);
    let mut streams: Vec<i64> = Vec::new();
    match conn {
        Some(db) => {
            let sql = "select StreamID from BatchStream where BatchID = :1 AND IsActive = 'Y'";
            let rows = db
                .conn
                .query(sql, &[&batchid])
                .expect("Query Failed to Fetch BatchStream from DB.");
            for row_result in &rows {
                let row = row_result.expect("Failed to read query output.");
                let stream_id = row.get("StreamID").expect("Cannot get StreamID from db.");
                streams.push(stream_id);
            }
        }
        None => {}
    };
    for stream_id in streams {
        match conn {
            Some(db) => {
                let sql = "select max(RunID) as RunID from RunControl where BatchID = :1 AND StreamID = :2 AND PoolID = :3 AND AsOnDate = :4";
                let rows = db
                    .conn
                    .query(
                        sql,
                        &[
                            &batchid,
                            &stream_id,
                            &pool_id,
                            &asondate.date().format("%d-%b-%y").to_string(),
                        ],
                    )
                    .expect("Query Failed to Fetch max(RunID) Data from DB.");
                for row_result in &rows {
                    let row = row_result.expect("Failed to read query output.");
                    run_id = row.get(0).unwrap_or(0);
                }
            }
            None => {}
        };
        let stream: StreamDef = get_stream_desc(ason.clone(), stream_id, pool.clone())
            .expect("Cannot fetch stream description from db.");
        let mut stream_status = "NOT STARTED";
        let conn = &pool.get().unwrap().conn;
        let mut success_count = 0;
        match conn {
            Some(db) => {
                let sql = "select ProcessStatus from RunControl where AsOnDate = :1 AND BatchID = :2 AND StreamID = :3 AND RunID = :4 AND PoolID = :5";
                let rows = db
                    .conn
                    .query(
                        sql,
                        &[
                            &asondate.date().format("%d-%b-%y").to_string(),
                            &batchid,
                            &stream_id,
                            &run_id,
                            &pool_id,
                        ],
                    )
                    .expect("Query Failed to Fetch max(AsOnDate) from DB.");
                for row_result in &rows {
                    let row = row_result.expect("Failed to read query output.");
                    let status: String = row
                        .get("ProcessStatus")
                        .expect("Cannot read ProcessStatus.");
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
            }
            None => {}
        };
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

pub fn get_lastest_ason(
    _path: Path<(i64,)>,
    pool: web::Data<Pool<OracleConnectionManager>>,
) -> Result<HttpResponse, Error> {
    let asondate = fetch_latest_ason(pool);
    let latest_ason = asondate.date().format("%Y-%m-%d").to_string();
    let ason = AsOn {
        as_on_date: latest_ason,
    };
    let resp = json::encode(&ason).expect("Cannot serialize as JSON.");
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(resp))
}

pub fn get_batch_lastest_ason(
    path: Path<(i64,)>,
    body: Json<StatusReq>,
    pool: web::Data<Pool<OracleConnectionManager>>,
) -> Result<HttpResponse, Error> {
    let asondate = fetch_latest_ason_by_batch(path.0, body.batch_id, pool);
    let latest_ason = asondate.date().format("%Y-%m-%d").to_string();
    let ason = AsOn {
        as_on_date: latest_ason,
    };
    let resp = json::encode(&ason).expect("Cannot serialize as JSON.");
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(resp))
}

pub fn get_batch_last_success_ason(
    path: Path<(i64,)>,
    body: Json<StatusReq>,
    pool: web::Data<Pool<OracleConnectionManager>>,
) -> Result<HttpResponse, Error> {
    println!(
        "Request received for `get_batch_last_success_ason` with body: {:#?}",
        body
    );
    let asondate = fetch_batch_last_success_ason(path.0, body.batch_id, pool);
    let last_ason = asondate.date().format("%Y-%m-%d").to_string();
    Ok(HttpResponse::Ok()
        .content_type("text/plain")
        .body(last_ason))
}

fn fetch_latest_ason(pool: web::Data<Pool<OracleConnectionManager>>) -> DateTime<Utc> {
    let conn = &pool.get().unwrap().conn;
    let mut asondate: DateTime<Utc> = Utc::now();
    match conn {
        Some(db) => {
            let sql = "select max(AsOnDate) from RunControl";
            let rows = db
                .conn
                .query(sql, &[])
                .expect("Query Failed to Fetch max(AsOnDate) from DB.");
            for row_result in &rows {
                let row = row_result.expect("Failed to read query output.");
                asondate = row.get(0).unwrap_or(Utc::now());
            }
        }
        None => {}
    };
    asondate
}

fn fetch_latest_ason_by_batch(
    pool_id: i64,
    b_id: i64,
    pool: web::Data<Pool<OracleConnectionManager>>,
) -> DateTime<Utc> {
    let conn = &pool.get().unwrap().conn;
    let mut asondate: DateTime<Utc> = Utc::now();
    match conn {
        Some(db) => {
            let sql = "select max(AsOnDate) from RunControl where BatchID = :1 AND PoolID = :2";
            let rows = db
                .conn
                .query(sql, &[&b_id, &pool_id])
                .expect("Query Failed to Fetch max(AsOnDate) from DB.");
            for row_result in &rows {
                let row = row_result.expect("Failed to read query output.");
                asondate = row.get(0).unwrap_or(Utc::now());
            }
        }
        None => {}
    };
    asondate
}

fn fetch_batch_last_success_ason(
    pool_id: i64,
    b_id: i64,
    pool: web::Data<Pool<OracleConnectionManager>>,
) -> DateTime<Utc> {
    let conn = &pool.get().unwrap().conn;
    let mut asondate: DateTime<Utc> = Utc::now() - chrono::Duration::days(1);
    match conn {
        Some(db) => {
            let sql =
                "select AsOnDate from RunControl where BatchID = :1 AND PoolID = :2 ORDER BY RunID DESC";
            let rows = db
                .conn
                .query(sql, &[&b_id, &pool_id])
                .expect("Query Failed to Fetch data from RunControl Table.");
            for row_result in &rows {
                let mut is_success = true;
                let row = row_result.expect("Failed to read query output.");
                asondate = row.get("AsOnDate").unwrap_or(Utc::now());
                let batch_status = status_by_id(
                    pool_id,
                    b_id,
                    asondate.date().format("%d-%m-%Y").to_string(),
                    pool.clone(),
                );
                for stream in batch_status.streams {
                    if stream.status != "SUCCESS" {
                        is_success = false;
                    }
                }
                if is_success {
                    return asondate;
                }
            }
        }
        None => {}
    };
    asondate
}

pub fn process_executed(bullet: Bullet, pool: web::Data<Pool<OracleConnectionManager>>) -> bool {
    let conn = &pool.get().unwrap().conn;
    let mut status = false;
    let asondate = chrono::NaiveDate::parse_from_str(&bullet.asondate, "%d-%m-%Y")
        .expect("Cannot parse string as NaiveDate")
        .and_hms(0, 0, 0);
    match conn {
        Some(db) => {
            let sql = "select * from RunControl where BatchID = :1 AND StreamID = :2 AND FlowID = :3 AND ProcessID = :4 AND AsOnDate = :5 AND RunID = :6";
            let rows = db
                .conn
                .query(
                    sql,
                    &[
                        &bullet.batch_id,
                        &bullet.stream_id,
                        &bullet.flow_id,
                        &bullet.process_id,
                        &asondate.date().format("%d-%b-%y").to_string(),
                        &bullet.run_id,
                    ],
                )
                .expect("Query Failed to Fetch RunControl data from DB.");
            for row_result in &rows {
                let row = row_result.expect("Failed to read get process status query output.");
                let ProcessStatus: String = row
                    .get("ProcessStatus")
                    .expect("Cannot Fetch ProcessStatus Column details from row result.");
                if ProcessStatus == "FAIL".to_string()
                    || ProcessStatus == "PROCESSING".to_string()
                    || ProcessStatus == "SUCCESS".to_string()
                {
                    status = true;
                }
            }
        }
        None => {}
    };
    status
}

pub fn check_additional_failed_processes(
    pool_id: i64,
    trigger_info: TriggerInfo,
    pool: web::Data<Pool<OracleConnectionManager>>,
) {
    print!("Deleting additional failed process.");
    let mut fail_set: Vec<Fail> = Vec::new();
    let mut failed_stream_ids = Vec::new();
    let mut failed_flow_ids: Vec<i64> = Vec::new();
    let mut failed_process_ids: Vec<i64> = Vec::new();
    let ason = trigger_info.trigger.as_on_date.clone();
    let conn = &pool.get().unwrap().conn;
    match conn {
        Some(db) => {
            let sql = "select * from RunControl where RunID = :1";
            let rows = db
                .conn
                .query(sql, &[&trigger_info.run_id])
                .expect("Query Failed to Fetch Run Details from DB.");
            for row_result in &rows {
                let row = row_result.expect("Failed to read query output.");
                let status: String = row
                    .get("ProcessStatus")
                    .expect("Cannot read ProcessStatus value from DB.");
                let stream_id: i64 = row
                    .get("StreamID")
                    .expect("Cannot read StreamID value from DB.");
                let flow_id: i64 = row
                    .get("FlowID")
                    .expect("Cannot read FlowID value from DB.");
                let process_id: i64 = row
                    .get("ProcessID")
                    .expect("Cannot read ProcessID value from DB.");
                if status == "FAIL".to_string() {
                    failed_stream_ids.push(stream_id);
                    failed_flow_ids.push(flow_id);
                    failed_process_ids.push(process_id);
                }
            }
        }
        None => {}
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
                    if failed_flow_ids.contains(&f_id) {
                        for process in &flow.process {
                            let fail_element = Fail {
                                run_id: trigger_info.run_id,
                                asondate: ason.clone(),
                                batch_id: trigger_info.trigger.batch_id,
                                stream_id: *id,
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
            for flow in stream.flows {
                let flow_id = flow.flowId.parse::<i64>().unwrap();
                for process in flow.process {
                    let dep_proc_ids: Vec<i64> = get_proc_dep(process.clone());
                    for p_id in dep_proc_ids {
                        if failed_process_ids.contains(&p_id) && failed_flow_ids.contains(&flow_id)
                        {
                            let fail_element = Fail {
                                run_id: trigger_info.run_id,
                                asondate: ason.clone(),
                                batch_id: trigger_info.trigger.batch_id,
                                stream_id: *id,
                                flow_id: flow_id,
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
                            failed_process_ids.push(*id);
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
            trigger_info.clone(),
            info.process_id,
            info.flow_id,
            info.stream_id,
            "FAIL".to_string(),
            pool.clone(),
        );
        let is_success = get_process_status(
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

fn get_stream_dep(
    batchid: i64,
    stream_id: i64,
    pool: web::Data<Pool<OracleConnectionManager>>,
) -> Vec<i64> {
    let conn = &pool.get().unwrap().conn;
    let mut res: Vec<i64> = Vec::new();
    match conn {
        Some(db) => {
            let sql =
                "select * from BATCHSTREAM where BatchID = :1 AND StreamID = :2 AND IsActive = 'Y'";
            let rows = db
                .conn
                .query(sql, &[&batchid, &stream_id])
                .expect("Query Failed to Fetch Data from BATCHSTREAM Table DB.");
            for row_result in &rows {
                let row = row_result.expect("Failed to read query output.");
                let stream_dep: Vec<u8> = row
                    .get("StreamDep")
                    .expect("Cannot Fetch stream_dep Column details from row result.");
                match str::from_utf8(&stream_dep) {
                    Ok(val) => {
                        let dep: Dep =
                            serde_json::from_str(val).expect("Cannot deserialize StreamDep Json.");
                        res = dep.dependencies;
                    }
                    Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
                };
            }
        }
        None => {}
    };
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

fn get_stream_name(stream_id: i64, pool: web::Data<Pool<OracleConnectionManager>>) -> String {
    let conn = &pool.get().unwrap().conn;
    let mut stream_name: String = String::new();
    match conn {
        Some(db) => {
            let sql = "select * from StreamDef where StreamID = :1";
            let rows = db
                .conn
                .query(sql, &[&stream_id])
                .expect("Query Failed to Fetch StreamDef Data from DB.");
            for row_result in &rows {
                let row = row_result.expect("Failed to read query output.");
                stream_name = row.get("StreamName").unwrap_or("0".to_string());
            }
        }
        None => {}
    };
    stream_name
}

fn get_stream_status_by_batch(
    pool_id: i64,
    batch_id: i64,
    ason: String,
    stream_id: i64,
    pool: web::Data<Pool<OracleConnectionManager>>,
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
