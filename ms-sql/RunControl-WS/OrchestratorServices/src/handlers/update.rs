use actix_web::web;
use actix_web::web::Json;
use actix_web::{Error, HttpResponse, Result};
use db::{get_last_assigned_run_id, get_stream_desc};
use odbc::*;
use serde::{Deserialize, Serialize};
use std::fs;
use trigger::StreamDef;

#[derive(Serialize, Deserialize, Debug, Clone)]
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateBatchInfo {
    pub batch_id: i64,
    pub as_on_date: String,
    pub status: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct HealthReport {
    pub tot_accounts: i64,
    pub acc_read_succ: i64,
    pub acc_read_fail: i64,
    pub tot_amt_ip: f64,
    pub tot_amt_op: f64,
    pub tot_no_cf: i64,
}

pub fn update_status(
    path: web::Path<(i64,)>,
    info: Json<UpdateInfo>,
) -> Result<HttpResponse, Error> {
    let con_str = "Driver={ODBC Driver 17 for SQL Server};Server=10.81.4.51;Database=runcontrol;UID=rcuser;PWD=db@123;";
    println!("Updating RunControl Status....\n");
    let as_on_date = chrono::NaiveDate::parse_from_str(&info.as_on_date, "%d-%m-%Y")
        .expect("Cannot parse string as DateTime.")
        .and_hms(0, 0, 0);
    let mut summary_data: Vec<u8> = Vec::new();
    if info.process_report != "".to_string() {
        summary_data = fs::read(&info.process_report).unwrap_or(summary_data);
    }
    let env = create_environment_v3()
        .map_err(|e| e.unwrap())
        .expect("Cannot create DB environment.");
    let conn = env
        .connect_with_connection_string(&con_str)
        .expect("Cannot establish a DB connection.");
    let stmt =
        Statement::with_parent(&conn).expect("Cannot create a statement instance to run queries.");
    let sql_cmd = format!("UPDATE RunControl set ProcessStatus = '{}', ProcessEndTime = {} where RunID = {} AND AsOnDate = {} AND BatchID = {} AND StreamID = {} AND FlowID = {} AND ExecutorID = {} AND ProcessID = {} AND PoolID = {};",
        info.process_status,
        chrono::Utc::now().timestamp(),
        // summary_data,
        info.run_id,
        as_on_date.timestamp(),
        info.batch_id,
        info.stream_id,
        info.flow_id,
        info.executor_id,
        info.process_id,
        path.0
    );
    match stmt
        .exec_direct(&sql_cmd)
        .expect("Failed to execute update status sql cmd!!")
    {
        Data(_) => {}
        NoData(_) => {
            println!("Query \"{}\" executed, no data returned", sql_cmd);
        }
    }
    Ok(HttpResponse::Ok()
        .content_type("text/plain")
        .body("Status Updated!!"))
}

pub fn update_batch_status_holiday(path: web::Path<(i64,)>, info: Json<UpdateBatchInfo>) {
    let con_str = "Driver={ODBC Driver 17 for SQL Server};Server=10.81.4.51;Database=runcontrol;UID=rcuser;PWD=db@123;";
    println!("Updating RunControl Status for Holiday....\n");
    let run_id = get_last_assigned_run_id().unwrap_or(0);
    let as_on_date = chrono::NaiveDate::parse_from_str(&info.as_on_date, "%d-%m-%Y")
        .expect("Cannot parse string as DateTime.")
        .and_hms(0, 0, 0);
    let summary_data: Vec<u8> = Vec::new();
    let mut stream_ids: Vec<i64> = Vec::new();
    let env = create_environment_v3()
        .map_err(|e| e.unwrap())
        .expect("Cannot create DB environment.");
    let conn = env
        .connect_with_connection_string(&con_str)
        .expect("Cannot establish a DB connection.");
    let stmt =
        Statement::with_parent(&conn).expect("Cannot create a statement instance to run queries.");
    let sql_cmd = format!(
        "DELETE from RunControl where AsOnDate = {} AND BatchID = {} AND PoolID = {};",
        as_on_date.timestamp(),
        info.batch_id,
        path.0,
    );
    match stmt
        .exec_direct(&sql_cmd)
        .expect("Failed to execute a sql cmd!!")
    {
        Data(_) => {}
        NoData(_) => {
            println!("Query \"{}\" executed, no data returned", sql_cmd);
        }
    };
    let stmt_2 =
        Statement::with_parent(&conn).expect("Cannot create a statement instance to run queries.");
    let sql_cmd_2 = format!(
        "SELECT StreamID from BatchStream where BatchID = {}",
        info.batch_id
    );
    match stmt_2
        .exec_direct(&sql_cmd_2)
        .expect("Failed to execute a sql cmd!!")
    {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch().expect("Cannot read output of query.") {
                match cursor
                    .get_data::<&str>(1)
                    .expect("Cannot get StreamID from db.")
                {
                    Some(val) => {
                        let id = val.parse::<i64>().expect("Cannot parse string as integer.");
                        stream_ids.push(id);
                    }
                    None => {
                        println!("Query \"{}\" executed, no data returned", sql_cmd_2);
                    }
                }
            }
        }
        NoData(_) => {
            println!("Query \"{}\" executed, no data returned", sql_cmd_2);
        }
    };
    for id in stream_ids {
        let stream: StreamDef = get_stream_desc(as_on_date.format("%d-%m-%Y").to_string(), id)
            .expect("Cannot fetch stream description from db.");
        for flow in stream.flows {
            for process in flow.process {
                let stmt_3 = Statement::with_parent(&conn)
                    .expect("Cannot create a statement instance to run queries.");
                let sql_cmd_3 = format!("INSERT INTO RUNCONTROL (RunID, AsOnDate, BatchID, StreamID, FlowID, ExecutorID, ProcessID, ProcessStartTime, ProcessEndTime, ProcessStatus, ProcessSummary, PoolID) values ({}, {}, {}, {}, {}, {}, {}, {}, {}, '{}', {:?}, {})",
                    run_id,
                    as_on_date.timestamp(),
                    info.batch_id,
                    id,
                    flow.flowId,
                    0,
                    process.processId,
                    chrono::Utc::now().timestamp(),
                    chrono::Utc::now().timestamp(),
                    info.status,
                    summary_data,
                    path.0
                );
                match stmt_3
                    .exec_direct(&sql_cmd_3)
                    .expect("Failed to execute a sql cmd!!")
                {
                    Data(_) => {}
                    NoData(_) => {
                        println!("Query \"{}\" executed, no data returned", sql_cmd_3);
                    }
                };
            }
        }
    }
}
