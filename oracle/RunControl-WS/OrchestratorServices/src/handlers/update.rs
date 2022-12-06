use actix_web::web;
use actix_web::web::Json;
use actix_web::{Error, HttpResponse};
use db::{get_last_assigned_run_id, get_stream_desc};
use dbpool::OracleConnectionManager;
use r2d2::Pool;
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
    pool: web::Data<Pool<OracleConnectionManager>>,
) {
    println!("Updating RunControl Status....\n");
    let conn = &pool.get().unwrap().conn;
    let as_on_date = chrono::NaiveDate::parse_from_str(&info.as_on_date, "%d-%m-%Y")
        .expect("Cannot parse string as DateTime.")
        .and_hms(0, 0, 0);
    let mut summary_data: Vec<u8> = Vec::new();
    if info.process_report != "".to_string() {
        summary_data = fs::read(&info.process_report).unwrap_or(summary_data);
    }

    match conn {
        Some(db) => {
            match db.conn.execute(
        "update RunControl set ProcessStatus = :1, ProcessEndTime = :2, ProcessSummary = :3
        where RunID = :4 AND AsOnDate = :5 AND BatchID = :6 AND StreamID = :7 AND FlowID = :8 AND ExecutorID = :9 AND ProcessID = :10 AND PoolID = :11",
        &[
            &info.process_status,
            &chrono::Utc::now(),
            &summary_data,
            &info.run_id,
            &as_on_date.format("%d-%b-%Y").to_string(),
            &info.batch_id,
            &info.stream_id,
            &info.flow_id,
            &info.executor_id,
            &info.process_id,
            &path.0
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

pub fn update_batch_status_holiday(
    path: web::Path<(i64,)>,
    info: Json<UpdateBatchInfo>,
    pool: web::Data<Pool<OracleConnectionManager>>,
) -> Result<HttpResponse, Error> {
    println!("Updating RunControl Status for Holiday....\n");
    let run_id = get_last_assigned_run_id(path.0, pool.clone()).unwrap_or(0);
    let conn = &pool.get().unwrap().conn;
    let as_on_date = chrono::NaiveDate::parse_from_str(&info.as_on_date, "%d-%m-%Y")
        .expect("Cannot parse string as DateTime.")
        .and_hms(0, 0, 0);
    let summary_data: Vec<u8> = Vec::new();
    let mut stream_ids: Vec<i64> = Vec::new();
    match conn {
        Some(db) => match db.conn.execute(
            "delete from RunControl where AsOnDate = :1 AND BatchID = :2 AND PoolID = :3",
            &[
                &as_on_date.format("%d-%b-%Y").to_string(),
                &info.batch_id,
                &path.0,
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
    match conn {
        Some(db) => {
            let sql = "select StreamID from BatchStream where BatchID = :1 AND IsActive = 'Y'";
            let rows = db
                .conn
                .query(sql, &[&info.batch_id])
                .expect("Query Failed to Fetch BatchStream from DB.");
            for row_result in &rows {
                let row = row_result.expect("Failed to read query output.");
                let id = row.get("StreamID").expect("Cannot get StreamID from db.");
                stream_ids.push(id);
            }
        }
        None => {}
    };
    for id in stream_ids {
        let stream: StreamDef =
            get_stream_desc(as_on_date.format("%d-%m-%Y").to_string(), id, pool.clone())
                .expect("Cannot fetch stream description from db.");
        for flow in stream.flows {
            for process in flow.process {
                match conn {
                    Some(db) => {
                        // Insert Query
                        match db.conn.execute(
        "INSERT INTO RUNCONTROL (RunID, AsOnDate, BatchID, StreamID, FlowID, ExecutorID, ProcessID, ProcessStartTime, ProcessEndTime, ProcessStatus, ProcessSummary, PoolID) values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12)",
        &[
            &run_id,
            &as_on_date.format("%d-%b-%Y").to_string(),
            &info.batch_id,
            &id,
            &flow.flowId,
            &0,
            &process.processId,
            &chrono::Utc::now(),
            &chrono::Utc::now(),
            &info.status,
            &summary_data,
            &path.0
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
        }
    }
    Ok(HttpResponse::Ok()
        .content_type("text/plain")
        .body("Status Updated for Holiday."))
}
