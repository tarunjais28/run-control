use curl::easy::{Easy, List};
use db;
use handlers::TriggerInfo;
use rustc_serialize::json;
use serde::{Deserialize, Serialize};
use std::io::{stdout, Read, Write};
use std::{thread, time};
use chrono;

// StreamDef Struct
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StreamDef {
    pub streamName: String,
    pub streamId: String,
    pub flows: Vec<FlowDef>,
}

// FlowDef Struct
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FlowDef {
    pub name: String,
    pub flowId: String,
    pub flowDependencies: Vec<String>,
    pub executorID: String,
    pub process: Vec<ProcDef>,
}

// ProcDef struct
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProcDef {
    pub processName: String,
    pub processId: String,
    pub processBinary: String,
    pub processArguments: Vec<String>,
    pub processDependencies: Vec<String>,
    pub processReport: String,
}

#[derive(Serialize, Deserialize, Debug, RustcDecodable, RustcEncodable, Clone)]
pub struct Bullet {
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

pub fn fire(poolid: i64, trigger: TriggerInfo) {
    db::clear_last_run_det(trigger.clone(), poolid);
    loop {
        let stream_ids: Vec<i64> = db::get_triggerable_stream_ids(trigger.clone());
        for id in stream_ids {
            let stream: StreamDef = db::get_stream_desc(trigger.trigger.as_on_date.clone(), id)
                .expect("Cannot fetch stream description from db.");
            let flow_ids: Vec<i64> = db::get_triggerable_flow_ids(stream.clone(), trigger.clone());
            for flow in stream.flows {
                let logical_worker_id = flow
                    .executorID
                    .parse::<i64>()
                    .expect("Cannot parse string as integer.");
                let flow_id: i64 = flow
                    .flowId
                    .parse::<i64>()
                    .expect("Cannot parse flow id in stream description.");
                if flow_ids.contains(&flow_id) {
                    let process_ids: Vec<i64> =
                        db::get_triggerable_process_ids(id, flow.clone(), trigger.clone());
                    for process in flow.process {
                        let process_id: i64 = process
                            .processId
                            .parse::<i64>()
                            .expect("Cannot parse process id in flow description.");
                        if process_ids.contains(&process_id) {
                            let mut bullet = Bullet {
                                run_id: trigger.run_id,
                                asondate: trigger.trigger.as_on_date.clone(),
                                batch_id: trigger.trigger.batch_id,
                                stream_id: id,
                                flow_id: flow_id,
                                executor_id: logical_worker_id,
                                process_id: process_id,
                                process_name: process.processName,
                                process_binary: process.processBinary,
                                process_args: process.processArguments,
                                process_report: process.processReport,
                            };
                            if !db::process_executed(bullet.clone()) {
                                // get pool id
                                let pool_id = db::get_pool_id(trigger.trigger.batch_id);
                                let actual_worker_id =
                                    db::get_actual_worker_id(pool_id, logical_worker_id);
                                let worker_status: bool = db::check_worker_status(actual_worker_id);
                                let conn_addr = db::get_executor_connection_url(actual_worker_id);
                                if worker_status {
                                    bullet.executor_id = actual_worker_id;
                                    execute(poolid, actual_worker_id, bullet.clone(), &conn_addr);
                                }
                            }
                        }
                    }
                }
            }
        }
        db::check_additional_failed_processes(poolid, trigger.clone());
        if db::get_batch_status(trigger.clone()) {
            break;
        }
        let sleep_time = time::Duration::from_secs(5);
        thread::sleep(sleep_time);
    }
    println!("Batch Executed!!");
}

fn execute(pool_id: i64, id: i64, info: Bullet, conn_addr: &str) {
    db::add_to_run_control(pool_id, "PROCESSING", id, info.clone());
    let body = json::encode(&info).expect("Cannot serialize as JSON.");
    match pull_trigger(conn_addr, body) {
        Ok(res) => {
            println!("{:#?}\n", res);
            println!("Process Strted =\n StreamID = {} \n StreamName = {} \n Time = {:?}", 
            info.stream_id, info.process_name, chrono::offset::Local::now());
            println!("\nProcess Executed.\n",);
        }
        Err(err) => {
            println!("{:#?}", err);
            println!("Process Strted =\n StreamID = {} \n StreamName = {} \n Time = {:?}", 
            info.stream_id, info.process_name, chrono::offset::Local::now());
            println!("\nProcess Execution Failed.\n");
        }
    }
}

fn pull_trigger(conn_addr: &str, body: String) -> Result<(), String> {
    let mut data_to_upload = body.as_str().as_bytes();
    let mut handle = Easy::new();
    let conn_url: String = format!("http://{}/execute", conn_addr);
    let mut list = List::new();
    list.append("Content-Type: application/json").unwrap();
    // let _ = handle.ssl_verify_peer(false);
    // let _ = handle.ssl_verify_host(false);
    handle.url(&conn_url).unwrap();
    handle.http_headers(list).unwrap();
    handle.post_field_size(body.len() as u64).unwrap();
    handle.post(true).unwrap();

    let mut transfer = handle.transfer();
    transfer
        .read_function(|into| Ok(data_to_upload.read(into).unwrap()))
        .unwrap();
    transfer
        .write_function(|data| Ok(stdout().write(data).unwrap()))
        .unwrap();
    transfer.perform().unwrap();
    Ok(())
}
