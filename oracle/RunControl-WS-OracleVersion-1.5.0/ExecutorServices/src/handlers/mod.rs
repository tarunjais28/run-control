use self::structs::{TriggerInfo, UpdateInfo};
use actix_web::web::Json;
use actix_web::{Error, HttpResponse, Result};
use curl::easy::{Easy, List};
use rustc_serialize::json;
use statics::DEF_SOC_ADDR;
use std::io;
use std::io::{stdout, Read, Write};
use std::process::Child;
use std::process::Command;
use std::thread;

mod structs;

pub fn execute(info: Json<TriggerInfo>) -> Result<HttpResponse, Error> {
    println!("Executing process on: {}\n {:#?}", info.executor_id, info);
    let net_socket_addr = unsafe { DEF_SOC_ADDR.to_string() };
    let execute_status = {
        Command::new(info.process_binary.as_str())
            .args(&info.process_args)
            .arg("--as-on-date".to_string())
            .arg(info.asondate.to_string())
            .spawn()
    };
    if execute_status.is_err() {
        let resp = UpdateInfo {
            run_id: info.run_id,
            as_on_date: info.asondate.clone(),
            batch_id: info.batch_id,
            stream_id: info.stream_id,
            flow_id: info.flow_id,
            executor_id: info.executor_id,
            process_id: info.process_id,
            process_status: "FAIL".to_string(),
            end_time: chrono::Utc::now().to_string(),
            process_report: info.process_report.to_string(),
        };
        let resp_body = json::encode(&resp).expect("Cannot serialize as JSON.");
        let _ = post_update_req(&net_socket_addr.to_string(), resp_body);
    } else {
        thread::spawn(move || {
            process_execution(&net_socket_addr.to_string(), info, execute_status);
        });
    }
    // Return process id to Orchestrator
    Ok(HttpResponse::Ok().content_type("plain/text").body(""))
}

fn post_update_req(conn_addr: &str, body: String) -> Result<(), String> {
    let mut data_to_upload = body.as_str().as_bytes();
    let mut handle = Easy::new();
    let conn_url: String = format!("http://{}/update/{}", conn_addr, 1);
    let mut list = List::new();
    list.append("Content-Type: application/json").unwrap();

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

fn process_execution(
    net_socket_addr: &str,
    info: Json<TriggerInfo>,
    execute_status: io::Result<Child>,
) {
    let mut child = execute_status.unwrap();
    let result = child.wait().expect("Failed to wait on child process.");
    if result.success() {
        let resp = UpdateInfo {
            run_id: info.run_id,
            as_on_date: info.asondate.clone(),
            batch_id: info.batch_id,
            stream_id: info.stream_id,
            flow_id: info.flow_id,
            executor_id: info.executor_id,
            process_id: info.process_id,
            process_status: "SUCCESS".to_string(),
            end_time: chrono::Utc::now().to_string(),
            process_report: info.process_report.to_string(),
        };
        let resp_body = json::encode(&resp).expect("Cannot serialize as JSON.");
        let _ = post_update_req(net_socket_addr, resp_body);
    } else {
        let resp = UpdateInfo {
            run_id: info.run_id,
            as_on_date: info.asondate.clone(),
            batch_id: info.batch_id,
            stream_id: info.stream_id,
            flow_id: info.flow_id,
            executor_id: info.executor_id,
            process_id: info.process_id,
            process_status: "FAIL".to_string(),
            end_time: chrono::Utc::now().to_string(),
            process_report: info.process_report.to_string(),
        };
        let resp_body = json::encode(&resp).expect("Cannot serialize as JSON.");
        let _ = post_update_req(net_socket_addr, resp_body);
    }
}
