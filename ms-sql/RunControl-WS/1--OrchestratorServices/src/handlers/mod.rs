use super::*;
use crate::{
    db::{get_last_assigned_run_id, get_lock_status},
    trigger::initiate_run,
};
use actix_web::{
    post,
    web::{Data, Json, Path},
    Error, HttpResponse,
};
use serde::{Deserialize, Serialize};
use std::thread;

pub mod update;

#[derive(Serialize, Deserialize, Debug, RustcDecodable, RustcEncodable, Clone)]
pub struct TriggerInfo {
    pub run_id: i64,
    pub trigger: RunInfo,
}

#[derive(Serialize, Deserialize, Debug, RustcDecodable, RustcEncodable, Clone)]
pub struct RunInfo {
    pub as_on_date: String,
    pub batch_id: i64,
    pub stream_ids: Vec<i64>,
}

#[post("/trigger/{pool_id}")]
async fn trigger_run(
    path: Path<(i64,)>,
    info: Json<RunInfo>,
    pool: Data<Params>,
) -> Result<HttpResponse, Error> {
    let as_on_date = chrono::NaiveDate::parse_from_str(&info.as_on_date, "%Y-%m-%d")
        .expect("Cannot parse String to NaiveDate.");
    let trigger = TriggerInfo {
        run_id: get_last_assigned_run_id(path.0 .0, pool.clone()).unwrap_or(0) + 1,
        trigger: RunInfo {
            as_on_date: as_on_date.format("%d-%m-%Y").to_string(),
            batch_id: info.batch_id,
            stream_ids: info.stream_ids.clone(),
        },
    };
    let is_locked: String = get_lock_status(
        &trigger.trigger.batch_id,
        &as_on_date.and_hms(0, 0, 0),
        pool.clone(),
    );
    if is_locked == "N" {
        thread::spawn(move || {
            initiate_run(path.0 .0, trigger, pool);
        });
        Ok(HttpResponse::Ok()
            .content_type("text/plain")
            .body("Batch Initiated"))
    } else {
        Ok(HttpResponse::Ok()
            .content_type("text/plain")
            .body("Batch Locked"))
    }
}
