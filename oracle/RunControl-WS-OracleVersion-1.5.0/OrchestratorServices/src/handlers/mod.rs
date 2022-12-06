use actix_web::web::Json;
use actix_web::web::Path;
use actix_web::{web, Error, HttpResponse};
use db::get_last_assigned_run_id;
use dbpool::OracleConnectionManager;
use r2d2::Pool;
use serde::{Deserialize, Serialize};
use std::thread;
use trigger::fire;

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

pub fn home(
    path: Path<(i64,)>,
    info: Json<RunInfo>,
    db: web::Data<Pool<OracleConnectionManager>>,
) -> Result<HttpResponse, Error> {
    let as_on_date = chrono::NaiveDate::parse_from_str(&info.as_on_date, "%Y-%m-%d")
        .expect("Cannot parse String to NaiveDate.");
    let trigger = TriggerInfo {
        run_id: get_last_assigned_run_id(db.clone()).unwrap_or(0) + 1,
        trigger: RunInfo {
            as_on_date: as_on_date.format("%d-%m-%Y").to_string(),
            batch_id: info.batch_id,
            stream_ids: info.stream_ids.clone(),
        },
    };
    thread::spawn(move || {
        fire(path.0, trigger, db);
    });
    Ok(HttpResponse::Ok()
        .content_type("text/plain")
        .body("Batch Initiated"))
}
