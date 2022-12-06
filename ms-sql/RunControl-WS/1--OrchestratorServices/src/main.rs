#![allow(non_snake_case)]

extern crate actix_web;
extern crate chrono;
extern crate curl;
extern crate odbc;
extern crate rustc_serialize;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

// Use this crate and set environmet variable RUST_LOG=odbc to see ODBC warnings
// Driver={ODBC Driver 17 for SQL Server};Server={TESTDB\SQLSERVER2017};Database={RunControl};UID={RunCtrl};PWD={OuPatqQbMaRH7Fu};
// Driver={ODBC Driver 17 for SQL Server};Server={CSBDUAMLDB};Database={BASEL4_UAT};UID={BASEL4USR};PWD={BASEL4@1234};
// Driver={ODBC Driver 17 for SQL Server};Server=10.81.4.48;Database=runcontrol;UID=dbuser;PWD=db@123;
// Driver={ODBC Driver 17 for SQL Server};Server=10.81.4.51;Database=runcontrol;UID=rcuser;PWD=db@123;
// Driver={ODBC Driver 17 for SQL Server};Server=10.0.7.70;Database=RunControl;UID=RCUser;PWD=RCu@HDFCl;
// Driver={ODBC Driver 11 for SQL Server};Server=192.168.66.13;Database=Runcontrol_NBS;UID=runcontrol;PWD=Surya@123;

use crate::structs::Params;
use actix_web::{middleware, web, App, HttpServer};
use db::{
    get_batch_full_status, get_batch_last_success_ason, get_batch_lastest_ason,
    get_batch_status_by_id, get_batch_status_latest, get_batches, get_lastest_ason, get_streams,
};
use handlers::trigger_run;
use handlers::update::{update_batch_status_holiday, update_status};
use macros::{LOG_PARAMS, PERF_PARAMS};

mod db;
mod handlers;
mod trigger;
#[macro_use]
mod macros;
mod configuration_parameters;
mod log;
mod structs;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let app_name = "orchestrator-services";
    let config_params = configuration_parameters::get_configuration_parameters(app_name);
    let (log, diag_log) = log::setup_loggers(
        config_params.log_file_path(),
        config_params.diagnostics_file_path(),
    );
    config_params.log_parameters(&log);

    log_info!(log, "Services started at `{}`", config_params.host_url());
    let pool: web::Data<Params> = web::Data::new(Params {
        con_str: config_params.con_str().to_string(),
        log,
        diag_log,
    });
    LOG_PARAMS.set_once_diagnostic_level(config_params.log_level().to_string());
    PERF_PARAMS.set_once_perf_diagnostics_enabled(config_params.is_perf_diagnostics_enabled());

    HttpServer::new(move || {
        App::new()
            .app_data(pool.clone())
            // enable logger
            .wrap(middleware::Logger::default())
            // register trigger handler, handle trigger run method
            .service(trigger_run)
            // register run control batches handler, handle run control get_batches method
            .service(get_batches)
            // register run control stream handler, handle run control get_streams method
            .service(get_streams)
            // register run control status handler, handle run control get_batch_status method
            .service(get_batch_status_by_id)
            // register run control status handler, handle run control get_batch_status_latest method
            .service(get_batch_status_latest)
            // register run control status handler, handle run control get_batch_status_latest method
            .service(get_batch_full_status)
            // register run control lastest_ason handler, handle run control lastest_ason method
            .service(get_lastest_ason)
            // register run control batch_lastest_ason handler, handle run control batch_lastest_ason method
            .service(get_batch_lastest_ason)
            // register run control batch_last_success_ason, handle run control batch_last_success_ason
            .service(get_batch_last_success_ason)
            // register run control update handler, handle run control update status method
            .service(update_status)
            // register run control update holiday api
            .service(update_batch_status_holiday)
    })
    .bind(config_params.host_url())
    .expect("Binding failed.")
    .run()
    .await
}
