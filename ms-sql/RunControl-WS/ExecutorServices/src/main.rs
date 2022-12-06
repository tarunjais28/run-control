// #![feature(rustc_private)]
#![allow(non_snake_case)]

extern crate actix_web;
extern crate chrono;
extern crate clap;
extern crate curl;
extern crate rustc_serialize;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use crate::structs::Params;
use actix_web::{middleware, web, App, HttpServer};
use handlers::execute;
use macros::{LOG_PARAMS, PERF_PARAMS};
use std::io;
// use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};

mod handlers;
#[macro_use]
mod macros;
mod configuration_parameters;
mod log;
mod structs;

fn main() -> io::Result<()> {
    let app_name = "executor-services";
    let sys = actix_rt::System::new(app_name);
    let config_params = configuration_parameters::get_configuration_parameters(app_name);
    let (log, diag_log) = log::setup_loggers(
        config_params.log_file_path(),
        config_params.diagnostics_file_path(),
    );
    config_params.log_parameters(&log);

    log_info!(log, "Services started at `{}`", config_params.host_url());
    let pool: web::Data<Params> = web::Data::new(Params {
        orch_url: config_params.orch_url().to_string(),
        log,
        diag_log,
    });
    LOG_PARAMS.set_once_diagnostic_level(config_params.log_level().to_string());
    PERF_PARAMS.set_once_perf_diagnostics_enabled(config_params.is_perf_diagnostics_enabled());

    HttpServer::new(move || {
        App::new()
            .register_data(pool.clone())
            // enable logger
            .wrap(middleware::Logger::default())
            // register execute handler, handle execution of a process
            .service(web::resource("/execute").route(web::post().to(execute)))
    })
    .bind(config_params.host_url())
    .expect("Binding failed.")
    .start();

    sys.run()
}
