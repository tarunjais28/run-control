#![feature(rustc_private)]
#![allow(non_snake_case)]

extern crate actix_rt;
extern crate actix_web;
extern crate chrono;
extern crate curl;
extern crate dbpool;
extern crate env_logger;
extern crate log;
extern crate openssl;
extern crate oracle;
extern crate r2d2;
extern crate rustc_serialize;
extern crate serde;
extern crate serde_json;

use actix_web::{middleware, web, App, HttpServer};
use db::{
    get_batch_full_status, get_batch_last_success_ason, get_batch_lastest_ason,
    get_batch_status_by_id, get_batch_status_latest, get_batches, get_lastest_ason, get_streams,
};
use handlers::home;
use handlers::update::{update_batch_status_holiday, update_status};
use std::env;
use std::io;

mod db;
mod handlers;
mod trigger;

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let host_url: &str = &args[1];
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();
    let sys = actix_rt::System::new("orchestrator-services");

    let manager = dbpool::OracleConnectionManager::new(
        args[2].to_string(),
        args[3].to_string(),
        args[4].to_string(),
    );
    let pool = r2d2::Pool::builder().max_size(15).build(manager).unwrap();

    HttpServer::new(move || {
        App::new()
            .data(pool.clone())
            // enable logger
            .wrap(middleware::Logger::default())
            // register home handler, handle trigger run method
            .service(web::resource("/trigger/{pool_id}").route(web::post().to_async(home)))
            // register run control batches handler, handle run control get_batches method
            .service(web::resource("/batches/{pool_id}").route(web::get().to_async(get_batches)))
            // register run control stream handler, handle run control get_streams method
            .service(web::resource("/streams/{pool_id}").route(web::post().to_async(get_streams)))
            // register run control status handler, handle run control get_batch_status method
            .service(
                web::resource("/batch_status/{pool_id}")
                    .route(web::post().to_async(get_batch_status_by_id)),
            )
            // register run control status handler, handle run control get_batch_status_latest method
            .service(
                web::resource("/last_batch_status/{pool_id}")
                    .route(web::get().to_async(get_batch_status_latest)),
            )
            // register run control status handler, handle run control get_batch_status_latest method
            .service(
                web::resource("/batch_full_status/{pool_id}")
                    .route(web::post().to_async(get_batch_full_status)),
            )
            // register run control lastest_ason handler, handle run control lastest_ason method
            .service(
                web::resource("/latest_ason/{pool_id}")
                    .route(web::get().to_async(get_lastest_ason)),
            )
            // register run control batch_lastest_ason handler, handle run control batch_lastest_ason method
            .service(
                web::resource("/batch_latest_ason/{pool_id}")
                    .route(web::post().to_async(get_batch_lastest_ason)),
            )
            // register run control batch_last_success_ason, handle run control batch_last_success_ason
            .service(
                web::resource("/batch_last_success_ason/{pool_id}")
                    .route(web::post().to_async(get_batch_last_success_ason)),
            )
            // register run control update handler, handle run control update status method
            .service(web::resource("/update/{pool_id}").route(web::post().to(update_status)))
            .service(
                web::resource("/update_batch_status_holiday/{pool_id}")
                    .route(web::post().to(update_batch_status_holiday)),
            )
    })
    .bind(host_url)
    .expect("Binding failed.")
    .start();
    sys.run()
}
