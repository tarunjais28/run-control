#![feature(rustc_private)]
#![allow(non_snake_case)]

extern crate actix_web;
extern crate chrono;
extern crate curl;
extern crate dbpool;
extern crate env_logger;
extern crate log;
// extern crate openssl;
extern crate oracle;
extern crate r2d2;
extern crate rustc_serialize;
extern crate serde;
extern crate serde_json;

use self::statics::get_socket_address;
use actix_web::{middleware, web, App, HttpServer};
use handlers::execute;
use std::env;
use std::io;
// use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};

mod handlers;
mod statics;

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let host_url: &str = &args[1];
    get_socket_address(args[2].to_string());
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();
    let sys = actix_rt::System::new("executor-services");

    // // load ssl keys
    // let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
    // builder
    //     .set_private_key_file("domain.key", SslFiletype::PEM)
    //     .unwrap();
    // builder.set_certificate_chain_file("domain.crt").unwrap();

    HttpServer::new(move || {
        App::new()
            // enable logger
            .wrap(middleware::Logger::default())
            // register execute handler, handle execution of a process
            .service(web::resource("/execute").route(web::post().to(execute)))
    })
    .bind(host_url)
    .expect("Binding failed.")
    .start();
    sys.run()
}
