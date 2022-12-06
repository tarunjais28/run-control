//! Oracle support for the `r2d2` connection pool.
pub extern crate oracle;
pub extern crate r2d2;

pub mod pool;
pub use pool::OracleConnectionManager;

