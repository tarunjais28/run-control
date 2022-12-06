use oracle::{Connection, Error};
use r2d2;

#[derive(Debug)]
pub struct OracleConnectionManager {
    connection_string: String,
    username: String,
    password: String
}

impl OracleConnectionManager {
    /// Creates a new `OracleConnectionManager`.
    pub fn new<S: Into<String>>(username: S, password: S, connection_string: S) -> OracleConnectionManager {
        OracleConnectionManager {
            connection_string: connection_string.into(),
            username: username.into(),
            password: password.into()
        }
    }
}

impl r2d2::ManageConnection for OracleConnectionManager {
    type Connection = Connection;
    type Error = Error;

    fn connect(&self) -> std::result::Result<Self::Connection, Self::Error> {
        return Connection::connect(&self.username, &self.password, &self.connection_string, &[]);
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> std::result::Result<(), Self::Error> {
        conn.ping()
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        self.is_valid(conn).is_err()
    }
}
