use std::net::{IpAddr, SocketAddr};

use crate::error::Error;

pub trait IntoSocketAddr {
    fn into_socket_addr(self) -> Result<SocketAddr, Error>;
}

impl IntoSocketAddr for SocketAddr {
    fn into_socket_addr(self) -> Result<SocketAddr, Error> {
        Ok(self)
    }
}

impl IntoSocketAddr for &SocketAddr {
    fn into_socket_addr(self) -> Result<SocketAddr, Error> {
        Ok(*self)
    }
}

impl IntoSocketAddr for (IpAddr, u16) {
    fn into_socket_addr(self) -> Result<SocketAddr, Error> {
        Ok(SocketAddr::new(self.0, self.1))
    }
}

impl IntoSocketAddr for &str {
    fn into_socket_addr(self) -> Result<SocketAddr, Error> {
        self.parse::<SocketAddr>()
            .map_err(|e| Error::Io(format!("invalid socket addr {self:?}: {e}")))
    }
}

impl IntoSocketAddr for String {
    fn into_socket_addr(self) -> Result<SocketAddr, Error> {
        self.as_str().into_socket_addr()
    }
}

impl IntoSocketAddr for &String {
    fn into_socket_addr(self) -> Result<SocketAddr, Error> {
        self.as_str().into_socket_addr()
    }
}
