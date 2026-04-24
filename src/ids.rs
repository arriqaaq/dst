use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeName(pub String);

impl NodeName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

impl fmt::Display for NodeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<&str> for NodeName {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl From<String> for NodeName {
    fn from(s: String) -> Self {
        Self(s)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeAddr(pub SocketAddr);

impl NodeAddr {
    pub fn new(ip: IpAddr, port: u16) -> Self {
        Self(SocketAddr::new(ip, port))
    }

    pub fn ip(&self) -> IpAddr {
        self.0.ip()
    }

    pub fn port(&self) -> u16 {
        self.0.port()
    }
}

impl fmt::Display for NodeAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug)]
pub struct AddrPool {
    next: u32,
}

impl AddrPool {
    pub fn new() -> Self {
        Self { next: 1 }
    }

    pub fn allocate(&mut self) -> IpAddr {
        let octets = [
            192,
            168,
            ((self.next >> 8) & 0xFF) as u8,
            (self.next & 0xFF) as u8,
        ];
        self.next += 1;
        IpAddr::V4(Ipv4Addr::from(octets))
    }
}

impl Default for AddrPool {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequential_allocation() {
        let mut alloc = AddrPool::new();
        let a = alloc.allocate();
        let b = alloc.allocate();
        assert_eq!(a, IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1)));
        assert_eq!(b, IpAddr::V4(Ipv4Addr::new(192, 168, 0, 2)));
    }

    #[test]
    fn node_name_ordering() {
        let a = NodeName::new("alpha");
        let b = NodeName::new("beta");
        assert!(a < b);
    }
}
