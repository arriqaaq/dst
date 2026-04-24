use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::sync::mpsc;

use crate::error::Error;
use crate::net::addr::IntoSocketAddr;
use crate::sim::context::TickContext;

#[derive(Debug, Clone)]
pub struct InboundPacket {
    pub from: SocketAddr,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct UdpSocket {
    local_addr: SocketAddr,
    rx: Arc<Mutex<mpsc::Receiver<InboundPacket>>>,
}

impl UdpSocket {
    pub async fn bind<A: IntoSocketAddr>(addr: A) -> Result<Self, Error> {
        let addr = addr.into_socket_addr()?;
        TickContext::with(|ctx| {
            let node = ctx
                .active_node
                .ok_or(Error::Config("UdpSocket::bind called outside a node task"))?;

            let ip = if addr.ip().is_unspecified() {
                node.ip()
            } else {
                addr.ip()
            };
            let bound = SocketAddr::new(ip, addr.port());

            let cap = ctx.network.config.udp_capacity;
            let (tx, rx) = mpsc::channel(cap);
            ctx.network.register_socket(bound, tx)?;

            Ok(UdpSocket {
                local_addr: bound,
                rx: Arc::new(Mutex::new(rx)),
            })
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn send_to<A: IntoSocketAddr>(&self, buf: &[u8], target: A) -> Result<usize, Error> {
        let target = target.into_socket_addr()?;
        TickContext::with(|ctx| {
            ctx.network
                .enqueue_packet(self.local_addr, target, buf.to_vec(), ctx.elapsed);
            Ok(buf.len())
        })
    }

    pub async fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr), Error> {
        let mut rx = self.rx.lock().await;
        match rx.recv().await {
            Some(datagram) => {
                let len = datagram.payload.len().min(buf.len());
                buf[..len].copy_from_slice(&datagram.payload[..len]);
                Ok((len, datagram.from))
            }
            None => Err(Error::Io("inbound channel closed".into())),
        }
    }
}
