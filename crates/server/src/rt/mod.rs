use std::net::SocketAddr;
use std::time::Instant;
use anyhow::Result;
use std::collections::VecDeque;
use transport::{interrupted, would_block};

#[cfg(target_os = "linux")]
use nix::sys::socket;

#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;

pub struct Reactor {
    poll: mio::Poll,
    send_queue: Vec<Packet>,
}

pub struct Packet {
    pub buf: Vec<u8>,
    pub addr: SocketAddr,
    pub pace: Instant,
}

pub struct SendQueue {
    bufs: VecDeque<Vec<u8>>,
    addrs: VecDeque<SocketAddr>,

    #[cfg(target_os = "linux")]
    data: socket::MultiHeaders,
    // csmgs
    // flags
}


impl SendQueue {
    pub fn new() -> Self {
        Self {
            bufs: VecDeque::new(),
            addrs: VecDeque::new(),

            #[cfg(target_os = "linux")]
            data: nix::cmsg_space!([RawFd; 2]),
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn try_send(&mut self, socket:&mio::net::UdpSocket) -> Result<()> {
        while !self.bufs.is_empty() {
            let buf = &self.bufs[0];
            let addr = self.addrs[0];

            match socket.send_to(buf, addr) {
                Ok(n) if n == buf.len() => {
                    self.bufs.pop_front();
                    self.addrs.pop_front();
                }
                Ok(n) => panic!("short write: {} < {}", n, buf.len()),
                Err(ref err) if interrupted(err) => continue,
                Err(ref err) if would_block(err) => break,
                Err(err) => return Err(err.into()),
            }
        }

        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn try_send(&mut self, socket:&mio::net::UdpSocket) -> Result<()> {
        let fd = socket.as_raw_fd();

        let res = socket::sendmmsg(fd, &mut self.data, &self.bufs, &self.addrs, &[], socket::MsgFlags::empty());
    
        res.unwrap();
        Ok(())
    }
}
