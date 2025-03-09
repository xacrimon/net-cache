use anyhow::{Result, bail};
use mio::net::UdpSocket;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use mio::net::{TcpListener, TcpStream};

static SERVER_ID: quiche::ConnectionId = quiche::ConnectionId::from_ref(&[0; 16]);
static ALPN_LIST: &[&[u8]] = &[b"nequ/1.0"];

fn would_block(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock
}

fn would_block_anyhow(err: &anyhow::Error) -> bool {
    err.downcast_ref::<io::Error>().map_or(false, would_block)
}

fn interrupted(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::Interrupted
}

fn interrupted_anyhow(err: &anyhow::Error) -> bool {
    err.downcast_ref::<io::Error>().map_or(false, interrupted)
}

pub struct RedisTcpServer {
    listener: TcpListener,
}

pub struct RedisTcpTransport {
    stream: TcpStream,
}

pub struct NeQuServer {
    config: quiche::Config,
    socket: UdpSocket,
}

impl NeQuServer {
    pub fn bind(addr: SocketAddr) -> Result<Self> {
        let socket = UdpSocket::bind(addr)?;

        let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION)?;
        config.verify_peer(false);
        config.set_application_protos(ALPN_LIST)?;
        config
            .load_cert_chain_from_pem_file("certificate.pem")
            .unwrap();
        config.load_priv_key_from_pem_file("key.pem").unwrap();

        Ok(Self { socket, config })
    }

    pub fn event_source(&mut self) -> &mut dyn mio::event::Source {
        &mut self.socket
    }

    pub fn poll_read(&mut self, buf: &mut [u8]) -> Result<(usize, SocketAddr)> {
        let (len, addr) = self.socket.recv_from(buf)?;
        Ok((len, addr))
    }

    pub fn accept(&mut self, recv_buf: Vec<u8>, peer: SocketAddr) -> Result<NeQuTransport> {
        let local = self.socket.local_addr()?;
        let conn = quiche::accept(&SERVER_ID, None, local, peer, &mut self.config)?;

        Ok(NeQuTransport {
            conn,
            local,
            peer,
            recv_buf,
            send_buf: Vec::new(),
        })
    }
}

pub struct NeQuTransport {
    conn: quiche::Connection,
    local: SocketAddr,
    peer: SocketAddr,
    recv_buf: Vec<u8>,
    send_buf: Vec<u8>,
}

impl NeQuTransport {
    pub fn recv_packet(&mut self, buf: &[u8]) -> Result<()> {
        self.recv_buf.extend_from_slice(buf);

        if self.recv_buf.len() < 4096 {
            return Ok(());
        }

        self.drive_recv()?;
        Ok(())
    }

    pub fn drive_recv(&mut self) -> Result<()> {
        let recv_info = quiche::RecvInfo {
            from: self.peer,
            to: self.local,
        };

        if self.recv_buf.is_empty() {
            return Ok(());
        }

        let buf = &mut self.recv_buf[..];
        let read = match self.conn.recv(buf, recv_info) {
            Ok(n) => n,
            Err(err) => bail!(err),
        };

        self.recv_buf.drain(..read);
        Ok(())
    }

    // call when:
    // - after recv
    // - after on_timeout
    // - stream_send/stream_shutdown etc
    // - stream_recv etc
    pub fn try_drive_send(&mut self, server: &mut NeQuServer) -> Result<SendStatus> {
        if self.send_buf.is_empty() {
            self.send_buf.extend_from_slice(&[0; 4096]);

            let (write, info) = match self.conn.send(&mut self.send_buf) {
                Ok((n, info)) => (n, info),
                Err(quiche::Error::Done) => {
                    return Ok(SendStatus::Done {
                        timeout: self.conn.timeout(),
                    });
                }
                Err(err) => bail!(err),
            };

            let now = std::time::Instant::now();
            if now > info.at {
                println!(
                    "received pace {} ns in the past",
                    (now - info.at).as_nanos()
                );
            } else {
                println!(
                    "received pace {} ns into the future",
                    (info.at - now).as_nanos()
                );
            }

            self.send_buf.truncate(write);
        }

        let sent = loop {
            match server.socket.send_to(&self.send_buf[..], self.peer) {
                Ok(sent) => break sent,
                Err(ref err) if interrupted(err) => continue,
                Err(ref err) if would_block(err) => {
                    return Ok(SendStatus::WouldBlock);
                }
                Err(err) => bail!(err),
            }
        };

        self.send_buf.drain(..sent);

        Ok(SendStatus::NotDone)
    }

    pub fn on_timeout(&mut self) {
        self.conn.on_timeout();
    }

    pub fn is_established(&self) -> bool {
        self.conn.is_established()
    }
}

pub enum SendStatus {
    Done { timeout: Option<Duration> },
    NotDone,
    WouldBlock,
}
