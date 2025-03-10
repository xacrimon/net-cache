use anyhow::{Result, bail};
use mio::net::UdpSocket;
use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tracing::{debug, trace};

pub const IOBUF_SIZE: usize = 65536;

const KEEPALIVE_INTERVAL: Duration = Duration::from_millis(1000);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_millis(5000);
const KEEPALIVE_STREAM: u64 = 0;
const KEEPALIVE_MESSAGE: &[u8] = b"0xdeadbeef";

const MAX_STREAMS: u64 = 1024;
const STREAM_BUFFER: u64 = 65536;
const CONNECTION_BUFFER: u64 = 4194304;

const CLIENT_STREAM_ID_START: u64 = 8;

static SERVER_ID: quiche::ConnectionId = quiche::ConnectionId::from_ref(&[0; 16]);
static ALPN: &[&[u8]] = &[b"nequ/1.0"];

#[repr(u64)]
enum Priority {
    Critical = 7,
    Default = 127,
}

pub fn connection_id_from_addrs(
    local: SocketAddr,
    peer: SocketAddr,
) -> quiche::ConnectionId<'static> {
    use std::hash::{Hash, Hasher};

    let mut hasher = fnv::FnvHasher::default();
    local.hash(&mut hasher);
    peer.hash(&mut hasher);
    let hash = hasher.finish();

    let id = Vec::from(hash.to_be_bytes());
    quiche::ConnectionId::from_vec(id)
}

fn quiche_server_config() -> Result<quiche::Config> {
    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION)?;

    config.load_cert_chain_from_pem_file("certificate.pem")?;
    config.load_priv_key_from_pem_file("key.pem")?;

    config.set_application_protos(ALPN)?;

    config.set_initial_max_streams_bidi(MAX_STREAMS);
    config.set_initial_max_streams_uni(MAX_STREAMS);
    config.set_initial_max_data(CONNECTION_BUFFER);
    config.set_initial_max_stream_data_bidi_local(STREAM_BUFFER);
    config.set_initial_max_stream_data_bidi_remote(STREAM_BUFFER);
    config.set_initial_max_stream_data_uni(STREAM_BUFFER);

    Ok(config)
}

fn quiche_client_config() -> Result<quiche::Config> {
    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION)?;

    config.set_application_protos(ALPN)?;

    config.set_initial_max_streams_bidi(MAX_STREAMS);
    config.set_initial_max_streams_uni(MAX_STREAMS);
    config.set_initial_max_data(CONNECTION_BUFFER);
    config.set_initial_max_stream_data_bidi_local(STREAM_BUFFER);
    config.set_initial_max_stream_data_bidi_remote(STREAM_BUFFER);
    config.set_initial_max_stream_data_uni(STREAM_BUFFER);

    Ok(config)
}

pub fn would_block(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock
}

pub fn would_block_anyhow(err: &anyhow::Error) -> bool {
    err.downcast_ref::<io::Error>().map_or(false, would_block)
}

pub fn interrupted(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::Interrupted
}

pub fn interrupted_anyhow(err: &anyhow::Error) -> bool {
    err.downcast_ref::<io::Error>().map_or(false, interrupted)
}

pub struct NeQuListener {
    config: quiche::Config,
    socket: UdpSocket,
}

impl NeQuListener {
    pub fn bind(addr: SocketAddr) -> Result<Self> {
        let socket = UdpSocket::bind(addr)?;
        let config = quiche_server_config()?;
        Ok(Self { socket, config })
    }

    pub fn read_packets(&mut self, buf: &mut [u8]) -> Result<Option<(usize, SocketAddr)>> {
        match self.socket.recv_from(buf) {
            Ok((n, addr)) => Ok(Some((n, addr))),
            Err(ref err) if interrupted(err) => return self.read_packets(buf),
            Err(ref err) if would_block(err) => Ok(None),
            Err(err) => bail!(err),
        }
    }

    pub fn accept(&mut self, recv_buf: Vec<u8>, peer: SocketAddr) -> Result<NeQuTransport> {
        let local = self.socket.local_addr()?;
        let conn = quiche::accept(&SERVER_ID, None, local, peer, &mut self.config)?;

        Ok(NeQuTransport {
            conn,
            local,
            peer,
            recv_buf,
            last_sent_keepalive: Instant::now(),
            last_received_keepalive: Instant::now(),
            next_stream_id: CLIENT_STREAM_ID_START,
            is_server: true,
        })
    }

    pub fn socket(&self) -> &UdpSocket {
        &self.socket
    }

    pub fn socket_mut(&mut self) -> &mut UdpSocket {
        &mut self.socket
    }
}

pub struct NeQuTransport {
    conn: quiche::Connection,
    local: SocketAddr,
    peer: SocketAddr,
    recv_buf: Vec<u8>,
    last_sent_keepalive: Instant,
    last_received_keepalive: Instant,
    next_stream_id: u64,
    is_server: bool,
}

impl NeQuTransport {
    pub fn connect(
        local: SocketAddr,
        peer: SocketAddr,
        scid: &quiche::ConnectionId,
    ) -> Result<Self> {
        let mut config = quiche_client_config()?;
        let conn = quiche::connect(None, &scid, local, peer, &mut config)?;

        Ok(Self {
            conn,
            local,
            peer,
            recv_buf: Vec::new(),
            last_sent_keepalive: Instant::now(),
            last_received_keepalive: Instant::now(),
            next_stream_id: CLIENT_STREAM_ID_START,
            is_server: false,
        })
    }

    pub fn new_stream(&mut self, priority: Priority) -> Result<u64> {
        let stream_id = self.next_stream_id;
        self.next_stream_id += 2;
        Ok(stream_id)
    }

    pub fn close_stream(&mut self, stream_id: u64) -> Result<()> {
        self.conn
            .stream_shutdown(stream_id, quiche::Shutdown::Write, 0)?;
        Ok(())
    }

    pub fn stream_send(&mut self, stream_id: u64, buf: &[u8]) -> Result<usize> {
        let n = self.conn.stream_send(stream_id, buf, false)?;
        Ok(n)
    }

    pub fn stream_recv(&mut self, stream_id: u64, buf: &mut [u8]) -> Result<usize> {
        let n = match self.conn.stream_recv(stream_id, buf) {
            Ok((n, _)) => n,
            Err(quiche::Error::Done) => 0,
            Err(err) => bail!(err),
        };

        Ok(n)
    }

    pub fn readable_streams(&self) -> impl Iterator<Item = u64> {
        self.conn.readable()
    }

    pub fn writable_streams(&self) -> impl Iterator<Item = u64> {
        self.conn.writable()
    }

    pub fn recv_packet(&mut self, buf: &[u8]) -> Result<()> {
        self.recv_buf.extend_from_slice(buf);
        if self.recv_buf.len() < IOBUF_SIZE {
            return Ok(());
        }

        self.drive_recv()?;
        Ok(())
    }

    pub fn drive_recv(&mut self) -> Result<()> {
        if self.recv_buf.is_empty() {
            return Ok(());
        }

        let recv_info = quiche::RecvInfo {
            from: self.peer,
            to: self.local,
        };

        let buf = &mut self.recv_buf[..];
        let established = self.conn.is_established();
        let read = match self.conn.recv(buf, recv_info) {
            Ok(n) => n,
            Err(err) => bail!(err),
        };

        if self.conn.is_established() != established {
            debug!("connection established");
            self.configure()?;
        }

        self.recv_buf.drain(..read);
        Ok(())
    }

    // call when:
    // - after recv
    // - after on_timeout
    // - stream_send/stream_shutdown etc
    // - stream_recv etc
    pub fn drive_send(&mut self) -> Result<Option<(Vec<u8>, quiche::SendInfo)>> {
        let max_size = self.conn.max_send_udp_payload_size();
        let mut buf = vec![0; max_size];

        let (n, info) = match self.conn.send(&mut buf) {
            Ok((n, info)) => (n, info),
            Err(quiche::Error::Done) => {
                return Ok(None);
            }
            Err(err) => bail!(err),
        };

        buf.truncate(n);

        let now = std::time::Instant::now();
        if now > info.at {
            trace!(
                "received pace {} us in the past",
                (now - info.at).as_micros()
            );
        } else {
            trace!(
                "received pace {} us into the future",
                (info.at - now).as_micros()
            );
        }
        debug!("serialized packet with {} bytes", n);

        Ok(Some((buf, info)))
    }

    pub fn next_timeout(&self) -> Option<Instant> {
        self.conn.timeout_instant()
    }

    pub fn on_timeout(&mut self) {
        self.conn.on_timeout();
    }

    pub fn peer(&self) -> SocketAddr {
        self.peer
    }

    fn configure(&mut self) -> Result<()> {
        if !self.is_server {
            self.conn
                .stream_priority(KEEPALIVE_STREAM, Priority::Critical as _, false)?;
        }

        Ok(())
    }

    fn recv_keepalive(&mut self) -> Result<bool> {
        if self.conn.stream_readable(KEEPALIVE_STREAM) {
            let buf = &mut [0; KEEPALIVE_MESSAGE.len()];
            let (_, fin) = self.conn.stream_recv(KEEPALIVE_STREAM, buf)?;
            assert!(!fin);
            assert_eq!(&buf[..], KEEPALIVE_MESSAGE);
            self.last_received_keepalive = Instant::now();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn send_keepalive(&mut self) -> Result<()> {
        let n = self
            .conn
            .stream_send(KEEPALIVE_STREAM, KEEPALIVE_MESSAGE, false)?;

        assert_eq!(n, KEEPALIVE_MESSAGE.len());
        self.last_sent_keepalive = Instant::now();
        Ok(())
    }

    pub fn keepalive(&mut self) -> Result<bool> {
        if !self.conn.is_established() {
            return Ok(true);
        }

        // if server and not responding to keepalivee
        if self.is_server {
            if !self.recv_keepalive()? {
                return Ok(self.last_received_keepalive.elapsed() < KEEPALIVE_TIMEOUT);
            } else {
                debug!("server sending keepalive response");
            }
        }

        // if client and interval not reached, don't send keepalive
        if !self.is_server {
            if self.last_sent_keepalive.elapsed() < KEEPALIVE_INTERVAL {
                return Ok(true);
            } else {
                debug!("client sending keepalive request");
            }
        }

        self.send_keepalive()?;

        if !self.is_server {
            while self.recv_keepalive()? {
                debug!("client received keepalive response");
            }
        }

        Ok(true)
    }

    pub fn is_closed(&self) -> bool {
        self.conn.is_closed()
    }

    pub fn close(&mut self) -> Result<bool> {
        match self.conn.close(false, 1, b"") {
            Ok(_) => Ok(false),
            Err(quiche::Error::Done) => Ok(true),
            Err(err) => bail!(err),
        }
    }
}

pub enum SendStatus {
    Done,
    NotDone,
    WouldBlock,
}
