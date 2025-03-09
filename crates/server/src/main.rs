mod cache;
mod config;
mod ttl;

use anyhow::Result;
use anyhow::bail;
use mio::Registry;
use mio::Token;
use mio::event::Event;
use mio::net::{TcpListener, TcpStream};
use mio::unix::pipe;
use mio::{Events, Interest, Poll};
use proto::Request;
use std::cmp;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::Hash;
use std::io;
use std::io::Read;
use std::io::Write;
use std::iter;
use std::net::SocketAddr;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::time::Duration;
use tracing_subscriber::filter::LevelFilter;
use transport::NeQuServer;
use transport::NeQuTransport;
use transport::SendStatus;

const LISTEN_ADDR: &str = "127.0.0.1:6379";
const LISTEN_ADDR_2: &str = "127.0.0.1:6479";
const IO_SIZE: usize = 4096;
const SERVER: Token = Token(0);
const SERVER_2: Token = Token(1);
const POLL_CAPACITY: usize = 128;
const POLL_TIMEOUT: Duration = Duration::from_millis(50);

fn allocate_id() -> usize {
    static NEXT_ID: Mutex<usize> = Mutex::new(2);

    let mut next = NEXT_ID.lock().unwrap();
    let id = *next;
    *next += 1;
    id
}

struct Pipe {
    tx: Mutex<pipe::Sender>,
    rx: Mutex<pipe::Receiver>,
}

static PIPE: LazyLock<Pipe> = LazyLock::new(|| {
    let (tx, rx) = mio::unix::pipe::new().unwrap();
    Pipe {
        tx: Mutex::new(tx),
        rx: Mutex::new(rx),
    }
});

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct ClientId(usize);

impl ClientId {
    fn new() -> Self {
        let id = allocate_id();
        Self(id)
    }
}

impl From<Token> for ClientId {
    fn from(token: Token) -> Self {
        Self(token.0)
    }
}

impl Into<mio::Token> for ClientId {
    fn into(self) -> mio::Token {
        Token(self.0)
    }
}

struct Client {
    stream: TcpStream,
    addr: SocketAddr,
    decoder: proto::RequestDecoder,
    to_send: Vec<u8>,
}

impl Client {
    fn new(stream: TcpStream, addr: SocketAddr) -> Self {
        Self {
            stream,
            addr,
            decoder: proto::RequestDecoder::new(),
            to_send: Vec::new(),
        }
    }

    fn read(&mut self) -> Result<usize> {
        let buf = self.decoder.read_at(IO_SIZE);
        let n = self.stream.read(buf)?;
        if n == 0 {
            return Ok(0);
        }

        self.decoder.advance(n);
        Ok(n)
    }

    fn write_buffered(&mut self) -> Result<(), io::Error> {
        let window_len = cmp::min(self.to_send.len(), IO_SIZE);
        let window = &self.to_send[0..window_len];

        let written = self.stream.write(window)?;
        self.to_send.drain(0..written);
        Ok(())
    }

    fn requests(&mut self) -> impl Iterator<Item = Result<Request>> + '_ {
        iter::from_fn(move || self.decoder.try_decode().transpose())
    }
}

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

fn handle_connection_event(
    registry: &Registry,
    client: &mut Client,
    event: &Event,
) -> Result<bool> {
    if event.is_writable() {
        match client.write_buffered() {
            Ok(()) => {}
            Err(ref err) if would_block(err) => {}
            Err(ref err) if interrupted(err) => {
                return handle_connection_event(registry, client, event);
            }
            Err(err) => bail!(err),
        }
    }

    if event.is_readable() {
        let mut connection_closed = false;

        loop {
            match client.read() {
                Ok(0) => {
                    connection_closed = true;
                    break;
                }

                Ok(_) => continue,
                Err(ref err) if would_block_anyhow(err) => break,
                Err(ref err) if interrupted_anyhow(err) => continue,
                Err(err) => bail!(err),
            }
        }

        if connection_closed {
            println!("closed connection with addr: {}", client.addr);
            return Ok(true);
        }
    }

    Ok(false)
}

struct State {
    clients: HashMap<SocketAddr, NeQuTransport>,
    write_queue: Vec<SocketAddr>,
    timeouts: Vec<(std::time::Instant, SocketAddr)>,
    writable: bool,
}

impl State {
    fn new() -> Self {
        Self {
            clients: HashMap::new(),
            write_queue: Vec::new(),
            timeouts: Vec::new(),
            writable: false,
        }
    }
}

fn handle_socket_event(server: &mut NeQuServer, state: &mut State, event: &Event) -> Result<()> {
    let mut dirty = HashSet::new();

    if event.is_writable() {
        state.writable = true;
    }

    if event.is_readable() {
        let mut buf = vec![0; IO_SIZE];

        loop {
            match server.poll_read(&mut buf) {
                Ok((n, peer)) => {
                    if !state.clients.contains_key(&peer) {
                        let buf = buf.clone();
                        let client = server.accept(buf, peer)?;
                        state.clients.insert(peer, client);
                    }

                    let client = state.clients.get_mut(&peer).unwrap();
                    client.recv_packet(&buf[..n])?;
                    dirty.insert(peer);
                }
                Err(ref err) if would_block_anyhow(err) => break,
                Err(ref err) if interrupted_anyhow(err) => continue,
                Err(err) => bail!(err),
            }
        }
    }

    for peer in dirty {
        let client = state.clients.get_mut(&peer).unwrap();
        client.drive_recv()?;

        if !state.write_queue.contains(&peer) {
            state.write_queue.push(peer);
        }
    }

    while let Some((timeout, peer)) = state.timeouts.first() {
        if *timeout > std::time::Instant::now() {
            break;
        }

        let (timeout, peer) = state.timeouts.remove(0);
        let client = state.clients.get_mut(&peer).unwrap();
        client.on_timeout();

        if !state.write_queue.contains(&peer) {
            state.write_queue.push(peer);
        }
    }

    while state.writable {
        let peer = match state.write_queue.first() {
            Some(peer) => peer,
            None => break,
        };

        let client = state.clients.get_mut(&peer).unwrap();

        match client.try_drive_send(server)? {
            SendStatus::Done { timeout } => {
                let peer = state.write_queue.remove(0);

                if let Some(timeout) = timeout {
                    let timeout = std::time::Instant::now() + timeout;
                    state.timeouts.push((timeout, peer));
                    state.timeouts.sort_by_key(|(timeout, _)| *timeout);
                }
            }

            SendStatus::NotDone => {
                continue;
            }

            SendStatus::WouldBlock => {
                state.writable = false;
            }
        }
    }

    if state.clients.values().any(|client| client.is_established()) {
        panic!("we did it!");
    }

    Ok(())
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .init();

    if let Err(err) = server() {
        eprintln!("error: {}", err.backtrace());
    }
    Ok(())
}

fn server() -> Result<()> {
    let mut clients = HashMap::new();
    let mut state = State::new();

    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(POLL_CAPACITY);

    let addr = LISTEN_ADDR.parse()?;
    let mut server = TcpListener::bind(addr)?;

    let addr_2 = LISTEN_ADDR_2.parse()?;
    let mut server_2 = NeQuServer::bind(addr_2)?;

    poll.registry()
        .register(&mut server, SERVER, Interest::READABLE)?;

    poll.registry().register(
        server_2.event_source(),
        SERVER_2,
        Interest::READABLE | Interest::WRITABLE,
    )?;

    loop {
        if let Err(err) = poll.poll(&mut events, Some(POLL_TIMEOUT)) {
            if interrupted(&err) {
                continue;
            }

            bail!(err);
        }

        'events: for event in events.iter() {
            match event.token() {
                SERVER => {
                    let (conn, addr) = match server.accept() {
                        Ok((conn, addr)) => (conn, addr),
                        Err(err) if would_block(&err) => continue 'events,
                        Err(err) => bail!(err),
                    };

                    conn.set_nodelay(true)?;

                    let id = ClientId::new();
                    let mut client = Client::new(conn, addr);

                    poll.registry().register(
                        &mut client.stream,
                        id.into(),
                        Interest::READABLE | Interest::WRITABLE,
                    )?;

                    clients.insert(id, client);

                    println!("accepted connection with addr: {}", addr);
                }

                token if clients.contains_key(&token.into()) => {
                    let registry = poll.registry();
                    let id = ClientId::from(token);
                    let client = clients.get_mut(&id).unwrap();
                    let done = handle_connection_event(registry, client, event)?;

                    for _request in client.requests() {
                        todo!("request handling");
                    }

                    if done {
                        let mut client = clients.remove(&id).unwrap();
                        registry.deregister(&mut client.stream)?;
                    }
                }

                SERVER_2 => handle_socket_event(&mut server_2, &mut state, event)?,

                token => println!("observed sporadic event with token: {:?}", token),
            }
        }
    }

    Ok(())
}
