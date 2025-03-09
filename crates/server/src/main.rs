mod cache;
mod config;

use anyhow::Result;
use anyhow::bail;
use mio::unix::pipe;
use mio::Registry;
use mio::Token;
use mio::event::Event;
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll};
use proto::Request;
use std::cmp;
use std::collections::HashMap;
use std::hash::Hash;
use std::io;
use std::io::Read;
use std::io::Write;
use std::iter;
use std::net::SocketAddr;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::time::Duration;

const LISTEN_ADDR: &str = "127.0.0.1:6379";
const IO_SIZE: usize = 4096;
const SERVER: Token = Token(0);
const POLL_CAPACITY: usize = 128;
const POLL_TIMEOUT: Duration = Duration::from_millis(100);
const CLIENT_INTERESTS: LazyLock<Interest> =
    LazyLock::new(|| Interest::READABLE | Interest::WRITABLE);

fn allocate_id() -> usize {
    static NEXT_ID: Mutex<usize> = Mutex::new(1);

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

fn main() -> Result<()> {
    let mut clients = HashMap::new();

    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(POLL_CAPACITY);

    let addr = LISTEN_ADDR.parse()?;
    let mut server = TcpListener::bind(addr)?;

    poll.registry()
        .register(&mut server, SERVER, Interest::READABLE)?;

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

                    poll.registry()
                        .register(&mut client.stream, id.into(), *CLIENT_INTERESTS)?;

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

                token => println!("observed sporadic event with token: {:?}", token),
            }
        }
    }
}
