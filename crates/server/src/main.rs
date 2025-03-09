mod cache;
mod config;
mod ttl;

use anyhow::Result;
use anyhow::bail;
use mio::Token;
use mio::event::Event;
use mio::{Events, Interest, Poll};
use slab::Slab;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::Hash;
use std::mem;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::time::Duration;
use tracing_subscriber::filter::LevelFilter;
use transport::IOBUF_SIZE;
use transport::NeQuServer;
use transport::NeQuTransport;
use transport::SendStatus;

const LISTEN_ADDR: &str = "127.0.0.1:6479";
const SERVER: Token = Token(0);
const POLL_CAPACITY: usize = 128;
const POLL_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct ClientId(NonZeroUsize);

impl ClientId {
    fn from_slab_key(key: usize) -> Self {
        Self(NonZeroUsize::new(key + 1).unwrap())
    }

    fn to_slab_key(self) -> usize {
        self.0.get() - 1
    }
}

struct State {
    clients: Slab<NeQuTransport>,
    peers: HashMap<SocketAddr, ClientId>,
    write_queue: HashSet<SocketAddr>,
    timeouts: Vec<(std::time::Instant, SocketAddr)>,
    writable: bool,
}

impl State {
    fn new() -> Self {
        Self {
            clients: Slab::new(),
            peers: HashMap::new(),
            write_queue: HashSet::new(),
            timeouts: Vec::new(),
            writable: false,
        }
    }

    fn client_for_peer(&mut self, peer: SocketAddr) -> &mut NeQuTransport {
        let key = self.peers[&peer].to_slab_key();
        self.clients.get_mut(key).unwrap()
    }

    fn is_existing_connection(&mut self, peer: SocketAddr) -> bool {
        let key = self.peers.get(&peer).map(|id| id.to_slab_key());
        key.map(|key| self.clients.contains(key)).unwrap_or(false)
    }

    fn add_client(&mut self, transport: NeQuTransport) {
        let peer = transport.peer();
        let key = self.clients.insert(transport);
        let id = ClientId::from_slab_key(key);
        self.peers.insert(peer, id);
    }

    fn disconnect_client(&mut self, id: ClientId) -> Result<()> {
        let client = self.clients.get_mut(id.to_slab_key()).unwrap();
        let already_closed = client.close()?;
        if already_closed {
            self.cleanup_disconnect_client(id);
        }

        Ok(())
    }

    fn cleanup_disconnect_client(&mut self, id: ClientId) {
        println!("removing client: {:?}", id);
        let key = id.to_slab_key();
        let client = self.clients.remove(key);
        let peer = client.peer();
        self.peers.remove(&peer);
        self.write_queue.remove(&peer);
        self.timeouts.retain(|(_, p)| *p != peer);
    }

    fn socket_recv_available(&mut self, server: &mut NeQuServer) -> Result<HashSet<SocketAddr>> {
        let mut updated = HashSet::new();
        let mut read_buf = vec![0; IOBUF_SIZE];

        while let Some((n, peer)) = server.read_packets(&mut read_buf)? {
            if !self.is_existing_connection(peer) {
                let buf = mem::replace(&mut read_buf, vec![0; IOBUF_SIZE]);
                let transport = server.accept(buf, peer)?;
                self.add_client(transport);
            }

            let client = self.client_for_peer(peer);
            client.recv_packet(&read_buf[..n])?;
            updated.insert(peer);
        }

        Ok(updated)
    }

    fn ingest_buffers(&mut self, clients: HashSet<SocketAddr>) -> Result<()> {
        for addr in clients {
            let client = self.client_for_peer(addr);
            client.drive_recv()?;
            self.write_queue.insert(addr);
        }

        Ok(())
    }

    fn check_timeouts(&mut self) -> Result<()> {
        while let Some((timeout, peer)) = self.timeouts.first() {
            if *timeout > std::time::Instant::now() {
                break;
            }

            let (timeout, peer) = self.timeouts.remove(0);
            let client = self.client_for_peer(peer);
            client.on_timeout();
            self.write_queue.insert(peer);
        }

        Ok(())
    }

    fn send_queued(&mut self, server: &mut NeQuServer) -> Result<()> {
        loop {
            if !self.writable {
                break;
            }

            let peer = match self.write_queue.iter().next() {
                Some(peer) => *peer,
                None => return Ok(()),
            };

            let client = self.client_for_peer(peer);

            match client.drive_send(server.socket())? {
                SendStatus::Done => {
                    let timeout = client.next_timeout();
                    self.write_queue.remove(&peer);

                    if let Some(timeout) = timeout {
                        let idx = self
                            .timeouts
                            .binary_search_by(|(t, _)| t.cmp(&timeout))
                            .map_or_else(|x| x, |x| x);

                        self.timeouts.insert(idx, (timeout, peer));
                    }
                }

                SendStatus::NotDone => continue,
                SendStatus::WouldBlock => self.writable = false,
            }
        }

        Ok(())
    }

    fn handle_socket_event(&mut self, server: &mut NeQuServer, event: &Event) -> Result<()> {
        if event.is_writable() {
            self.writable = true;
        }

        if event.is_readable() {
            let updated = self.socket_recv_available(server)?;
            self.ingest_buffers(updated)?;
            self.send_queued(server)?;
        }

        Ok(())
    }

    fn handle_periodic(&mut self, server: &mut NeQuServer) -> Result<()> {
        let mut failed = vec![];
        let mut closed = vec![];

        for (key, client) in &mut self.clients {
            let id = ClientId::from_slab_key(key);

            if client.is_closed() {
                closed.push(id);
                continue;
            }

            if !client.keepalive()? {
                failed.push(id);
            }
        }

        for id in failed {
            self.disconnect_client(id)?;
        }

        for id in closed {
            self.cleanup_disconnect_client(id);
        }

        self.check_timeouts()?;
        self.send_queued(server)?;
        Ok(())
    }
}

fn run() -> Result<()> {
    let mut state = State::new();

    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(POLL_CAPACITY);

    let addr = LISTEN_ADDR.parse()?;
    let mut server = NeQuServer::bind(addr)?;

    poll.registry().register(
        server.socket_mut(),
        SERVER,
        Interest::READABLE | Interest::WRITABLE,
    )?;

    loop {
        if let Err(err) = poll.poll(&mut events, Some(POLL_TIMEOUT)) {
            if transport::interrupted(&err) {
                continue;
            }

            bail!(err);
        }

        for event in events.iter() {
            match event.token() {
                SERVER => state.handle_socket_event(&mut server, event)?,
                token => println!("observed sporadic event with token: {:?}", token),
            }
        }

        state.handle_periodic(&mut server)?;
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .init();

    if let Err(err) = run() {
        eprintln!("err: {}", err);
        eprintln!("backtrace: {}", err.backtrace());
    }
    Ok(())
}
