use anyhow::{Result, bail};
use slab::Slab;
use std::collections::HashMap;
use std::collections::HashSet;
use std::mem;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::time::Instant;
use transport::{IOBUF_SIZE, NeQuListener, NeQuTransport, interrupted, would_block};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct ClientId(NonZeroUsize);

impl ClientId {
    fn from_slab_key(key: usize) -> Self {
        Self(NonZeroUsize::new(key + 1).unwrap())
    }

    fn to_slab_key(self) -> usize {
        self.0.get() - 1
    }
}

pub struct Packet {
    pub buf: Vec<u8>,
    pub addr: SocketAddr,
    pub pace: Instant,
}

pub struct Server {
    clients: Slab<NeQuTransport>,
    peers: HashMap<SocketAddr, ClientId>,
    send_queue: Vec<Packet>,
    timeouts: Vec<(std::time::Instant, SocketAddr)>,
}

impl Server {
    pub fn new() -> Self {
        Self {
            clients: Slab::new(),
            peers: HashMap::new(),
            send_queue: Vec::new(),
            timeouts: Vec::new(),
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
        self.send_queue.retain(|p| p.addr != peer);
        self.timeouts.retain(|(_, p)| *p != peer);
    }

    fn socket_recv_available(
        &mut self,
        listener: &mut NeQuListener,
    ) -> Result<HashSet<SocketAddr>> {
        let mut updated = HashSet::new();
        let mut read_buf = vec![0; IOBUF_SIZE];

        while let Some((n, peer)) = listener.read_packets(&mut read_buf)? {
            if !self.is_existing_connection(peer) {
                let buf = mem::replace(&mut read_buf, vec![0; IOBUF_SIZE]);
                let transport = listener.accept(buf, peer)?;
                self.add_client(transport);
            }

            let client = self.client_for_peer(peer);
            client.recv_packet(&read_buf[..n])?;
            updated.insert(peer);
        }

        Ok(updated)
    }

    fn packet_batch(id: ClientId, client: &mut NeQuTransport) -> Result<Vec<Packet>> {
        let mut batch = Vec::new();
        while let Some((buf, info)) = client.drive_send()? {
            let packet = Packet {
                buf,
                addr: info.to,
                pace: info.at,
            };
            batch.push(packet);
        }

        Ok(batch)
    }

    fn drive_clients(&mut self, clients: HashSet<SocketAddr>) -> Result<()> {
        for addr in clients {
            let id = self.peers[&addr];
            let client = self.client_for_peer(addr);
            client.drive_recv()?;

            let batch = Self::packet_batch(id, client)?;

            let timeout = client.next_timeout();
            if let Some(timeout) = timeout {
                let idx = self
                    .timeouts
                    .binary_search_by(|(t, _)| t.cmp(&timeout))
                    .map_or_else(|x| x, |x| x);

                self.timeouts.insert(idx, (timeout, addr));
            }

            self.send_queue.extend(batch);
        }

        Ok(())
    }

    fn check_timeouts(&mut self) -> Result<()> {
        while let Some((timeout, peer)) = self.timeouts.first() {
            if *timeout > std::time::Instant::now() {
                break;
            }

            let (timeout, peer) = self.timeouts.remove(0);
            let id = self.peers[&peer];
            let client = self.client_for_peer(peer);
            client.on_timeout();

            let batch = Self::packet_batch(id, client)?;
            self.send_queue.extend(batch);
        }

        Ok(())
    }

    fn send_queued(&mut self, server: &mut NeQuListener) -> Result<()> {
        while self.send_queue.len() > 0 {
            let packet = &self.send_queue[0];

            match server.socket().send_to(&packet.buf, packet.addr) {
                Ok(n) if n == packet.buf.len() => {
                    self.send_queue.remove(0);
                }
                Ok(n) => panic!("short write: {} < {}", n, packet.buf.len()),
                Err(ref err) if interrupted(err) => continue,
                Err(ref err) if would_block(err) => break,
                Err(err) => bail!(err),
            }
        }

        Ok(())
    }

    pub fn handle_socket_event(
        &mut self,
        server: &mut NeQuListener,
        event: &mio::event::Event,
    ) -> Result<()> {
        if event.is_writable() {
            self.send_queued(server)?;
        }

        if event.is_readable() {
            let updated = self.socket_recv_available(server)?;
            self.drive_clients(updated)?;
        }

        Ok(())
    }

    pub fn update_periodic(&mut self, server: &mut NeQuListener) -> Result<()> {
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

        let peers = self.peers.keys().copied().collect();
        self.drive_clients(peers)?;

        self.send_queued(server)?;
        Ok(())
    }
}
