mod cache;
mod config;
mod server;
mod ttl;
mod rt;

use anyhow::Result;
use anyhow::bail;
use mio::{Events, Interest, Poll};
use server::Server;
use std::time::Duration;
use tracing_subscriber::filter::LevelFilter;
use transport::NeQuListener;

const LISTEN_ADDR: &str = "127.0.0.1:6479";
const SERVER: mio::Token = mio::Token(0);
const POLL_CAPACITY: usize = 128;
const POLL_TIMEOUT: Duration = Duration::from_millis(100);

struct State {
    server: Server,
}

impl State {
    fn new() -> Self {
        Self {
            server: Server::new(),
        }
    }
}

fn run() -> Result<()> {
    let mut state = State::new();

    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(POLL_CAPACITY);

    let addr = LISTEN_ADDR.parse()?;
    let mut listener = NeQuListener::bind(addr)?;

    poll.registry().register(
        listener.socket_mut(),
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
                SERVER => state.server.handle_socket_event(&mut listener, event)?,
                token => println!("observed sporadic event with token: {:?}", token),
            }
        }

        state.server.update_periodic(&mut listener)?;
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
