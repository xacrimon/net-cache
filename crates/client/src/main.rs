use transport::{IOBUF_SIZE, SendStatus};

use anyhow::Result;
use tracing_subscriber::filter::LevelFilter;
use transport::NeQuTransport;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .init();

    let local = "127.0.0.1:15981".parse()?;
    let peer = "127.0.0.1:6479".parse()?;
    let socket = std::net::UdpSocket::bind(local)?;
    socket.set_nonblocking(false)?;
    socket.set_read_timeout(Some(std::time::Duration::from_millis(100)))?;

    let socket = mio::net::UdpSocket::from_std(socket);
    let mut timeout = None;

    let scid = transport::connection_id_from_addrs(local, peer);
    let mut transport = NeQuTransport::connect(local, peer, &scid)?;

    loop {
        loop {
            let mut buf = vec![0; IOBUF_SIZE];

            let n = match socket.recv(&mut buf) {
                Ok(0) => panic!("socket closed"),
                Ok(n) => n,
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    break;
                }
                Err(e) => {
                    panic!("recv failed: {:?}", e);
                }
            };

            transport.recv_packet(&buf[..n])?;
        }

        transport.drive_recv()?;

        match transport.keepalive() {
            Ok(_) => {}
            Err(e) => {
                eprintln!("keepalive failed: {:?}", e);
                eprintln!("backtrace: {:?}", e.backtrace());
            }
        }

        if let Some(timeout) = timeout.take() {
            let now = std::time::Instant::now();
            if timeout <= now {
                transport.on_timeout();
            }
        }

        loop {
            match transport.drive_send(&socket)? {
                SendStatus::Done => {
                    timeout = transport.next_timeout();
                    break;
                }
                SendStatus::NotDone => {
                    continue;
                }
                SendStatus::WouldBlock => {
                    println!("send would block");
                    break;
                }
            }
        }
    }

    Ok(())
}
