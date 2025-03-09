use std::net::UdpSocket;

use anyhow::Result;
use quiche::ConnectionId;
use tracing_subscriber::filter::LevelFilter;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .init();

    let addr = "127.0.0.1:15981".parse()?;
    let addr_server = "127.0.0.1:6479".parse()?;
    let socket = UdpSocket::bind(addr)?;
    socket.set_nonblocking(false)?;
    socket.set_read_timeout(Some(std::time::Duration::from_millis(1000)))?;
    socket.connect(addr_server)?;

    let scid = ConnectionId::from_ref(&[1; 16]);
    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION)?;
    config.verify_peer(false);
    config.set_application_protos(&[b"nequ/1.0"])?;
    config.set_max_idle_timeout(1000);

    let mut conn = quiche::connect(None, &scid, addr, addr_server, &mut config)?;

    let mut recv_buf = Vec::new();

    loop {
        loop {
            let mut buf = vec![0; 100000];
            match socket.recv(&mut buf) {
                Ok(0) => panic!("socket closed"),
                Ok(n) => recv_buf.extend_from_slice(&buf[..n]),
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    println!("recv would block");
                    break;
                }
                Err(e) => {
                    panic!("recv failed: {:?}", e);
                }
            };
        }

        if !recv_buf.is_empty() {
            let info = quiche::RecvInfo {
                from: addr_server,
                to: addr,
            };

            match conn.recv(&mut recv_buf[..], info) {
                Ok(read) => {
                    println!("Got {} bytes.", read);
                    recv_buf.drain(..read);
                }
                Err(e) => {
                    panic!("Error while reading: {:?}", e);
                }
            }
        }

        loop {
            let mut out = vec![0; 100000];

            match conn.send(&mut out) {
                Ok((write, _)) => {
                    out.truncate(write);
                    println!("Written {} bytes", write);
                }
                Err(quiche::Error::Done) => {
                    println!("Done writing");
                    break;
                }
                Err(err) => {
                    panic!("send failed: {:?}", err);
                }
            }

            if !out.is_empty() {
                dbg!(out.len());
                match socket.send(&out) {
                    Ok(sent) => {
                        out.drain(..sent);
                        println!("Sent {} bytes", sent);
                    }
                    Err(e) => {
                        panic!("send failed: {:?}", e);
                    }
                }
            }
        }
    }

    Ok(())
}
