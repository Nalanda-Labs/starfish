//! TLS: an echo server and a client for it, in one program.
//!
//! The server makes itself a certificate for `localhost` when it starts,
//! and the client is told to trust exactly that certificate. A real server
//! would load a certificate issued for its name, and a real client would
//! trust the usual authorities; only the two configurations at the top
//! would differ.
//!
//! Run with `cargo run --features tls --example tls_echo`. With `serve` as
//! an argument the server stays up on port 8443 for other clients to try:
//!
//! ```text
//! cargo run --features tls --example tls_echo -- serve
//! openssl s_client -connect localhost:8443 -servername localhost
//! ```

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use starfish::io::{BufReader, BufWriter};
use starfish::net::{TcpListener, TcpStream};
use starfish::tls::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use starfish::tls::rustls::{ClientConfig, RootCertStore, ServerConfig};
use starfish::tls::{TlsAcceptor, TlsConnector};
use starfish::{spawn, timeout, Runtime};

type AnyError = Box<dyn std::error::Error>;

/// The server's configuration and one for a client that trusts it.
fn configurations() -> Result<(TlsAcceptor, TlsConnector), AnyError> {
    let made = rcgen::generate_simple_self_signed(vec![String::from("localhost")])?;
    let certificate = made.cert.der().clone();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(made.key_pair.serialize_der()));

    let server = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate.clone()], key)?;

    let mut trusted = RootCertStore::empty();
    trusted.add(certificate)?;
    let client = ClientConfig::builder()
        .with_root_certificates(trusted)
        .with_no_client_auth();

    Ok((
        TlsAcceptor::new(Arc::new(server)),
        TlsConnector::new(Arc::new(client)),
    ))
}

/// Serves one connection: every line comes back in capitals.
async fn serve(acceptor: TlsAcceptor, connection: TcpStream) -> std::io::Result<()> {
    // A client may not stall the handshake for ever.
    let Ok(stream) = timeout(Duration::from_secs(5), acceptor.accept(connection)).await else {
        return Ok(());
    };
    // One buffered reader over the stream; replies go out through it.
    let mut lines = BufReader::new(stream?);
    while let Some(line) = lines.next_line().await? {
        let reply = format!("{}\n", line.to_uppercase());
        lines.get_mut().write_all(reply.as_bytes()).await?;
    }
    // The client said goodbye; say it back.
    lines.get_mut().shutdown().await
}

async fn app(stay_up: bool) -> Result<(), AnyError> {
    let (acceptor, connector) = configurations()?;
    let port = if stay_up { 8443 } else { 0 };
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], port)))?;
    let addr = listener.local_addr()?;
    let server = spawn(async move {
        while let Ok(connection) = listener.accept().await {
            let acceptor = acceptor.clone();
            spawn(async move {
                if let Err(err) = serve(acceptor, connection).await {
                    eprintln!("connection ended badly: {err}");
                }
            });
        }
    });
    if stay_up {
        println!("serving TLS on {addr}; stop with Ctrl-C");
        starfish::signal::termination().await?;
        return Ok(());
    }

    let connection = TcpStream::connect(addr).await?;
    let stream = connector.connect("localhost", connection).await?;
    println!(
        "connected with {:?}",
        stream.session().protocol_version().ok_or("no version agreed")?
    );
    // Small writes are collected into one TLS record by the writer.
    let mut out = BufWriter::new(stream);
    for line in ["hello", "over an encrypted connection"] {
        out.write_all(line.as_bytes()).await?;
        out.write_all(b"\n").await?;
    }
    out.flush().await?;
    let mut stream = out.into_inner();
    stream.shutdown().await?;

    let mut reply = Vec::new();
    let mut buf = [0u8; 256];
    loop {
        match stream.read(&mut buf).await? {
            0 => break,
            read => reply.extend_from_slice(&buf[..read]),
        }
    }
    print!("{}", String::from_utf8_lossy(&reply));
    server.abort();
    Ok(())
}

fn main() {
    let stay_up = std::env::args().nth(1).as_deref() == Some("serve");
    let outcome = Runtime::new()
        .shards(1)
        .run(move || async move { app(stay_up).await.map_err(|err| err.to_string()) });
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(err)) => eprintln!("failed: {err}"),
        Err(err) => eprintln!("the runtime failed: {err}"),
    }
}
