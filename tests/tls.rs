//! TLS over the runtime's streams. Needs the `tls` feature:
//! `cargo test --features tls --test tls`.
#![cfg(feature = "tls")]

use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;

use starfish::io::BufReader;
use starfish::net::{TcpListener, TcpStream};
use starfish::tls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use starfish::tls::rustls::{ClientConfig, RootCertStore, ServerConfig};
use starfish::tls::{TlsAcceptor, TlsConnector, TlsStream};
use starfish::{spawn, submit_to, timeout, Runtime};

fn rt(shards: usize) -> Runtime {
    Runtime::new().shards(shards).pin_threads(false)
}

/// A certificate for `localhost` signed by itself, and its key.
fn identity() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let made = rcgen::generate_simple_self_signed(vec![String::from("localhost")]).unwrap();
    let key = PrivatePkcs8KeyDer::from(made.key_pair.serialize_der());
    (made.cert.der().clone(), PrivateKeyDer::Pkcs8(key))
}

/// A server with a fresh certificate, and a client that trusts exactly
/// that certificate. Both offer the made-up protocol `starfish-test`.
fn pair() -> (TlsAcceptor, TlsConnector) {
    let (certificate, key) = identity();
    let mut server = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate.clone()], key)
        .unwrap();
    server.alpn_protocols = vec![b"starfish-test".to_vec()];
    let mut roots = RootCertStore::empty();
    roots.add(certificate).unwrap();
    let mut client = ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
    client.alpn_protocols = vec![b"starfish-test".to_vec()];
    (TlsAcceptor::new(Arc::new(server)), TlsConnector::new(Arc::new(client)))
}

/// A connected pair of TCP streams on the loopback interface.
async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr().unwrap();
    let accepting = spawn(async move { listener.accept().await.unwrap() });
    let client = TcpStream::connect(addr).await.unwrap();
    (client, accepting.await.unwrap())
}

/// Both ends of a TLS connection, handshake done.
async fn tls_pair() -> (TlsStream<TcpStream>, TlsStream<TcpStream>) {
    let (acceptor, connector) = pair();
    let (client, server) = tcp_pair().await;
    let accepting = spawn(async move { acceptor.accept(server).await.unwrap() });
    let client = connector.connect("localhost", client).await.unwrap();
    (client, accepting.await.unwrap())
}

#[test]
fn data_goes_both_ways_and_the_end_is_announced() {
    rt(1)
        .run(|| async {
            let (mut client, mut server) = tls_pair().await;
            assert_eq!(client.alpn_protocol(), Some(&b"starfish-test"[..]));
            assert_eq!(server.alpn_protocol(), Some(&b"starfish-test"[..]));
            assert!(client.get_ref().peer_addr().is_ok());
            assert!(client.session().protocol_version().is_some());

            let echo = spawn(async move {
                let mut buf = vec![0u8; 16 * 1024];
                let mut total = 0;
                loop {
                    let read = server.read(&mut buf).await.unwrap();
                    if read == 0 {
                        break;
                    }
                    total += read;
                    server.write_all(&buf[..read]).await.unwrap();
                }
                server.shutdown().await.unwrap();
                total
            });

            // A megabyte in pieces larger than a TLS record, each sent and
            // then read back, since one task cannot do both at once.
            let piece: Vec<u8> = (0..100_000u32).map(|i| (i.wrapping_mul(2654435761) >> 24) as u8).collect();
            let mut back = vec![0u8; piece.len()];
            for _ in 0..10 {
                client.write_all(&piece).await.unwrap();
                client.read_exact(&mut back).await.unwrap();
                assert!(back == piece, "the echo differs from what was sent");
            }
            // An empty write and an empty read do nothing.
            client.write_all(&[]).await.unwrap();
            assert_eq!(client.read(&mut []).await.unwrap(), 0);

            client.shutdown().await.unwrap();
            assert_eq!(echo.await.unwrap(), 1_000_000);
            // The server said goodbye too: a proper end, and it stays one.
            assert_eq!(client.read(&mut back).await.unwrap(), 0);
            assert_eq!(client.read(&mut back).await.unwrap(), 0);
        })
        .unwrap();
}

#[test]
fn a_buffered_reader_works_on_top_and_shards_can_share_an_acceptor() {
    rt(2)
        .run(|| async {
            let (acceptor, connector) = pair();
            let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = listener.local_addr().unwrap();
            // The acceptor is used on another shard than it was made on.
            let serving = spawn(submit_to(1, move || async move {
                let connection = listener.accept().await.unwrap();
                let stream = acceptor.accept(connection).await.unwrap();
                let mut lines = BufReader::new(stream);
                let mut seen = Vec::new();
                while let Some(line) = lines.next_line().await.unwrap() {
                    seen.push(line);
                }
                seen
            }));
            let mut client = connector.connect("localhost", TcpStream::connect(addr).await.unwrap()).await.unwrap();
            client.write_all(b"first\nsec").await.unwrap();
            client.write_all(b"ond\nthird\n").await.unwrap();
            client.shutdown().await.unwrap();
            assert_eq!(serving.await.unwrap().unwrap(), ["first", "second", "third"]);
        })
        .unwrap();
}

#[test]
fn a_server_that_cannot_prove_who_it_is_is_refused() {
    rt(1)
        .run(|| async {
            // A client that trusts some other certificate.
            let (acceptor, _) = pair();
            let (_, stranger) = pair();
            let (client, server) = tcp_pair().await;
            let accepting = spawn(async move { acceptor.accept(server).await.map(|_| ()) });
            let refused = stranger.connect("localhost", client).await.err().unwrap();
            assert_eq!(refused.kind(), ErrorKind::InvalidData);
            // The server is told, not left waiting.
            assert!(accepting.await.unwrap().is_err());

            // The right certificate under the wrong name.
            let (acceptor, connector) = pair();
            let (client, server) = tcp_pair().await;
            let accepting = spawn(async move { acceptor.accept(server).await.map(|_| ()) });
            let refused = connector.connect("example.org", client).await.err().unwrap();
            assert_eq!(refused.kind(), ErrorKind::InvalidData);
            assert!(accepting.await.unwrap().is_err());

            // A name that is no name at all never reaches the network.
            let (client, _server) = tcp_pair().await;
            let refused = connector.connect("not a name", client).await.err().unwrap();
            assert_eq!(refused.kind(), ErrorKind::InvalidInput);
        })
        .unwrap();
}

#[test]
fn what_is_not_tls_is_an_error_and_so_is_a_connection_cut_short() {
    rt(1)
        .run(|| async {
            // Plain text where a handshake should be.
            let (acceptor, connector) = pair();
            let (client, server) = tcp_pair().await;
            let (result, _) = client.write_all("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
            result.unwrap();
            let refused = acceptor.accept(server).await.err().unwrap();
            assert_eq!(refused.kind(), ErrorKind::InvalidData);

            // A client that goes away in the middle of the handshake.
            let (client, server) = tcp_pair().await;
            drop(client);
            let refused = acceptor.accept(server).await.err().unwrap();
            assert_eq!(refused.kind(), ErrorKind::UnexpectedEof);

            // A connection closed without the TLS goodbye is not an end.
            let (client, server) = tcp_pair().await;
            let accepting = spawn(async move { acceptor.accept(server).await.unwrap() });
            let mut client = connector.connect("localhost", client).await.unwrap();
            let mut server = accepting.await.unwrap();
            server.write_all(b"half a mess").await.unwrap();
            drop(server);
            let mut buf = [0u8; 64];
            let read = client.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..read], b"half a mess");
            let cut = client.read(&mut buf).await.err().unwrap();
            assert_eq!(cut.kind(), ErrorKind::UnexpectedEof);
        })
        .unwrap();
}

#[test]
fn an_operation_given_up_part_way_ends_the_session() {
    rt(1)
        .run(|| async {
            let (mut client, mut server) = tls_pair().await;
            // Nothing is coming: the read is given up while it waits.
            let mut buf = [0u8; 16];
            assert!(timeout(Duration::from_millis(10), client.read(&mut buf)).await.is_err());
            let after = client.read(&mut buf).await.err().unwrap();
            assert!(after.to_string().contains("unusable"), "{after}");
            assert!(client.write_all(b"too late").await.is_err());
            assert!(client.shutdown().await.is_err());
            // The other end sees the connection go, not a goodbye.
            drop(client);
            assert_eq!(server.read(&mut buf).await.err().unwrap().kind(), ErrorKind::UnexpectedEof);
        })
        .unwrap();
}
