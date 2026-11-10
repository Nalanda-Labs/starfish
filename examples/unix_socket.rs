//! A server on a Unix-domain socket: local, addressed by a path, and aware
//! of who is calling.
//!
//! A path can have only one listening socket, so there is no listener per
//! shard as with TCP. Every shard gets a handle to the same socket, and
//! each connection goes to one of the shards waiting for it.
//!
//! Run with `cargo run --example unix_socket`. It works on a socket file
//! in the system's temporary directory and removes it afterwards.

use starfish::fs::remove_file;
use starfish::io::{BufReader, BufWriter};
use starfish::net::{UnixListener, UnixStream};
use starfish::{shard_count, shard_id, spawn, submit_to, Runtime};

/// Answers each line with who sent it and which shard received it.
async fn serve(stream: UnixStream) -> starfish::Result<()> {
    let caller = stream.peer_credentials()?;
    let shard = shard_id()?;
    let mut lines = BufReader::new(&stream).limit(4096);
    let mut out = BufWriter::new(&stream);
    while let Some(line) = lines.next_line().await? {
        let reply = format!(
            "shard {shard} heard {line:?} from process {} of user {}\n",
            caller.process, caller.user
        );
        out.write_all(reply.as_bytes()).await?;
        out.flush().await?;
    }
    Ok(())
}

async fn app() -> starfish::Result<()> {
    let path = std::env::temp_dir().join(format!("starfish-example-{}.sock", std::process::id()));
    // A socket file left by a run that crashed would be in the way.
    let _ = remove_file(&path).await;
    let listener = UnixListener::bind(&path)?;
    println!("listening on {}", path.display());

    let mut acceptors = Vec::new();
    for shard in 0..shard_count()? {
        let listener = listener.try_clone()?;
        acceptors.push(spawn(submit_to(shard, move || async move {
            while let Ok(stream) = listener.accept().await {
                spawn(async move {
                    if let Err(err) = serve(stream).await {
                        eprintln!("connection failed: {err}");
                    }
                });
            }
        })));
    }

    for n in 1..=6 {
        let stream = UnixStream::connect(&path).await?;
        let (result, _) = stream.write_all(format!("message {n}\n")).await;
        result?;
        let mut reply = BufReader::new(&stream);
        println!("{}", reply.next_line().await?.unwrap_or_default());
    }

    // The file does not go away by itself.
    remove_file(&path).await?;
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(3).run(app)?
}
