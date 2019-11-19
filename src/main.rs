use std::os::unix::net::{UnixDatagram, UnixStream};
use std::time::Duration;

mod message;

struct DoryStreamSocket {
    socket: UnixStream,
    write_timeout: u64,
}

impl DoryStreamSocket {
    fn new(path: &str, write_timeout: u64) -> DoryStreamSocket {
        let new_socket = DoryStreamSocket {
            socket: UnixStream::connect(path).unwrap(),
            write_timeout,
        };
        new_socket
            .socket
            .set_write_timeout(Some(Duration::new(write_timeout, 0)));

        new_socket
    }
}

struct DoryDatagramSocket {
    socket: UnixDatagram,
    write_timeout: u64,
}

impl DoryDatagramSocket {
    fn new(path: &str, write_timeout: u64) -> DoryDatagramSocket {
        let new_socket = DoryDatagramSocket {
            socket: UnixDatagram::bind(path).unwrap(),
            write_timeout,
        };
        new_socket
            .socket
            .set_write_timeout(Some(Duration::new(write_timeout, 0)));

        new_socket
    }
}

fn main() {
    let x = message::DoryMessageAnyKey::new(b"topic", b"test", b"zorcz");
    let z = message::DoryMessageWithKey::new(b"topic", b"test", b"zorcz", 123);
    println!("{:#?}", x);
    println!("{:#?}", z);

    let enc = x.unwrap();
    let enc2 = z.unwrap();
    println!("{:?}", enc.serialize());
    println!("{:?}", enc2.serialize());
}
