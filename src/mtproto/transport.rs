use std::future::Future;
use std::io::{Error, ErrorKind};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// MTProto Intermediate Transport implementation.
///
/// Every network await is bounded: connecting by `CONNECT_TIMEOUT`, each read/write by
/// `IO_TIMEOUT` (a stalled peer fails the call instead of hanging the transfer forever).
pub struct IntermediateTransport {
    stream: TcpStream,
}

/// Maximum time to establish the TCP connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// Maximum time a single socket read or write may stay pending.
const IO_TIMEOUT: Duration = Duration::from_secs(120);

fn timed_out(what: &str) -> Error {
    Error::new(ErrorKind::TimedOut, format!("MTProto {what} timed out"))
}

async fn with_timeout<T>(what: &str, fut: impl Future<Output = Result<T, Error>>) -> Result<T, Error> {
    match tokio::time::timeout(IO_TIMEOUT, fut).await {
        Ok(r) => r,
        Err(_) => Err(timed_out(what)),
    }
}

impl IntermediateTransport {
    const PROTOCOL_TAG: [u8; 4] = [0xee, 0xee, 0xee, 0xee];
    const MAX_PACKET_SIZE: usize = 4 * 1024 * 1024;
    const HEADER_SIZE: usize = 4;

    pub async fn connect(addr: &str) -> Result<Self, Error> {
        let mut stream = match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr)).await {
            Ok(r) => r?,
            Err(_) => return Err(timed_out("connect")),
        };
        stream.set_nodelay(true)?;
        with_timeout("write", async {
            stream.write_all(&Self::PROTOCOL_TAG).await?;
            stream.flush().await
        })
        .await?;
        Ok(Self { stream })
    }

    /// Sends `payload` framed as `[len:u32 LE][payload]` (cold path: handshake).
    pub async fn send_packet(&mut self, payload: &[u8]) -> Result<(), Error> {
        if payload.len() > Self::MAX_PACKET_SIZE {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Packet size {} exceeds maximum allowed size {}", payload.len(), Self::MAX_PACKET_SIZE),
            ));
        }
        let mut frame = Vec::with_capacity(Self::HEADER_SIZE + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(payload);
        self.send_frame(&frame).await
    }

    /// Sends an already framed packet (`[len:u32 LE][payload]`, as built by
    /// `pack_encrypted_frame`) with a single write and no copy.
    pub async fn send_frame(&mut self, frame: &[u8]) -> Result<(), Error> {
        if frame.len() < Self::HEADER_SIZE || frame.len() - Self::HEADER_SIZE > Self::MAX_PACKET_SIZE {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Frame size {} is outside the allowed range", frame.len()),
            ));
        }
        with_timeout("write", self.stream.write_all(frame)).await
    }

    /// Reads one packet into `buf` (cleared first, capacity reused) and returns its length.
    pub async fn read_packet(&mut self, buf: &mut Vec<u8>) -> Result<usize, Error> {
        let mut len_buf = [0u8; 4];
        with_timeout("read", self.stream.read_exact(&mut len_buf)).await?;
        let len = u32::from_le_bytes(len_buf) as usize;

        buf.clear();
        if len == 0 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Zero-length MTProto packet received",
            ));
        }

        if len > Self::MAX_PACKET_SIZE {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Packet size {} exceeds maximum allowed size {}", len, Self::MAX_PACKET_SIZE),
            ));
        }

        // Read straight into spare capacity (no zero-fill of the buffer first).
        buf.reserve(len);
        let mut limited = (&mut self.stream).take(len as u64);
        while buf.len() < len {
            let n = with_timeout("read", limited.read_buf(buf)).await?;
            if n == 0 {
                return Err(Error::from(ErrorKind::UnexpectedEof));
            }
        }
        Ok(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_transport_handshake_and_framing() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        let server_handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();

            let mut tag = [0u8; 4];
            socket.read_exact(&mut tag).await.unwrap();
            assert_eq!(tag, [0xee, 0xee, 0xee, 0xee]);

            let mut len_buf = [0u8; 4];
            socket.read_exact(&mut len_buf).await.unwrap();
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            socket.read_exact(&mut buf).await.unwrap();

            socket.write_all(&(len as u32).to_le_bytes()).await.unwrap();
            socket.write_all(&buf).await.unwrap();
            socket.flush().await.unwrap();
        });

        let mut transport = IntermediateTransport::connect(&addr).await.unwrap();
        let msg = b"Hello MTProto";
        transport.send_packet(msg).await.unwrap();

        let mut read_buf = Vec::with_capacity(1024);
        let bytes_read = transport.read_packet(&mut read_buf).await.unwrap();

        assert_eq!(bytes_read, msg.len());
        assert_eq!(&read_buf[..bytes_read], msg);

        server_handle.await.unwrap();
    }
}
