use bytes::{BufMut, BytesMut};
use std::io::{Error, ErrorKind};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// MTProto Intermediate Transport implementation.
pub struct IntermediateTransport {
    stream: TcpStream,
}

impl IntermediateTransport {
    const PROTOCOL_TAG: [u8; 4] = [0xee, 0xee, 0xee, 0xee];
    const MAX_PACKET_SIZE: usize = 4 * 1024 * 1024;
    const HEADER_SIZE: usize = 4;

    pub async fn connect(addr: &str) -> Result<Self, Error> {
        let mut stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        stream.write_all(&Self::PROTOCOL_TAG).await?;
        stream.flush().await?;
        Ok(Self { stream })
    }

    pub async fn send_packet(&mut self, payload: &[u8]) -> Result<(), Error> {
        let len = payload.len() as u32;

        if payload.len() > Self::MAX_PACKET_SIZE {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Packet size {} exceeds maximum allowed size {}", payload.len(), Self::MAX_PACKET_SIZE),
            ));
        }

        if payload.len() <= 4096 {
            let mut buf = [0u8; 4096 + Self::HEADER_SIZE];
            buf[0..4].copy_from_slice(&len.to_le_bytes());
            buf[4..4 + payload.len()].copy_from_slice(payload);
            self.stream.write_all(&buf[..4 + payload.len()]).await?;
        } else {
            let mut frame = BytesMut::with_capacity(Self::HEADER_SIZE + payload.len());
            frame.put_u32_le(len);
            frame.put_slice(payload);
            self.stream.write_all(&frame).await?;
        }

        Ok(())
    }

    pub async fn read_packet(&mut self, buf: &mut Vec<u8>) -> Result<usize, Error> {
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf).await?;
        let len = u32::from_le_bytes(len_buf) as usize;

        if len == 0 {
            buf.clear();
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Zero-length MTProto packet received",
            ));
        }

        if len > Self::MAX_PACKET_SIZE {
            buf.clear();
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Packet size {} exceeds maximum allowed size {}", len, Self::MAX_PACKET_SIZE),
            ));
        }

        buf.resize(len, 0);
        self.stream.read_exact(&mut buf[..len]).await?;

        Ok(len)
    }

    pub fn stream(&self) -> &TcpStream {
        &self.stream
    }

    pub async fn flush(&mut self) -> Result<(), Error> {
        self.stream.flush().await
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
