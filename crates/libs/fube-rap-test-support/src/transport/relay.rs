//! Bounded clear-frame observer for production Hosting tests. It validates
//! framing and public furpc headers, but forwards the original bytes unchanged.
use super::{metadata, Metadata};
use furpc::{BoxError, Error, HeaderId, HeaderRecord, Headers};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const FIXED_HEADER_BYTES: usize = 12;
const MAX_OBSERVED_FRAME_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct Frame {
    bytes: Vec<u8>,
    pub headers: Headers,
    body_offset: usize,
}
impl Frame {
    pub async fn read(reader: &mut (impl AsyncRead + Unpin)) -> Result<Option<Self>, BoxError> {
        let mut fixed = [0; FIXED_HEADER_BYTES];
        match reader.read(&mut fixed[..1]).await {
            Ok(0) => return Ok(None),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => return Ok(None),
            Err(error) => return Err(error.into()),
        }
        reader.read_exact(&mut fixed[1..]).await?;
        let size = u32::from_le_bytes(fixed[..4].try_into()?) as usize;
        let body_offset =
            FIXED_HEADER_BYTES + usize::from(u16::from_le_bytes([fixed[6], fixed[7]]));
        if !(FIXED_HEADER_BYTES..=MAX_OBSERVED_FRAME_BYTES).contains(&size)
            || body_offset > size
            || fixed[4] != 0
        {
            return Err(Error::Protocol("invalid observed clear frame").into());
        }
        let mut checked = fixed;
        checked[5] = 0;
        checked[8..].fill(0);
        if fixed[5] != 0 && fixed[5] != crc8(&checked) {
            return Err(Error::Protocol("observed header CRC mismatch").into());
        }
        let mut bytes = vec![0; size];
        bytes[..FIXED_HEADER_BYTES].copy_from_slice(&fixed);
        reader.read_exact(&mut bytes[FIXED_HEADER_BYTES..]).await?;
        let checksum = u32::from_le_bytes(fixed[8..].try_into()?);
        if checksum != 0 && checksum != crc32(&bytes[FIXED_HEADER_BYTES..]) {
            return Err(Error::Protocol("observed body CRC mismatch").into());
        }
        let mut area = &bytes[FIXED_HEADER_BYTES..body_offset];
        let mut headers = Headers::new();
        while !area.is_empty() {
            let prefix = area
                .get(..4)
                .ok_or(Error::Protocol("truncated observed header"))?;
            let id = u16::from_le_bytes([prefix[0], prefix[1]]);
            let length = usize::from(u16::from_le_bytes([prefix[2], prefix[3]]));
            let value = area
                .get(4..4 + length)
                .filter(|value| !value.is_empty())
                .ok_or(Error::Protocol("invalid observed header length"))?;
            if id != 0x8000 {
                headers.append(HeaderRecord::from_encoded(
                    HeaderId::new(id)?,
                    value.to_vec(),
                )?)?;
            }
            area = &area[4 + length..];
        }
        Ok(Some(Self {
            bytes,
            headers,
            body_offset,
        }))
    }
    pub async fn forward(&self, writer: &mut (impl AsyncWrite + Unpin)) -> std::io::Result<()> {
        writer.write_all(&self.bytes).await
    }
    pub fn metadata(&self) -> furpc::Result<Metadata> {
        metadata(&self.headers)
    }
    pub fn body(&self) -> &[u8] {
        &self.bytes[self.body_offset..]
    }
}

pub async fn forward(
    reader: &mut (impl AsyncRead + Unpin),
    writer: &mut (impl AsyncWrite + Unpin),
    mut observe: impl FnMut(&Frame),
) -> Result<(), BoxError> {
    while let Some(frame) = Frame::read(reader).await? {
        observe(&frame);
        match frame.forward(writer).await {
            Ok(()) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
                ) =>
            {
                return Ok(())
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn crc8(bytes: &[u8]) -> u8 {
    let mut value = 0u8;
    for byte in bytes {
        value ^= byte;
        for _ in 0..8 {
            value = (value << 1) ^ if value & 0x80 != 0 { 0x07 } else { 0 };
        }
    }
    value
}
fn crc32(bytes: &[u8]) -> u32 {
    let mut value = u32::MAX;
    for byte in bytes {
        value ^= u32::from(*byte);
        for _ in 0..8 {
            value = (value >> 1) ^ if value & 1 != 0 { 0xedb8_8320 } else { 0 };
        }
    }
    !value
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        net::TcpListener,
        time::{timeout, Duration},
    };

    async fn furpc_hello() -> Frame {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint =
            furpc::Endpoint::from_shared(format!("tcp://{}", listener.local_addr().unwrap()))
                .unwrap();
        let connect = tokio::spawn(async move { endpoint.connect().await });
        let (mut socket, _) = listener.accept().await.unwrap();
        let frame = timeout(Duration::from_secs(3), Frame::read(&mut socket))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        connect.abort();
        assert!(connect.await.err().unwrap().is_cancelled());
        frame
    }

    #[tokio::test]
    async fn observer_forwards_actual_furpc_frame_bytes_unchanged() {
        let frame = furpc_hello().await;
        assert_eq!(frame.metadata().unwrap().actor, 1);
        let (mut writer, mut reader) = tokio::io::duplex(frame.bytes.len());
        frame.forward(&mut writer).await.unwrap();
        let mut forwarded = vec![0; frame.bytes.len()];
        reader.read_exact(&mut forwarded).await.unwrap();
        assert_eq!(forwarded, frame.bytes);
        let reread = Frame::read(&mut forwarded.as_slice())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reread.body(), frame.body());
        assert_eq!(reread.metadata().unwrap().id, frame.metadata().unwrap().id);
    }

    #[tokio::test]
    async fn observer_rejects_corrupt_checksums_and_truncated_frames() {
        let original = furpc_hello().await;
        let mut header = original.bytes.clone();
        header[5] = if header[5] == 1 { 2 } else { 1 };
        let error = Frame::read(&mut header.as_slice()).await.unwrap_err();
        assert!(matches!(
            error.downcast_ref::<Error>(),
            Some(Error::Protocol("observed header CRC mismatch"))
        ));

        let mut body = original.bytes.clone();
        body[5] = 0;
        let expected = crc32(&body[FIXED_HEADER_BYTES..]);
        body[8..12].copy_from_slice(&(if expected == 1 { 2u32 } else { 1u32 }).to_le_bytes());
        let error = Frame::read(&mut body.as_slice()).await.unwrap_err();
        assert!(matches!(
            error.downcast_ref::<Error>(),
            Some(Error::Protocol("observed body CRC mismatch"))
        ));

        let error = Frame::read(&mut &original.bytes[..3]).await.unwrap_err();
        assert_eq!(
            error.downcast_ref::<std::io::Error>().unwrap().kind(),
            std::io::ErrorKind::UnexpectedEof
        );
        assert!(Frame::read(&mut &[][..]).await.unwrap().is_none());
    }
}
