//! Public bounded clear-frame I/O and deadline helpers.
//!
//! The former parallel Connection object is retired. Operational socket
//! ownership, correlation and cancellation live in [`crate::native_channel`].
//! Free I/O helpers remain used by peer transports, test hosts and fixtures;
//! callers, not these functions, own progress watchdogs and cancellation safety.

use std::time::Duration;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    time::Instant,
};

use crate::{
    frame::{validate_fixed, Frame, FIXED_HEADER_SIZE},
    message::Message,
    Error, Result,
};

/// Independent watchdog for a started frame, not an idle-session timeout.
pub const MAX_FRAME_TIME: Duration = Duration::from_secs(30);

pub fn deadline_after(timeout: Duration) -> Result<Instant> {
    let ticks = timeout.as_nanos() / 100;
    if ticks == 0 || ticks > i64::MAX as u128 {
        return Err(Error::InvalidInput(
            "timeout must be positive, at least 100ns, and fit native TimeSpan",
        ));
    }
    Instant::now()
        .checked_add(timeout)
        .ok_or(Error::InvalidInput("timeout exceeds monotonic clock range"))
}

/// Read one complete bounded clear frame. Canceling a partial read requires
/// discarding the reader; callers must preserve frame progress or close it.
pub async fn read_message<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Message> {
    let mut fixed = [0; FIXED_HEADER_SIZE];
    reader.read_exact(&mut fixed).await?;
    let (size, _) = validate_fixed(&fixed)?;
    let mut bytes = vec![0; size];
    bytes[..FIXED_HEADER_SIZE].copy_from_slice(&fixed);
    reader.read_exact(&mut bytes[FIXED_HEADER_SIZE..]).await?;
    Message::from_frame(Frame::decode(&bytes)?)
}

/// Write a complete frame. Canceling a partial write requires closing or
/// discarding the stream rather than transmitting after an incomplete suffix.
pub async fn write_message<W: AsyncWrite + Unpin>(writer: &mut W, message: &Message) -> Result<()> {
    writer.write_all(&message.to_frame()?.encode()?).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{Header, MessageId};

    #[test]
    fn deadline_configuration_is_explicit_and_bounded() {
        assert!(deadline_after(Duration::ZERO).is_err());
        assert!(deadline_after(Duration::from_nanos(99)).is_err());
        assert!(deadline_after(Duration::MAX).is_err());
        assert!(deadline_after(Duration::from_secs(30)).is_ok());
    }

    #[tokio::test]
    async fn framing_every_split_and_coalescing() {
        let message = Message {
            headers: vec![
                Header::actor(7).unwrap(),
                Header::message_id(MessageId {
                    guid: [1; 16],
                    index: 42,
                })
                .unwrap(),
            ],
            body: vec![1, 2, 3, 4],
        };
        let bytes = message.to_frame().unwrap().encode().unwrap();
        for split in 0..=bytes.len() {
            let (mut writer, mut reader) = tokio::io::duplex(8192);
            let send = async {
                writer.write_all(&bytes[..split]).await.unwrap();
                tokio::task::yield_now().await;
                writer.write_all(&bytes[split..]).await.unwrap();
                writer.write_all(&bytes).await.unwrap();
            };
            let receive = async {
                assert_eq!(read_message(&mut reader).await.unwrap(), message);
                assert_eq!(read_message(&mut reader).await.unwrap(), message);
            };
            tokio::join!(send, receive);
        }
    }

    #[tokio::test]
    async fn invalid_fixed_rejects_without_body() {
        let (mut writer, mut reader) = tokio::io::duplex(64);
        let mut fixed = [0; FIXED_HEADER_SIZE];
        fixed[..4].copy_from_slice(&12u32.to_le_bytes());
        fixed[4] = 1;
        writer.write_all(&fixed).await.unwrap();
        assert!(matches!(
            read_message(&mut reader).await,
            Err(Error::Protocol(_))
        ));
    }
}
