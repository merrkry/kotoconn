//! Session frames: a 7-byte header (command, big-endian stream ID, big-endian
//! payload length) followed by the payload.
use bytes::{BufMut, Bytes, BytesMut};
use std::{
    io,
    num::NonZeroU32,
    pin::Pin,
    task::{Context, Poll, ready},
};
use tokio::io::{AsyncRead, ReadBuf};
use tokio_util::io::poll_read_buf;

pub(crate) const HEADER: usize = 7;

pub(crate) type Sid = NonZeroU32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Command {
    Waste = 0,
    Syn = 1,
    Push = 2,
    Fin = 3,
    Settings = 4,
    Alert = 5,
    UpdatePaddingScheme = 6,
    SynAck = 7,
    HeartRequest = 8,
    HeartResponse = 9,
    ServerSettings = 10,
}

/// A received frame whose stream ID and payload fit its command. Commands
/// sent only in the other direction are still decoded; the session ignores them.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Frame {
    Waste,
    Syn(Sid),
    Push(Sid, Bytes),
    Fin(Sid),
    Settings(Bytes),
    Alert(Bytes),
    UpdatePaddingScheme(Bytes),
    /// An empty payload acknowledges the stream; otherwise it carries the error.
    SynAck(Sid, Bytes),
    HeartRequest,
    HeartResponse,
    ServerSettings(Bytes),
    /// Later protocol versions may add commands; peers ignore unknown ones.
    Unknown(u8),
}

impl Frame {
    fn parts(&self) -> (Command, u32, &[u8]) {
        match self {
            Self::Waste => (Command::Waste, 0, &[]),
            Self::Syn(sid) => (Command::Syn, sid.get(), &[]),
            Self::Push(sid, data) => (Command::Push, sid.get(), data),
            Self::Fin(sid) => (Command::Fin, sid.get(), &[]),
            Self::Settings(data) => (Command::Settings, 0, data),
            Self::Alert(data) => (Command::Alert, 0, data),
            Self::UpdatePaddingScheme(data) => (Command::UpdatePaddingScheme, 0, data),
            Self::SynAck(sid, data) => (Command::SynAck, sid.get(), data),
            Self::HeartRequest => (Command::HeartRequest, 0, &[]),
            Self::HeartResponse => (Command::HeartResponse, 0, &[]),
            Self::ServerSettings(data) => (Command::ServerSettings, 0, data),
            // SAFETY: Unknown frames are only decoded, never constructed for sending.
            Self::Unknown(_) => unreachable!("unknown frames are never sent"),
        }
    }

    pub fn encode(&self, out: &mut BytesMut) {
        let (command, sid, data) = self.parts();
        out.reserve(HEADER + data.len());
        out.put_slice(&header(command, sid, data.len()));
        out.put_slice(data);
    }
}

/// Every caller bounds its payload: data chunks by the stream chunk size,
/// padding schemes at parse time, and control messages by construction.
pub(crate) fn header(command: Command, sid: u32, length: usize) -> [u8; HEADER] {
    // SAFETY: See the payload bounds above; a larger payload is a caller bug.
    let length = u16::try_from(length).expect("AnyTLS frame payload exceeds 65535 bytes");
    let mut header = [0; HEADER];
    header[0] = command as u8;
    header[1..5].copy_from_slice(&sid.to_be_bytes());
    header[5..].copy_from_slice(&length.to_be_bytes());
    header
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// Reads each payload straight into its own uninitialized buffer, as the
/// reference reads it: one plaintext copy, nothing to zero-fill, and a
/// retained frame pins only its own bytes, so receive credit stays an
/// accurate memory bound. Progress survives a cancelled `next`.
pub(crate) struct FrameReader<R> {
    io: R,
    header: [u8; HEADER],
    header_read: usize,
    payload: Option<BytesMut>,
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    pub fn new(io: R) -> Self {
        Self {
            io,
            header: [0; HEADER],
            header_read: 0,
            payload: None,
        }
    }

    /// `None` marks a clean end of the transport between frames.
    pub async fn next(&mut self) -> io::Result<Option<Frame>> {
        std::future::poll_fn(|cx| self.poll_next(cx)).await
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Option<Frame>>> {
        while self.header_read < HEADER {
            let mut out = ReadBuf::new(&mut self.header[self.header_read..]);
            ready!(Pin::new(&mut self.io).poll_read(cx, &mut out))?;
            let read = out.filled().len();
            if read == 0 && self.header_read == 0 {
                return Poll::Ready(Ok(None));
            }
            if read == 0 {
                return Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()));
            }
            self.header_read += read;
        }

        let length = usize::from(u16::from_be_bytes([self.header[5], self.header[6]]));
        let payload = if length == 0 {
            Bytes::new()
        } else {
            let payload = self
                .payload
                .get_or_insert_with(|| BytesMut::with_capacity(length));
            while payload.len() < length {
                let mut unread = payload.limit(length - payload.len());
                let read = ready!(poll_read_buf(Pin::new(&mut self.io), cx, &mut unread))?;
                if read == 0 {
                    return Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()));
                }
            }
            // SAFETY: get_or_insert_with above installed the payload buffer.
            self.payload.take().expect("payload buffer").freeze()
        };

        self.header_read = 0;
        let sid = u32::from_be_bytes([
            self.header[1],
            self.header[2],
            self.header[3],
            self.header[4],
        ]);
        Poll::Ready(decode(self.header[0], sid, payload).map(Some))
    }
}

fn decode(command: u8, sid: u32, payload: Bytes) -> io::Result<Frame> {
    const WASTE: u8 = Command::Waste as u8;
    const SYN: u8 = Command::Syn as u8;
    const PUSH: u8 = Command::Push as u8;
    const FIN: u8 = Command::Fin as u8;
    const SETTINGS: u8 = Command::Settings as u8;
    const ALERT: u8 = Command::Alert as u8;
    const UPDATE: u8 = Command::UpdatePaddingScheme as u8;
    const SYN_ACK: u8 = Command::SynAck as u8;
    const HEART_REQUEST: u8 = Command::HeartRequest as u8;
    const HEART_RESPONSE: u8 = Command::HeartResponse as u8;
    const SERVER_SETTINGS: u8 = Command::ServerSettings as u8;

    let stream = || NonZeroU32::new(sid).ok_or_else(|| invalid("stream frame uses stream ID 0"));
    let control = || {
        if sid == 0 {
            Ok(())
        } else {
            Err(invalid("control frame uses a stream ID"))
        }
    };
    let empty = || {
        if payload.is_empty() {
            Ok(())
        } else {
            Err(invalid("frame cannot carry a payload"))
        }
    };
    let required = |payload: Bytes| {
        if payload.is_empty() {
            Err(invalid("frame requires a payload"))
        } else {
            Ok(payload)
        }
    };

    Ok(match command {
        WASTE => Frame::Waste,
        SYN => {
            empty()?;
            Frame::Syn(stream()?)
        }
        PUSH => Frame::Push(stream()?, payload),
        FIN => {
            empty()?;
            Frame::Fin(stream()?)
        }
        SETTINGS => {
            control()?;
            Frame::Settings(required(payload)?)
        }
        ALERT => {
            control()?;
            Frame::Alert(payload)
        }
        UPDATE => {
            control()?;
            Frame::UpdatePaddingScheme(required(payload)?)
        }
        SYN_ACK => Frame::SynAck(stream()?, payload),
        HEART_REQUEST => {
            control()?;
            empty()?;
            Frame::HeartRequest
        }
        HEART_RESPONSE => {
            control()?;
            empty()?;
            Frame::HeartResponse
        }
        SERVER_SETTINGS => {
            control()?;
            Frame::ServerSettings(required(payload)?)
        }
        other => Frame::Unknown(other),
    })
}

/// Settings payloads are UTF-8 `key=value` lines. Lines without `=` carry no setting.
pub(crate) fn settings(payload: &[u8]) -> impl Iterator<Item = (&str, &str)> {
    std::str::from_utf8(payload)
        .unwrap_or_default()
        .split('\n')
        .filter_map(|line| line.split_once('='))
}

pub(crate) fn setting<'a>(payload: &'a [u8], key: &str) -> Option<&'a str> {
    // Later duplicates win, as in the reference map decoding.
    settings(payload)
        .filter(|(name, _)| *name == key)
        .map(|(_, value)| value)
        .last()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    async fn decode_all(wire: &[u8]) -> io::Result<Vec<Frame>> {
        let mut reader = FrameReader::new(wire);
        let mut decoded = Vec::new();
        while let Some(frame) = reader.next().await? {
            decoded.push(frame);
        }
        Ok(decoded)
    }

    #[tokio::test]
    async fn decodes_big_endian_headers_across_partial_reads() {
        // Literal reference encoding: PSH sid=258 "hi", then FIN sid=258.
        let wire = [2, 0, 0, 1, 2, 0, 2, b'h', b'i', 3, 0, 0, 1, 2, 0, 0];
        let (mut writer, reader) = tokio::io::duplex(1);
        let feeding = tokio::spawn(async move {
            for byte in wire {
                writer.write_all(&[byte]).await.unwrap();
            }
        });
        let mut reader = FrameReader::new(reader);
        let mut decoded = Vec::new();
        while let Some(frame) = reader.next().await.unwrap() {
            decoded.push(frame);
        }
        feeding.await.unwrap();

        let sid = Sid::new(258).unwrap();
        assert_eq!(
            decoded,
            [Frame::Push(sid, Bytes::from_static(b"hi")), Frame::Fin(sid)]
        );
        let mut encoded = BytesMut::new();
        for frame in &decoded {
            frame.encode(&mut encoded);
        }
        assert_eq!(&encoded[..], &wire);
    }

    #[tokio::test]
    async fn rejects_truncated_frames_and_contradictory_commands() {
        for wire in [
            &[1, 0, 0, 0, 0, 0, 0][..],
            &[2, 0, 0, 0, 0, 0, 1, 0],
            &[3, 0, 0, 0, 1, 0, 1, 0],
            &[4, 0, 0, 0, 1, 0, 1, b'v'],
            &[4, 0, 0, 0, 0, 0, 0],
            &[8, 0, 0, 0, 1, 0, 0],
            &[8, 0, 0, 0, 0, 0, 1, 0],
        ] {
            assert_eq!(
                decode_all(wire).await.unwrap_err().kind(),
                io::ErrorKind::InvalidData,
                "{wire:?}"
            );
        }
        for wire in [&[2, 0, 0, 0][..], &[2, 0, 0, 0, 1, 0, 2, 0]] {
            assert_eq!(
                decode_all(wire).await.unwrap_err().kind(),
                io::ErrorKind::UnexpectedEof
            );
        }
        assert_eq!(
            decode_all(&[42, 0, 0, 0, 7, 0, 1, 0]).await.unwrap(),
            [Frame::Unknown(42)]
        );
    }

    #[test]
    fn settings_ignore_malformed_lines_and_keep_the_last_duplicate() {
        let payload = b"v=1\nclient=a=b\nnoise\nv=2";
        assert_eq!(setting(payload, "v"), Some("2"));
        assert_eq!(setting(payload, "client"), Some("a=b"));
        assert_eq!(setting(payload, "noise"), None);
    }
}
