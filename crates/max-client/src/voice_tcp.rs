use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rustls::pki_types::ServerName;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::conn::{lookup_v4, tls_config};
use crate::error::{Error, Result};
use crate::models::{mobile_handshake_payload, Attach, VideoTicket, VoiceTicket};
use crate::profile::Session;

const HOST: &str = "api.oneme.ru";
const VER: u8 = 10;

pub struct VoiceLink {
    stream: tokio_rustls::client::TlsStream<TcpStream>,
    seq: u16,
    hold: Arc<AtomicBool>,
    cids: Arc<Mutex<Vec<i64>>>,
}

impl Drop for VoiceLink {
    fn drop(&mut self) {
        self.hold.store(false, Ordering::SeqCst);
    }
}

impl VoiceLink {
    pub async fn open(
        session: &Session,
        hold: Arc<AtomicBool>,
        cids: Arc<Mutex<Vec<i64>>>,
    ) -> Result<Self> {
        hold.store(true, Ordering::SeqCst);
        let (stream, seq) = match connect_voice(session).await {
            Ok(opened) => opened,
            Err(err) => {
                hold.store(false, Ordering::SeqCst);
                return Err(err);
            }
        };
        Ok(Self {
            stream,
            seq,
            hold,
            cids,
        })
    }

    pub async fn ticket(&mut self) -> Result<VoiceTicket> {
        let payload = roundtrip(self, 82, upload_request()).await?;
        voice_ticket(&payload)
    }

    pub async fn ticket_video(&mut self, note: bool) -> Result<VideoTicket> {
        let payload = roundtrip(self, 82, video_upload_request(note)).await?;
        video_ticket(&payload)
    }

    /// Wait for NOTIF_ATTACH (136) with matching videoId.
    /// Returns `true` if ready arrived, `false` if `timeout` elapsed.
    pub async fn wait_video_ready(&mut self, video_id: i64, timeout: Duration) -> Result<bool> {
        match tokio::time::timeout(timeout, async {
            loop {
                let (_cmd, _seq, opcode, body) = read_frame(&mut self.stream).await?;
                if opcode != 136 {
                    continue;
                }
                if attach_ready_id(&body) == Some(video_id) {
                    return Ok(());
                }
            }
        })
        .await
        {
            Ok(Ok(())) => Ok(true),
            Ok(Err(err)) => Err(err),
            Err(_) => Ok(false),
        }
    }

    pub async fn finish(
        &mut self,
        chat_id: i64,
        text: &str,
        reply_to: Option<i64>,
        audio_id: i64,
        token: &str,
        duration_ms: i64,
        wave: &[u8],
        photos: &[Attach],
    ) -> Result<Option<(i64, i64)>> {
        let cid = now_ms();
        if let Ok(mut extra) = self.cids.lock() {
            extra.push(cid);
        }
        let payload = roundtrip(
            self,
            64,
            send_request(
                chat_id,
                text,
                cid,
                reply_to,
                audio_id,
                token,
                duration_ms,
                wave,
                photos,
            ),
        )
        .await?;
        Ok(sent_stamp(&payload))
    }

    pub async fn finish_video(
        &mut self,
        chat_id: i64,
        text: &str,
        reply_to: Option<i64>,
        video_id: i64,
        token: &str,
        video_type: Option<i64>,
        thumbhash: Option<&[u8]>,
        photos: &[Attach],
    ) -> Result<Option<(i64, i64)>> {
        let cid = now_ms();
        if let Ok(mut extra) = self.cids.lock() {
            extra.push(cid);
        }
        let payload = roundtrip(
            self,
            64,
            send_video_request(
                chat_id, text, cid, reply_to, video_id, token, video_type, thumbhash, photos,
            ),
        )
        .await?;
        Ok(sent_stamp(&payload))
    }
}

async fn connect_voice(
    session: &Session,
) -> Result<(tokio_rustls::client::TlsStream<TcpStream>, u16)> {
    let addr = lookup_v4(HOST, 443).await?;
    let tcp = TcpStream::connect(addr).await?;
    tcp.set_nodelay(true)?;
    let connector = TlsConnector::from(Arc::new(tls_config()));
    let name = ServerName::try_from(HOST)
        .map_err(|err| Error::Protocol(err.to_string()))?
        .to_owned();
    let mut stream = connector
        .connect(name, tcp)
        .await
        .map_err(|err| Error::Protocol(err.to_string()))?;
    let seq = {
        let mut link = StreamSeq {
            stream: &mut stream,
            seq: 0,
        };
        let instance_id = if session.instance_id.is_empty() {
            // Match conn.rs: stable-ish id for this process when session has none.
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_nanos() as u64)
                .unwrap_or(0);
            format!("{:016x}", nanos ^ 0xA5A5_A5A5_A5A5_A5A5)
        } else {
            session.instance_id.clone()
        };
        let mut mixed = 0i64;
        for byte in session.device_id.bytes() {
            mixed = mixed.wrapping_mul(31).wrapping_add(i64::from(byte));
        }
        let client_session_id = (mixed.unsigned_abs() % 70) as i64 + 1;
        let hello = roundtrip_on(
            &mut link,
            6,
            json_as_msgpack(&mobile_handshake_payload(
                &session.device_id,
                &instance_id,
                client_session_id,
            )),
        )
        .await?;
        if let Some(err) = error_text(&hello) {
            return Err(Error::Protocol(err));
        }
        let logged = roundtrip_on(&mut link, 19, login_request(&session.token)).await?;
        if let Some(err) = error_text(&logged) {
            return Err(Error::Protocol(err));
        }
        link.seq
    };
    Ok((stream, seq))
}

struct StreamSeq<'a> {
    stream: &'a mut tokio_rustls::client::TlsStream<TcpStream>,
    seq: u16,
}

async fn roundtrip(link: &mut VoiceLink, opcode: u16, payload: rmpv::Value) -> Result<rmpv::Value> {
    let mut seq = StreamSeq {
        stream: &mut link.stream,
        seq: link.seq,
    };
    let value = roundtrip_on(&mut seq, opcode, payload).await?;
    link.seq = seq.seq;
    Ok(value)
}

async fn roundtrip_on(
    link: &mut StreamSeq<'_>,
    opcode: u16,
    payload: rmpv::Value,
) -> Result<rmpv::Value> {
    link.seq = link.seq.wrapping_add(1);
    if link.seq == 0 {
        link.seq = 1;
    }
    let seq = link.seq;
    let frame = pack_frame(0, seq, opcode, &payload)?;
    tokio::time::timeout(Duration::from_secs(20), async {
        link.stream.write_all(&frame).await?;
        link.stream.flush().await?;
        loop {
            let (cmd, got, opcode, body) = read_frame(link.stream).await?;
            // Keep scanning: NOTIF_ATTACH (136) can arrive before the reply.
            if opcode == 136 {
                continue;
            }
            if got != seq && got != (seq & 0xff) {
                continue;
            }
            if cmd == 3 {
                return Err(Error::Protocol(
                    error_text(&body).unwrap_or_else(|| "voice session rejected".into()),
                ));
            }
            if let Some(err) = error_text(&body) {
                return Err(Error::Protocol(err));
            }
            return Ok(body);
        }
    })
    .await
    .map_err(|_| Error::Protocol("voice session timed out".into()))?
}

async fn read_frame(
    stream: &mut tokio_rustls::client::TlsStream<TcpStream>,
) -> Result<(u8, u16, u16, rmpv::Value)> {
    let mut header = [0u8; 10];
    stream.read_exact(&mut header).await?;
    let cmd = header[1];
    let seq = u16::from_be_bytes([header[2], header[3]]);
    let opcode = u16::from_be_bytes([header[4], header[5]]);
    let packed = u32::from_be_bytes([header[6], header[7], header[8], header[9]]);
    let compressed = packed & 0x0100_0000 != 0;
    let len = (packed & 0x00ff_ffff) as usize;
    let mut body = vec![0u8; len];
    if len > 0 {
        stream.read_exact(&mut body).await?;
    }
    if compressed {
        body = inflate(&body)?;
    }
    Ok((cmd, seq, opcode, decode_payload(&body)?))
}

fn pack_frame(cmd: u8, seq: u16, opcode: u16, payload: &rmpv::Value) -> Result<Vec<u8>> {
    let mut packed = Vec::new();
    rmpv::encode::write_value(&mut packed, payload)
        .map_err(|err| Error::Protocol(err.to_string()))?;
    let mut out = Vec::with_capacity(10 + packed.len());
    out.push(VER);
    out.push(cmd);
    out.extend_from_slice(&seq.to_be_bytes());
    out.extend_from_slice(&opcode.to_be_bytes());
    out.extend_from_slice(&(packed.len() as u32).to_be_bytes());
    out.extend_from_slice(&packed);
    Ok(out)
}

fn decode_payload(bytes: &[u8]) -> Result<rmpv::Value> {
    if bytes.is_empty() {
        return Ok(rmpv::Value::Map(Vec::new()));
    }
    let offset = skip_leading_ints(bytes);
    decode_one(&bytes[offset..]).or_else(|_| decode_one(bytes))
}

fn decode_one(bytes: &[u8]) -> Result<rmpv::Value> {
    let mut cursor = std::io::Cursor::new(bytes);
    rmpv::decode::read_value(&mut cursor).map_err(|err| Error::Protocol(err.to_string()))
}

fn skip_leading_ints(payload: &[u8]) -> usize {
    let mut offset = 0;
    let mut count = 0;
    while count < 2 && offset < payload.len() {
        let byte = payload[offset];
        let step = if byte <= 0x7f || byte >= 0xe0 {
            1
        } else if byte == 0xd0 {
            2
        } else if byte == 0xd1 {
            3
        } else if byte == 0xd2 {
            5
        } else {
            break;
        };
        if offset + step > payload.len() {
            break;
        }
        offset += step;
        count += 1;
    }
    if count == 2 {
        offset
    } else {
        0
    }
}

fn inflate(body: &[u8]) -> Result<Vec<u8>> {
    let mut cap = body.len().saturating_mul(4).max(64);
    for _ in 0..8 {
        let mut out = vec![0u8; cap];
        if let Ok(written) = lz4_flex::block::decompress_into(body, &mut out) {
            out.truncate(written);
            return Ok(out);
        }
        cap = cap.saturating_mul(2);
    }
    Err(Error::Protocol("voice session is corrupt".into()))
}

fn upload_request() -> rmpv::Value {
    rmpv::Value::Map(vec![
        pair("count", int(1)),
        pair("type", int(2)),
        pair("uploaderType", int(1)),
    ])
}

/// Regular video: type=0/uploaderType=0 (CDN).
/// Video note: type=1/uploaderType=0 (CDN) — same as mobile / max-account-api.
/// Never use uploaderType=1 for video: that yields /uploadVideo and rejects the body.
fn video_upload_request(note: bool) -> rmpv::Value {
    let kind = if note { 1 } else { 0 };
    rmpv::Value::Map(vec![
        pair("count", int(1)),
        pair("type", int(kind)),
        pair("uploaderType", int(0)),
    ])
}

fn login_request(token: &str) -> rmpv::Value {
    rmpv::Value::Map(vec![
        pair("interactive", rmpv::Value::Boolean(true)),
        pair("token", text(token)),
        pair("chatsCount", int(0)),
        pair("chatsSync", int(0)),
        pair("contactsSync", int(0)),
        pair("presenceSync", int(0)),
        pair("draftsSync", int(0)),
    ])
}

fn send_request(
    chat_id: i64,
    text_body: &str,
    cid: i64,
    reply_to: Option<i64>,
    audio_id: i64,
    token: &str,
    duration_ms: i64,
    wave: &[u8],
    photos: &[Attach],
) -> rmpv::Value {
    let mut attaches = Vec::new();
    for photo in photos {
        if let Attach::Photo { token } = photo {
            attaches.push(rmpv::Value::Map(vec![
                pair("_type", text("PHOTO")),
                pair("photoToken", text(token)),
            ]));
        }
    }
    attaches.push(rmpv::Value::Map(vec![
        pair("_type", text("AUDIO")),
        pair("audioId", int(audio_id)),
        pair("token", text(token)),
        pair("duration", int(duration_ms.max(1000))),
        pair("wave", wave_bin(wave)),
    ]));
    let mut message = vec![
        pair("text", text(text_body)),
        pair("cid", int(cid)),
        pair("elements", rmpv::Value::Array(Vec::new())),
        pair("attaches", rmpv::Value::Array(attaches)),
    ];
    if let Some(reply_to) = reply_to {
        message.push(pair(
            "link",
            rmpv::Value::Map(vec![
                pair("type", text("REPLY")),
                pair("messageId", int(reply_to)),
            ]),
        ));
    }
    rmpv::Value::Map(vec![
        pair("chatId", int(chat_id)),
        pair("notify", rmpv::Value::Boolean(true)),
        pair("message", rmpv::Value::Map(message)),
    ])
}

fn voice_ticket(payload: &rmpv::Value) -> Result<VoiceTicket> {
    let info = map_get(payload, "info")
        .and_then(|value| match value {
            rmpv::Value::Array(items) => items.first(),
            _ => None,
        })
        .ok_or_else(|| Error::Protocol("voice upload did not return a url".into()))?;
    let url = map_get(info, "url")
        .and_then(as_string)
        .filter(|url| !url.is_empty())
        .ok_or_else(|| Error::Protocol("voice upload did not return a url".into()))?;
    let audio_id = map_get(info, "videoId")
        .and_then(as_i64)
        .ok_or_else(|| Error::Protocol("voice upload did not return an id".into()))?;
    let token = map_get(info, "token")
        .and_then(as_string)
        .filter(|token| !token.is_empty())
        .ok_or_else(|| Error::Protocol("voice upload did not return a token".into()))?;
    Ok(VoiceTicket {
        url,
        audio_id,
        token,
    })
}

fn video_ticket(payload: &rmpv::Value) -> Result<VideoTicket> {
    let info = map_get(payload, "info")
        .and_then(|value| match value {
            rmpv::Value::Array(items) => items.first(),
            _ => None,
        })
        .ok_or_else(|| Error::Protocol("video upload did not return a url".into()))?;
    let url = map_get(info, "url")
        .and_then(as_string)
        .filter(|url| !url.is_empty())
        .ok_or_else(|| Error::Protocol("video upload did not return a url".into()))?;
    let video_id = map_get(info, "videoId")
        .and_then(as_i64)
        .ok_or_else(|| Error::Protocol("video upload did not return an id".into()))?;
    let token = map_get(info, "token")
        .and_then(as_string)
        .filter(|token| !token.is_empty())
        .ok_or_else(|| Error::Protocol("video upload did not return a token".into()))?;
    Ok(VideoTicket {
        url,
        video_id,
        token,
    })
}

fn send_video_request(
    chat_id: i64,
    text_body: &str,
    cid: i64,
    reply_to: Option<i64>,
    video_id: i64,
    token: &str,
    video_type: Option<i64>,
    thumbhash: Option<&[u8]>,
    photos: &[Attach],
) -> rmpv::Value {
    let mut attaches = Vec::new();
    for photo in photos {
        if let Attach::Photo { token } = photo {
            attaches.push(rmpv::Value::Map(vec![
                pair("_type", text("PHOTO")),
                pair("photoToken", text(token)),
            ]));
        }
    }
    // Regular: videoId + token + videoType:0 (after VIDEO_READY).
    // Note (mobile): token + videoType:1 + thumbhash (empty ok; server fills).
    let video = if video_type == Some(1) {
        vec![
            pair("_type", text("VIDEO")),
            pair("token", text(token)),
            pair("videoType", int(1)),
            pair(
                "thumbhash",
                rmpv::Value::Binary(thumbhash.unwrap_or(&[]).to_vec()),
            ),
        ]
    } else {
        vec![
            pair("_type", text("VIDEO")),
            pair("videoId", int(video_id)),
            pair("token", text(token)),
            pair("videoType", int(0)),
        ]
    };
    attaches.push(rmpv::Value::Map(video));
    let mut message = vec![
        pair("text", text(text_body)),
        pair("cid", int(cid)),
        pair("elements", rmpv::Value::Array(Vec::new())),
        pair("attaches", rmpv::Value::Array(attaches)),
    ];
    if let Some(reply_to) = reply_to {
        message.push(pair(
            "link",
            rmpv::Value::Map(vec![
                pair("type", text("REPLY")),
                pair("messageId", int(reply_to)),
            ]),
        ));
    }
    rmpv::Value::Map(vec![
        pair("chatId", int(chat_id)),
        pair("notify", rmpv::Value::Boolean(true)),
        pair("message", rmpv::Value::Map(message)),
    ])
}

fn sent_stamp(payload: &rmpv::Value) -> Option<(i64, i64)> {
    let message = map_get(payload, "message")?;
    let id = map_get(message, "id").and_then(as_i64)?;
    let time = map_get(message, "time").and_then(as_i64).unwrap_or(0);
    Some((id, time))
}

fn wave_bin(wave: &[u8]) -> rmpv::Value {
    let mut bytes = vec![0u8; 80];
    let count = wave.len().min(80);
    bytes[..count].copy_from_slice(&wave[..count]);
    rmpv::Value::Binary(bytes)
}

fn attach_ready_id(value: &rmpv::Value) -> Option<i64> {
    for key in ["videoId", "audioId", "fileId"] {
        if let Some(id) = map_get(value, key).and_then(as_i64) {
            return Some(id);
        }
    }
    None
}

fn error_text(value: &rmpv::Value) -> Option<String> {
    let err = map_get(value, "error").and_then(as_string)?;
    if err.is_empty() {
        return None;
    }
    let message = map_get(value, "message")
        .and_then(as_string)
        .filter(|text| !text.is_empty() && text.len() < 120 && !text.contains("http"));
    Some(match message {
        Some(message) => format!("{err}: {message}"),
        None => err,
    })
}

fn pair(key: &str, value: rmpv::Value) -> (rmpv::Value, rmpv::Value) {
    (text(key), value)
}

fn text(value: &str) -> rmpv::Value {
    rmpv::Value::String(value.into())
}

fn int(value: i64) -> rmpv::Value {
    rmpv::Value::Integer(value.into())
}

fn map_get<'a>(value: &'a rmpv::Value, key: &str) -> Option<&'a rmpv::Value> {
    let rmpv::Value::Map(items) = value else {
        return None;
    };
    items.iter().find_map(|(name, item)| {
        let rmpv::Value::String(name) = name else {
            return None;
        };
        (name.as_str() == Some(key)).then_some(item)
    })
}

fn as_string(value: &rmpv::Value) -> Option<String> {
    match value {
        rmpv::Value::String(text) => Some(text.as_str().unwrap_or("").to_owned()),
        _ => None,
    }
}

fn as_i64(value: &rmpv::Value) -> Option<i64> {
    match value {
        rmpv::Value::Integer(number) => number
            .as_i64()
            .or_else(|| number.as_u64().and_then(|item| i64::try_from(item).ok())),
        rmpv::Value::String(text) => text.as_str().unwrap_or("").parse().ok(),
        rmpv::Value::Ext(1, bytes) => {
            let mut cursor = std::io::Cursor::new(bytes);
            rmpv::decode::read_value(&mut cursor)
                .ok()
                .as_ref()
                .and_then(as_i64)
        }
        _ => None,
    }
}

fn json_as_msgpack(value: &serde_json::Value) -> rmpv::Value {
    match value {
        serde_json::Value::Null => rmpv::Value::Nil,
        serde_json::Value::Bool(item) => rmpv::Value::Boolean(*item),
        serde_json::Value::Number(number) => {
            if let Some(item) = number.as_i64() {
                int(item)
            } else if let Some(item) = number.as_u64() {
                item.try_into().map(int).unwrap_or(int(0))
            } else {
                rmpv::Value::F64(number.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(item) => text(item),
        serde_json::Value::Array(items) => {
            rmpv::Value::Array(items.iter().map(json_as_msgpack).collect())
        }
        serde_json::Value::Object(map) => rmpv::Value::Map(
            map.iter()
                .map(|(key, item)| pair(key, json_as_msgpack(item)))
                .collect(),
        ),
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn voice_wave_is_eighty_raw_bytes() {
        let mut wave = vec![0u8; 80];
        wave[0] = 127;
        wave[79] = 40;
        let payload = send_request(9, "", 3, Some(4), 7, "tok", 1500, &wave, &[]);
        let raw = pack_frame(0, 1, 64, &payload).unwrap();
        assert_eq!(raw[0], 10);
        assert_eq!(raw[1], 0);
        assert_eq!(u16::from_be_bytes([raw[2], raw[3]]), 1);
        assert_eq!(u16::from_be_bytes([raw[4], raw[5]]), 64);
        let len = u32::from_be_bytes([raw[6], raw[7], raw[8], raw[9]]) as usize;
        assert_eq!(raw.len(), 10 + len);
        let value = decode_one(&raw[10..]).unwrap();
        let wave = map_get(map_get(&value, "message").unwrap(), "attaches")
            .and_then(|item| match item {
                rmpv::Value::Array(items) => items.first(),
                _ => None,
            })
            .and_then(|item| map_get(item, "wave"))
            .unwrap();
        match wave {
            rmpv::Value::Binary(bytes) => {
                assert_eq!(bytes.len(), 80);
                assert_eq!(bytes[0], 127);
                assert_eq!(bytes[79], 40);
            }
            other => panic!("wave is {other:?}"),
        }
        assert_eq!(
            map_get(map_get(&value, "message").unwrap(), "link")
                .and_then(|item| map_get(item, "messageId"))
                .and_then(as_i64),
            Some(4)
        );
    }

    #[test]
    fn leading_ints_are_skipped() {
        let mut bytes = vec![0xf0, 0x4b];
        let map = rmpv::Value::Map(vec![pair("ok", rmpv::Value::Boolean(true))]);
        rmpv::encode::write_value(&mut bytes, &map).unwrap();
        let value = decode_payload(&bytes).unwrap();
        assert_eq!(map_get(&value, "ok"), Some(&rmpv::Value::Boolean(true)));
        assert_eq!(skip_leading_ints(&[0x80]), 0);
    }
}
