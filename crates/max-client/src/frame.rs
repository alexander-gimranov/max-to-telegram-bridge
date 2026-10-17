use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, Result};

pub const VER: i64 = 11;
pub const CMD_REQUEST: i64 = 0;
pub const CMD_EVENT: i64 = 2;
pub const CMD_ERROR: i64 = 3;

pub const PING: i64 = 1;
pub const SESSION_INIT: i64 = 6;
pub const AUTH_REQUEST: i64 = 17;
pub const AUTH: i64 = 18;
pub const LOGIN: i64 = 19;
pub const CONTACT_INFO: i64 = 32;
#[allow(dead_code)]
pub const CONTACT_LIST: i64 = 36;
pub const CONTACT_ADD_BY_PHONE: i64 = 41;
pub const CHAT_INFO: i64 = 48;
pub const CHAT_HISTORY: i64 = 49;
pub const CHAT_MARK: i64 = 50;
pub const CHATS_LIST: i64 = 53;
pub const MSG_SEND: i64 = 64;
pub const PHOTO_UPLOAD: i64 = 80;
pub const VIDEO_UPLOAD: i64 = 82;
pub const VIDEO_PLAY: i64 = 83;
pub const AUTH_LOGIN_CHECK_PASSWORD: i64 = 115;
pub const NOTIF_MESSAGE: i64 = 128;
pub const NOTIF_MARK: i64 = 130;
pub const NOTIF_ATTACH: i64 = 136;
pub const MSG_DELIVERY: i64 = 303;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame {
    pub ver: i64,
    pub cmd: i64,
    pub seq: i64,
    pub opcode: i64,
    pub payload: Value,
}

pub fn request(seq: i64, opcode: i64, payload: Value) -> Frame {
    Frame {
        ver: VER,
        cmd: CMD_REQUEST,
        seq,
        opcode,
        payload,
    }
}

#[cfg(test)]
pub fn encode(frame: &Frame) -> Result<String> {
    Ok(serde_json::to_string(frame)?)
}

const BIN_VER: u8 = 10;
const BIN_HEAD: usize = 10;

pub fn encode_bin(frame: &Frame) -> Result<Vec<u8>> {
    let packed = pack_payload(&frame.payload)?;
    let mut out = Vec::with_capacity(BIN_HEAD + packed.len());
    out.push(BIN_VER);
    out.push(frame.cmd as u8);
    out.extend_from_slice(&(frame.seq as i16).to_be_bytes());
    out.extend_from_slice(&(frame.opcode as i16).to_be_bytes());
    out.push(0);
    let len = packed.len();
    out.push(((len >> 16) & 0xff) as u8);
    out.push(((len >> 8) & 0xff) as u8);
    out.push((len & 0xff) as u8);
    out.extend_from_slice(&packed);
    Ok(out)
}

pub fn decode_bin(raw: &[u8]) -> Result<Frame> {
    if raw.len() < BIN_HEAD {
        return Err(Error::Protocol("MAX frame is too short".into()));
    }
    let len = ((raw[7] as usize) << 16) | ((raw[8] as usize) << 8) | (raw[9] as usize);
    if raw.len() < BIN_HEAD + len {
        return Err(Error::Protocol("MAX frame is too short".into()));
    }
    let body = &raw[BIN_HEAD..BIN_HEAD + len];
    let packed = if raw[6] == 0 {
        body.to_vec()
    } else {
        let hint = body.len().saturating_mul(raw[6] as usize).max(1);
        let mut out = vec![0u8; hint];
        let written = lz4_flex::block::decompress_into(body, &mut out)
            .map_err(|_| Error::Protocol("MAX frame is corrupt".into()))?;
        out.truncate(written);
        out
    };
    let payload = if packed.is_empty() {
        serde_json::json!({})
    } else {
        let mut cursor = std::io::Cursor::new(packed);
        let value = rmpv::decode::read_value(&mut cursor)
            .map_err(|err| Error::Protocol(err.to_string()))?;
        msgpack_to_json(value)
    };
    Ok(Frame {
        ver: i64::from(raw[0]),
        cmd: i64::from(raw[1]),
        seq: i64::from(i16::from_be_bytes([raw[2], raw[3]])),
        opcode: i64::from(i16::from_be_bytes([raw[4], raw[5]])),
        payload,
    })
}

fn pack_payload(payload: &Value) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    rmpv::encode::write_value(&mut out, &json_to_msgpack(payload))
        .map_err(|err| Error::Protocol(err.to_string()))?;
    Ok(out)
}

fn json_to_msgpack(value: &Value) -> rmpv::Value {
    match value {
        Value::Null => rmpv::Value::Nil,
        Value::Bool(item) => rmpv::Value::Boolean(*item),
        Value::Number(number) => {
            if let Some(item) = number.as_i64() {
                rmpv::Value::Integer(item.into())
            } else if let Some(item) = number.as_u64() {
                rmpv::Value::Integer(item.into())
            } else {
                rmpv::Value::F64(number.as_f64().unwrap_or(0.0))
            }
        }
        Value::String(text) => rmpv::Value::String(text.clone().into()),
        Value::Array(items) => {
            rmpv::Value::Array(items.iter().map(json_to_msgpack).collect())
        }
        Value::Object(map) => rmpv::Value::Map(
            map.iter()
                .map(|(key, item)| {
                    let packed = if key == "wave" {
                        wave_bytes(item).unwrap_or_else(|| json_to_msgpack(item))
                    } else if key == "mode" {
                        mode_bytes(item).unwrap_or_else(|| json_to_msgpack(item))
                    } else if key == "chatsCountGroups" {
                        group_count_bytes(item).unwrap_or_else(|| json_to_msgpack(item))
                    } else {
                        json_to_msgpack(item)
                    };
                    (rmpv::Value::String(key.clone().into()), packed)
                })
                .collect(),
        ),
    }
}

fn wave_bytes(value: &Value) -> Option<rmpv::Value> {
    binary_field(value, 80)
}

fn mode_bytes(value: &Value) -> Option<rmpv::Value> {
    binary_field(value, 96)
}

fn group_count_bytes(value: &Value) -> Option<rmpv::Value> {
    // PyMax sends bytearray.fromhex("0a32") inside exp.chatsCountGroups.
    binary_field(value, 2)
}

fn binary_field(value: &Value, len: usize) -> Option<rmpv::Value> {
    let text = value.as_str()?;
    let bytes = base64::engine::general_purpose::STANDARD.decode(text).ok()?;
    if bytes.len() == len {
        Some(rmpv::Value::Binary(bytes))
    } else {
        None
    }
}

fn msgpack_to_json(value: rmpv::Value) -> Value {
    match value {
        rmpv::Value::Nil => Value::Null,
        rmpv::Value::Boolean(item) => Value::Bool(item),
        rmpv::Value::Integer(number) => {
            if let Some(item) = number.as_i64() {
                serde_json::json!(item)
            } else if let Some(item) = number.as_u64() {
                serde_json::json!(item)
            } else {
                Value::Null
            }
        }
        rmpv::Value::F32(number) => serde_json::Number::from_f64(f64::from(number))
            .map(Value::Number)
            .unwrap_or(Value::Null),
        rmpv::Value::F64(number) => serde_json::Number::from_f64(number)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        rmpv::Value::String(text) => Value::String(text.as_str().unwrap_or("").to_owned()),
        rmpv::Value::Binary(bytes) => {
            Value::String(base64::engine::general_purpose::STANDARD.encode(bytes))
        }
        rmpv::Value::Array(items) => Value::Array(items.into_iter().map(msgpack_to_json).collect()),
        rmpv::Value::Map(items) => {
            let mut map = serde_json::Map::new();
            for (key, item) in items {
                let name = match key {
                    rmpv::Value::String(text) => text.as_str().unwrap_or("").to_owned(),
                    rmpv::Value::Integer(number) => number.to_string(),
                    other => other.to_string(),
                };
                map.insert(name, msgpack_to_json(item));
            }
            Value::Object(map)
        }
        rmpv::Value::Ext(1, bytes) => {
            let mut cursor = std::io::Cursor::new(bytes);
            match rmpv::decode::read_value(&mut cursor) {
                Ok(inner) => msgpack_to_json(inner),
                Err(_) => Value::Null,
            }
        }
        rmpv::Value::Ext(_, _) => Value::Null,
    }
}

#[cfg(test)]
pub fn decode(raw: &str) -> Result<Frame> {
    let value: Value = serde_json::from_str(raw)?;
    let obj = value
        .as_object()
        .ok_or_else(|| Error::Protocol("frame is not an object".into()))?;
    Ok(Frame {
        ver: obj.get("ver").and_then(Value::as_i64).unwrap_or(VER),
        cmd: obj.get("cmd").and_then(Value::as_i64).unwrap_or(0),
        seq: json_i64(obj.get("seq")).unwrap_or(0),
        opcode: json_i64(obj.get("opcode")).unwrap_or(0),
        payload: obj.get("payload").cloned().unwrap_or(Value::Null),
    })
}

pub fn payload_error(payload: &Value) -> Option<String> {
    let obj = payload.as_object()?;
    let err = obj.get("error")?;
    if err.is_null() {
        return None;
    }
    if let Some(text) = err.as_str() {
        if text.is_empty() {
            return None;
        }
        let message = obj
            .get("message")
            .or_else(|| obj.get("localizedMessage"))
            .and_then(Value::as_str);
        return Some(match message {
            Some(message) if !message.is_empty() => format!("{text}: {message}"),
            _ => text.to_owned(),
        });
    }
    if let Some(map) = err.as_object() {
        let message = map
            .get("message")
            .or_else(|| map.get("title"))
            .or_else(|| map.get("error"))
            .and_then(Value::as_str)
            .unwrap_or("request failed");
        return Some(message.to_owned());
    }
    Some(err.to_string())
}

pub fn json_i64(value: Option<&Value>) -> Option<i64> {
    let value = value?;
    if let Some(number) = value.as_i64() {
        return Some(number);
    }
    if let Some(number) = value.as_u64() {
        return i64::try_from(number).ok();
    }
    value.as_str().and_then(|text| text.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn frame_round_trip() {
        let frame = request(3, SESSION_INIT, json!({"deviceId": "abc"}));
        let raw = encode(&frame).unwrap();
        let back = decode(&raw).unwrap();
        assert_eq!(back.ver, 11);
        assert_eq!(back.cmd, CMD_REQUEST);
        assert_eq!(back.seq, 3);
        assert_eq!(back.opcode, SESSION_INIT);
        assert_eq!(back.payload["deviceId"], "abc");
    }

    #[test]
    fn voice_wave_is_eighty_raw_bytes() {
        let payload = json!({
            "wave": base64::engine::general_purpose::STANDARD.encode([0u8; 80])
        });
        let frame = request(4, MSG_SEND, payload);
        let raw = encode_bin(&frame).unwrap();
        assert_eq!(raw[0], 10);
        let back = decode_bin(&raw).unwrap();
        assert_eq!(back.seq, 4);
        assert_eq!(back.opcode, MSG_SEND);
        let wave = base64::engine::general_purpose::STANDARD
            .decode(back.payload["wave"].as_str().unwrap())
            .unwrap();
        assert_eq!(wave, vec![0u8; 80]);
    }

    #[test]
    fn reads_error_string() {
        let payload = json!({"error": "proto.payload", "message": "bad"});
        assert_eq!(
            payload_error(&payload).as_deref(),
            Some("proto.payload: bad")
        );
    }
}
