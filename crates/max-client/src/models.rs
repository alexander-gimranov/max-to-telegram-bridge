use base64::Engine;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::frame::json_i64;

const CERT_META: &str = "1684414033eb263e2c615f8b7df5ed8793850a07656304997fbf07e9e21e1e93";
const DEX_META: &str = "8db68fcc0e85e8f041fe4a875c0a9bcfe542a8f679603728c651ed81b64dd684";
const SO_ARM64: &str = "634ecc42b246784d975f180b4fecf903df235cdf0476da47163a85630eb1a6a8";

#[derive(Debug, Clone)]
pub struct Name {
    pub name: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub kind: String,
}

#[derive(Debug, Clone)]
pub struct User {
    pub id: i64,
    pub names: Vec<Name>,
    pub photo: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Chat {
    pub id: i64,
    pub title: Option<String>,
    pub kind: String,
    pub peers: Vec<i64>,
    pub photo: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChatFace {
    pub label: String,
    pub group: bool,
}

#[derive(Debug, Clone)]
pub struct Message {
    pub id: i64,
    pub chat_id: Option<i64>,
    pub sender: Option<i64>,
    pub text: String,
    pub status: Option<String>,
    pub cid: Option<i64>,
    pub reply_to: Option<i64>,
    pub forward: Option<ForwardSource>,
    pub time: i64,
    pub media: Vec<Media>,
}

#[derive(Debug, Clone)]
pub struct ForwardSource {
    pub sender: Option<i64>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Media {
    Photo { url: String },
    Voice { url: String, duration_ms: i64 },
    Video {
        video_id: i64,
        token: String,
        video_type: Option<i64>,
        source_chat: Option<i64>,
        source_message: Option<i64>,
    },
}

#[derive(Debug, Clone)]
pub enum Attach {
    Photo { token: String },
    Voice {
        audio_id: i64,
        token: String,
        duration_ms: i64,
    },
    Video {
        video_id: i64,
        token: String,
        video_type: Option<i64>,
    },
}

#[derive(Debug, Clone)]
pub struct VoiceTicket {
    pub url: String,
    pub audio_id: i64,
    pub token: String,
}

#[derive(Debug, Clone)]
pub struct VideoTicket {
    pub url: String,
    pub video_id: i64,
    pub token: String,
}

impl User {
    pub fn display_name(&self) -> String {
        display_names(&self.names)
    }
}

pub fn display_names(names: &[Name]) -> String {
    let mut ranked: Vec<&Name> = names.iter().collect();
    ranked.sort_by_key(|item| match item.kind.as_str() {
        "CUSTOM" => 0,
        "ONEME" => 1,
        _ => 2,
    });
    for item in ranked {
        if let Some(name) = item.name.as_deref() {
            if !name.is_empty() {
                return name.to_owned();
            }
        }
        let first = item.first_name.as_deref().unwrap_or("");
        let last = item.last_name.as_deref().unwrap_or("");
        let full = format!("{first} {last}").trim().to_owned();
        if !full.is_empty() {
            return full;
        }
    }
    String::new()
}

pub fn web_user_agent() -> Value {
    json!({
        "deviceType": "WEB",
        "locale": "ru",
        "deviceLocale": "ru",
        "osVersion": "Linux",
        "deviceName": "Chrome",
        "headerUserAgent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
        "appVersion": "26.8.4",
        "screen": "1080x1920 1.0x",
        "timezone": "Europe/Moscow"
    })
}

pub fn handshake_payload(device_id: &str, desktop: bool) -> Value {
    if desktop {
        return json!({
            "deviceId": device_id,
            "clientSessionId": 1,
            "userAgent": {
                "deviceType": "DESKTOP",
                "appVersion": "26.35.2",
                "osVersion": "Ubuntu 26.04.1 LTS",
                "locale": "en",
                "deviceLocale": "en",
                "deviceName": "ubuntu",
                "screen": "2.0x",
                "timezone": "Etc/UTC",
                "msiProductCode": "",
                "buildNumber": 80824
            }
        });
    }
    json!({
        "deviceId": device_id,
        "userAgent": web_user_agent(),
    })
}

pub fn mobile_user_agent() -> Value {
    json!({
        "deviceType": "ANDROID",
        "appVersion": "26.25.0",
        "osVersion": "Android 14",
        "timezone": "Europe/Moscow",
        "screen": "428dpi 428dpi 1080x2400",
        "pushDeviceType": "GCM",
        "arch": "arm64-v8a",
        "locale": "ru",
        "buildNumber": 6790,
        "deviceName": "Pixel 8",
        "deviceLocale": "ru"
    })
}

pub fn mobile_handshake_payload(device_id: &str, instance_id: &str, session_id: i64) -> Value {
    json!({
        "mt_instanceid": instance_id,
        "clientSessionId": session_id,
        "deviceId": device_id,
        "userAgent": mobile_user_agent(),
    })
}

pub fn auth_mode(device_id: &str, calls_seed: i64) -> String {
    let seed = calls_seed.to_be_bytes();
    let device = device_id.as_bytes();
    let mut out = Vec::with_capacity(96);
    for hex in [CERT_META, DEX_META, SO_ARM64] {
        let mut hasher = Sha256::new();
        hasher.update(decode_hex(hex));
        hasher.update(seed);
        hasher.update(device);
        out.extend_from_slice(&hasher.finalize());
    }
    base64::engine::general_purpose::STANDARD.encode(out)
}

fn decode_hex(text: &str) -> [u8; 32] {
    let mut out = [0u8; 32];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16).unwrap_or(0);
    }
    out
}

pub fn auth_request_payload(phone: &str, mode: &str) -> Value {
    json!({
        "phone": phone,
        "type": "START_AUTH",
        "mode": mode,
    })
}

pub fn auth_code_payload(token: &str, code: &str) -> Value {
    json!({
        "token": token,
        "verifyCode": code,
        "authTokenType": "CHECK_CODE",
    })
}

pub fn auth_password_payload(track_id: &str, password: &str) -> Value {
    json!({
        "trackId": track_id,
        "password": password,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeChallenge {
    pub token: String,
    pub code_length: i64,
    pub wait_secs: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodeStatus {
    Token(String),
    Password { track_id: String, hint: Option<String> },
    Unregistered,
}

pub fn parse_code_challenge(payload: &Value) -> Result<CodeChallenge> {
    let token = payload
        .get("token")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .ok_or_else(|| Error::Protocol("auth request did not return a token".into()))?;
    Ok(CodeChallenge {
        token: token.to_owned(),
        code_length: json_i64(payload.get("codeLength")).unwrap_or(6),
        wait_secs: auth_wait_secs(json_i64(payload.get("requestMaxDuration"))),
    })
}

pub fn parse_code_status(payload: &Value) -> Result<CodeStatus> {
    if let Some(track_id) = payload
        .get("passwordChallenge")
        .and_then(|challenge| challenge.get("trackId"))
        .and_then(Value::as_str)
        .filter(|track_id| !track_id.is_empty())
    {
        let hint = payload
            .get("passwordChallenge")
            .and_then(|challenge| challenge.get("hint"))
            .and_then(Value::as_str)
            .filter(|hint| !hint.is_empty())
            .map(str::to_owned);
        return Ok(CodeStatus::Password {
            track_id: track_id.to_owned(),
            hint,
        });
    }
    if attr_token(payload, "REGISTER").is_some() {
        return Ok(CodeStatus::Unregistered);
    }
    if let Some(token) = attr_token(payload, "LOGIN") {
        return Ok(CodeStatus::Token(token));
    }
    Err(Error::Protocol(
        "auth code response did not return a token".into(),
    ))
}

pub fn parse_login_token(payload: &Value) -> Result<String> {
    attr_token(payload, "LOGIN")
        .ok_or_else(|| Error::Protocol("password check did not return a token".into()))
}

fn attr_token(payload: &Value, kind: &str) -> Option<String> {
    payload
        .get("tokenAttrs")
        .and_then(|attrs| attrs.get(kind))
        .and_then(|item| item.get("token"))
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
}

pub fn login_payload(
    token: &str,
    chats_sync: i64,
    contacts_sync: i64,
    drafts_sync: i64,
    presence_sync: i64,
    desktop: bool,
) -> Value {
    let mut payload = json!({
        "token": token,
        "interactive": true,
        "chatsCount": 40,
        "chatsSync": chats_sync,
        "contactsSync": contacts_sync,
        "draftsSync": drafts_sync,
        "presenceSync": presence_sync,
    });
    if desktop {
        payload["callsSync"] = json!(0);
        payload["bannersSync"] = json!(0);
        payload["configHash"] = json!("");
        payload["lastLogin"] = json!(0);
    } else {
        payload["userAgent"] = mobile_user_agent();
        payload["configHash"] = json!(
            "00000000-0000000000000000-00000000-0000000000000000-0000000000000000-0-0000000000000000-00000000"
        );
    }
    payload
}

fn auth_wait_secs(value: Option<i64>) -> i64 {
    match value {
        Some(wait) if wait >= 1_000 => wait / 1_000,
        Some(wait) if wait > 0 => wait,
        _ => 60,
    }
}

pub fn send_payload(
    chat_id: i64,
    text: &str,
    cid: i64,
    reply_to: Option<i64>,
    attaches: &[Attach],
) -> Value {
    let mut message = json!({
        "text": text,
        "cid": cid,
        "elements": [],
        "attaches": attaches.iter().map(attach_json).collect::<Vec<_>>(),
    });
    if let Some(reply_to) = reply_to {
        message["link"] = json!({
            "type": "REPLY",
            "messageId": reply_to,
        });
    }
    json!({
        "chatId": chat_id,
        "notify": true,
        "message": message,
    })
}

fn attach_json(attach: &Attach) -> Value {
    match attach {
        Attach::Photo { token } => json!({
            "_type": "PHOTO",
            "photoToken": token,
        }),
        Attach::Voice {
            audio_id,
            token,
            duration_ms,
        } => json!({
            "_type": "AUDIO",
            "audioId": audio_id,
            "token": token,
            "duration": duration_ms,
        }),
        Attach::Video {
            video_id,
            token,
            video_type,
        } => {
            let mut attach = json!({
                "_type": "VIDEO",
                "videoId": video_id,
                "token": token,
            });
            if video_type == &Some(1) {
                attach["videoType"] = json!(1);
            }
            attach
        }
    }
}

pub fn history_payload(chat_id: i64, backward: i64, from_ms: i64) -> Value {
    json!({
        "chatId": chat_id,
        "forward": 0,
        "backward": backward,
        "backwardTime": 0,
        "forwardTime": 0,
        "getChat": false,
        "from": from_ms,
        "itemType": "REGULAR",
        "getMessages": true,
        "interactive": true,
    })
}

pub fn parse_message(payload: &Value) -> Option<Message> {
    let wrapped = payload.as_object().and_then(|obj| obj.get("message"));
    let (body, outer) = if let Some(message) = wrapped {
        (message, Some(payload))
    } else {
        (payload, None)
    };
    let body = body.as_object()?;
    let id = json_i64(body.get("id"))?;
    let chat_id = json_i64(body.get("chatId"))
        .or_else(|| outer.and_then(|value| json_i64(value.get("chatId"))));
    let sender = json_i64(body.get("sender")).or_else(|| json_i64(body.get("senderId")));
    let text = body
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    let status = body
        .get("status")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let cid = json_i64(body.get("cid"));
    let media = parse_media(body);
    if media.is_empty() && attach_kinds(body).iter().any(|kind| kind == "VIDEO") {
        eprintln!("bridge: message {id} video attachment has no videoId");
    }
    Some(Message {
        id,
        chat_id,
        sender,
        text,
        status,
        cid,
        reply_to: reply_target(body),
        forward: forward_source(body),
        time: json_i64(body.get("time")).unwrap_or(0),
        media,
    })
}

fn parse_media(body: &serde_json::Map<String, Value>) -> Vec<Media> {
    let direct = media_list(body.get("attaches"), None, None);
    if !direct.is_empty() {
        return direct;
    }
    let Some(link) = body.get("link").and_then(Value::as_object) else {
        return direct;
    };
    if link.get("type").and_then(Value::as_str) != Some("FORWARD") {
        return direct;
    }
    let Some(message) = link.get("message").and_then(Value::as_object) else {
        return direct;
    };
    media_list(
        message.get("attaches"),
        json_i64(link.get("chatId")),
        json_i64(message.get("id")),
    )
}

fn media_list(
    attaches: Option<&Value>,
    source_chat: Option<i64>,
    source_message: Option<i64>,
) -> Vec<Media> {
    let Some(items) = attaches.and_then(Value::as_array) else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|item| {
            let obj = item.as_object()?;
            match attach_kind(obj) {
                Some("PHOTO") => {
                    let url = obj.get("baseUrl").and_then(Value::as_str)?;
                    if url.is_empty() {
                        return None;
                    }
                    Some(Media::Photo {
                        url: url.to_owned(),
                    })
                }
                Some("AUDIO") => {
                    let url = obj.get("url").and_then(Value::as_str)?;
                    if url.is_empty() {
                        return None;
                    }
                    Some(Media::Voice {
                        url: url.to_owned(),
                        duration_ms: json_i64(obj.get("duration")).unwrap_or(0),
                    })
                }
                Some("VIDEO") => {
                    let video_id = json_i64(obj.get("videoId"))?;
                    let token = obj
                        .get("token")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_owned();
                    Some(Media::Video {
                        video_id,
                        token,
                        video_type: json_i64(obj.get("videoType")),
                        source_chat,
                        source_message,
                    })
                }
                _ => None,
            }
        })
        .collect()
}

fn attach_kind(obj: &serde_json::Map<String, Value>) -> Option<&str> {
    obj.get("_type")
        .and_then(Value::as_str)
        .or_else(|| obj.get("type").and_then(Value::as_str))
}

fn attach_kinds(body: &serde_json::Map<String, Value>) -> Vec<String> {
    let mut kinds = Vec::new();
    push_attach_kinds(&mut kinds, body.get("attaches"));
    let Some(link) = body.get("link").and_then(Value::as_object) else {
        return kinds;
    };
    if link.get("type").and_then(Value::as_str) != Some("FORWARD") {
        return kinds;
    }
    let Some(message) = link.get("message").and_then(Value::as_object) else {
        return kinds;
    };
    push_attach_kinds(&mut kinds, message.get("attaches"));
    kinds
}

fn push_attach_kinds(kinds: &mut Vec<String>, attaches: Option<&Value>) {
    let Some(items) = attaches.and_then(Value::as_array) else {
        return;
    };
    for item in items {
        let Some(obj) = item.as_object() else {
            continue;
        };
        kinds.push(attach_kind(obj).unwrap_or("unknown").to_owned());
    }
}

fn reply_target(body: &serde_json::Map<String, Value>) -> Option<i64> {
    let link = body.get("link")?.as_object()?;
    match link.get("type").and_then(Value::as_str) {
        Some("REPLY") => linked_message_id(link),
        Some("FORWARD") => {
            let message = link.get("message")?.as_object()?;
            let inner = message.get("link")?.as_object()?;
            if inner.get("type").and_then(Value::as_str) != Some("REPLY") {
                return None;
            }
            linked_message_id(inner)
        }
        _ => None,
    }
}

fn linked_message_id(link: &serde_json::Map<String, Value>) -> Option<i64> {
    link.get("message")
        .and_then(Value::as_object)
        .and_then(|message| json_i64(message.get("id")))
        .or_else(|| json_i64(link.get("messageId")))
}

fn forward_source(body: &serde_json::Map<String, Value>) -> Option<ForwardSource> {
    let link = body.get("link")?.as_object()?;
    if link.get("type").and_then(Value::as_str) != Some("FORWARD") {
        return None;
    }
    let message = link.get("message").and_then(Value::as_object);
    let sender = message.and_then(|message| {
        json_i64(message.get("sender")).or_else(|| json_i64(message.get("senderId")))
    });
    let name = link
        .get("chatName")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_owned);
    Some(ForwardSource { sender, name })
}

pub fn parse_user(value: &Value) -> Option<User> {
    let obj = value.as_object()?;
    let id = json_i64(obj.get("id"))?;
    let names = obj
        .get("names")
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(parse_name).collect())
        .unwrap_or_default();
    Some(User {
        id,
        names,
        photo: first_url(obj, &["baseUrl", "baseRawUrl"]),
    })
}

fn first_url(obj: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        obj.get(*key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|url| !url.is_empty())
            .map(str::to_owned)
    })
}

fn parse_name(value: &Value) -> Option<Name> {
    let obj = value.as_object()?;
    Some(Name {
        name: obj.get("name").and_then(Value::as_str).map(str::to_owned),
        first_name: obj
            .get("firstName")
            .and_then(Value::as_str)
            .map(str::to_owned),
        last_name: obj
            .get("lastName")
            .and_then(Value::as_str)
            .map(str::to_owned),
        kind: obj
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned(),
    })
}

pub fn parse_chat(value: &Value) -> Option<Chat> {
    let obj = value.as_object()?;
    Some(Chat {
        id: json_i64(obj.get("id"))?,
        title: obj.get("title").and_then(Value::as_str).map(str::to_owned),
        kind: obj
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned(),
        peers: peer_ids(obj.get("participants")),
        photo: first_url(obj, &["baseIconUrl", "baseUrl", "iconUrl"]),
    })
}

fn peer_ids(value: Option<&Value>) -> Vec<i64> {
    let Some(map) = value.and_then(Value::as_object) else {
        return Vec::new();
    };
    let mut ids: Vec<i64> = map.keys().filter_map(|key| key.parse().ok()).collect();
    ids.sort_unstable();
    ids
}

pub fn parse_chats(payload: &Value) -> Vec<Chat> {
    let Some(obj) = payload.as_object() else {
        return Vec::new();
    };
    let Some(items) = obj.get("chats").and_then(Value::as_array) else {
        return Vec::new();
    };
    items.iter().filter_map(parse_chat).collect()
}

pub fn parse_users(payload: &Value) -> Vec<User> {
    let Some(obj) = payload.as_object() else {
        return Vec::new();
    };
    let Some(items) = obj.get("contacts").and_then(Value::as_array) else {
        return Vec::new();
    };
    items.iter().filter_map(parse_user).collect()
}

pub fn parse_messages(payload: &Value) -> Vec<Message> {
    let Some(obj) = payload.as_object() else {
        return Vec::new();
    };
    let Some(items) = obj.get("messages").and_then(Value::as_array) else {
        return Vec::new();
    };
    items.iter().filter_map(parse_message).collect()
}

pub fn ack_payload(chat_id: i64, message_id: i64) -> Value {
    json!({"chatId": chat_id, "messageId": message_id})
}

pub fn me_id(payload: &Value) -> Option<i64> {
    json_i64(
        payload
            .get("profile")
            .and_then(|profile| profile.get("contact"))
            .and_then(|contact| contact.get("id")),
    )
}

pub fn sync_time(payload: &Value) -> Option<i64> {
    json_i64(payload.get("time"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use serde_json::json;

    #[test]
    fn unwraps_message_event() {
        let payload = json!({
            "chatId": 55,
            "message": {
                "id": "10",
                "sender": 7,
                "text": "hi",
                "cid": 99,
                "time": 1,
                "type": "USER"
            }
        });
        let message = parse_message(&payload).unwrap();
        assert_eq!(message.id, 10);
        assert_eq!(message.chat_id, Some(55));
        assert_eq!(message.sender, Some(7));
        assert_eq!(message.text, "hi");
        assert_eq!(message.cid, Some(99));
        assert_eq!(message.reply_to, None);
    }

    #[test]
    fn reply_link_keeps_the_target_message() {
        let payload = json!({
            "chatId": 55,
            "message": {
                "id": 10,
                "sender": 7,
                "text": "hi",
                "link": {
                    "type": "REPLY",
                    "message": {"id": "42"}
                }
            }
        });
        assert_eq!(parse_message(&payload).unwrap().reply_to, Some(42));
        let forwarded = json!({
            "message": {
                "id": 11,
                "text": "fwd",
                "link": {"type": "FORWARD", "message": {"id": 7}}
            }
        });
        assert_eq!(parse_message(&forwarded).unwrap().reply_to, None);
    }

    #[test]
    fn send_payload_uses_camel_case() {
        let payload = send_payload(9, "hello", 100, None, &[]);
        assert_eq!(payload["chatId"], 9);
        assert_eq!(payload["message"]["text"], "hello");
        assert_eq!(payload["message"]["cid"], 100);
        assert!(payload["message"]["attaches"].as_array().unwrap().is_empty());
        assert!(payload["message"].get("link").is_none());
        let reply = send_payload(
            9,
            "hello",
            100,
            Some(42),
            &[
                Attach::Photo {
                    token: "pic".into(),
                },
                Attach::Voice {
                    audio_id: 7,
                    token: "aud".into(),
                    duration_ms: 1500,
                },
            ],
        );
        assert_eq!(reply["message"]["link"]["type"], "REPLY");
        assert_eq!(reply["message"]["link"]["messageId"], 42);
        assert_eq!(reply["message"]["attaches"][0]["_type"], "PHOTO");
        assert_eq!(reply["message"]["attaches"][0]["photoToken"], "pic");
        assert_eq!(reply["message"]["attaches"][1]["_type"], "AUDIO");
        assert_eq!(reply["message"]["attaches"][1]["audioId"], 7);
        assert_eq!(reply["message"]["attaches"][1]["duration"], 1500);
        assert!(reply["message"]["attaches"][1].get("wave").is_none());
    }

    #[test]
    fn photo_and_voice_come_from_attaches() {
        let payload = json!({
            "message": {
                "id": 1,
                "text": "",
                "attaches": [
                    {"_type": "PHOTO", "baseUrl": "https://cdn.example/a"},
                    {"_type": "VIDEO", "videoId": 1, "token": "vtok", "videoType": 1},
                    {"_type": "AUDIO", "url": "https://cdn.example/b", "duration": 1500},
                    {"_type": "AUDIO", "url": ""}
                ]
            }
        });
        let message = parse_message(&payload).unwrap();
        assert_eq!(
            message.media,
            vec![
                Media::Photo {
                    url: "https://cdn.example/a".into()
                },
                Media::Video {
                    video_id: 1,
                    token: "vtok".into(),
                    video_type: Some(1),
                    source_chat: None,
                    source_message: None,
                },
                Media::Voice {
                    url: "https://cdn.example/b".into(),
                    duration_ms: 1500
                }
            ]
        );
    }

    #[test]
    fn forwarded_video_note_comes_from_the_original_message() {
        let payload = json!({
            "chatId": 55,
            "message": {
                "id": 10,
                "sender": 7,
                "text": "",
                "attaches": [],
                "link": {
                    "type": "FORWARD",
                    "chatId": 99,
                    "message": {
                        "id": 3,
                        "text": "",
                        "attaches": [
                            {"_type": "VIDEO", "videoId": 8, "token": "v", "videoType": 1}
                        ]
                    }
                }
            }
        });
        let message = parse_message(&payload).unwrap();
        assert_eq!(message.text, "");
        assert_eq!(message.reply_to, None);
        assert!(message.forward.is_some());
        assert_eq!(
            message.media,
            vec![Media::Video {
                video_id: 8,
                token: "v".into(),
                video_type: Some(1),
                source_chat: Some(99),
                source_message: Some(3),
            }]
        );
    }

    #[test]
    fn forwarded_reply_uses_the_inner_message() {
        let payload = json!({
            "chatId": 55,
            "message": {
                "id": 10,
                "sender": 7,
                "text": "",
                "link": {
                    "type": "FORWARD",
                    "chatId": 99,
                    "chatName": "Аня Н",
                    "message": {
                        "id": 3,
                        "sender": 4,
                        "link": {"type": "REPLY", "message": {"id": "42"}},
                        "attaches": [
                            {"_type": "VIDEO", "videoId": 8, "token": "v", "videoType": 1}
                        ]
                    }
                }
            }
        });
        let message = parse_message(&payload).unwrap();
        assert_eq!(message.reply_to, Some(42));
        let forward = message.forward.unwrap();
        assert_eq!(forward.sender, Some(4));
        assert_eq!(forward.name.as_deref(), Some("Аня Н"));
        assert_eq!(message.media[0], Media::Video {
            video_id: 8,
            token: "v".into(),
            video_type: Some(1),
            source_chat: Some(99),
            source_message: Some(3),
        });
    }

    #[test]
    fn video_attach_sets_video_type_for_notes() {
        let note = send_payload(
            9,
            "",
            100,
            None,
            &[Attach::Video {
                video_id: 7,
                token: "tok".into(),
                video_type: Some(1),
            }],
        );
        assert_eq!(note["message"]["attaches"][0]["_type"], "VIDEO");
        assert_eq!(note["message"]["attaches"][0]["videoId"], 7);
        assert_eq!(note["message"]["attaches"][0]["token"], "tok");
        assert_eq!(note["message"]["attaches"][0]["videoType"], 1);
        let plain = send_payload(
            9,
            "",
            100,
            None,
            &[Attach::Video {
                video_id: 8,
                token: "tok2".into(),
                video_type: None,
            }],
        );
        assert!(plain["message"]["attaches"][0].get("videoType").is_none());
    }

    #[test]
    fn display_name_prefers_full_name() {
        let user = parse_user(&json!({
            "id": 1,
            "names": [{"firstName": "Ada", "lastName": "Lovelace", "name": "Ada Lovelace"}]
        }))
        .unwrap();
        assert_eq!(user.display_name(), "Ada Lovelace");
    }

    #[test]
    fn display_name_prefers_custom_name() {
        let user = parse_user(&json!({
            "id": 1,
            "names": [
                {"firstName": "Ada", "lastName": "", "type": "ONEME"},
                {"firstName": "Ada N", "lastName": "", "type": "CUSTOM"}
            ]
        }))
        .unwrap();
        assert_eq!(user.display_name(), "Ada N");
    }

    #[test]
    fn dialog_peers_come_from_participants() {
        let chat = parse_chat(&json!({
            "id": 9,
            "type": "DIALOG",
            "participants": {"5": 1, "8": 2}
        }))
        .unwrap();
        assert_eq!(chat.peers, vec![5, 8]);
        assert!(chat.title.is_none());
        assert!(chat.photo.is_none());
    }

    #[test]
    fn contact_photo_prefers_base_url() {
        let user = parse_user(&json!({
            "id": 8,
            "baseUrl": " https://i.oneme.ru/i?r=a ",
            "baseRawUrl": "https://i.oneme.ru/i?r=raw",
            "names": [{"name": "Маня Н", "type": "CUSTOM"}]
        }))
        .unwrap();
        assert_eq!(user.photo.as_deref(), Some("https://i.oneme.ru/i?r=a"));
        let raw = parse_user(&json!({
            "id": 8,
            "baseUrl": " ",
            "baseRawUrl": "https://i.oneme.ru/i?r=raw",
            "names": [{"name": "Маня Н", "type": "CUSTOM"}]
        }))
        .unwrap();
        assert_eq!(raw.photo.as_deref(), Some("https://i.oneme.ru/i?r=raw"));
        let empty = parse_user(&json!({
            "id": 8,
            "baseUrl": "",
            "baseRawUrl": "  ",
            "names": [{"name": "Маня Н", "type": "CUSTOM"}]
        }))
        .unwrap();
        assert!(empty.photo.is_none());
    }

    #[test]
    fn chat_icon_prefers_base_icon_url() {
        let chat = parse_chat(&json!({
            "id": 3,
            "type": "CHAT",
            "title": "Kitchen",
            "baseIconUrl": "https://i.oneme.ru/i?r=icon",
            "baseUrl": "https://i.oneme.ru/i?r=other",
            "iconUrl": "https://i.oneme.ru/i?r=legacy"
        }))
        .unwrap();
        assert_eq!(chat.photo.as_deref(), Some("https://i.oneme.ru/i?r=icon"));
        let fallback = parse_chat(&json!({
            "id": 3,
            "type": "CHAT",
            "iconUrl": "https://i.oneme.ru/i?r=legacy"
        }))
        .unwrap();
        assert_eq!(fallback.photo.as_deref(), Some("https://i.oneme.ru/i?r=legacy"));
    }

    #[test]
    fn code_response_returns_login_token() {
        let status = parse_code_status(&json!({
            "tokenAttrs": {"LOGIN": {"token": "login-token"}}
        }))
        .unwrap();
        assert_eq!(status, CodeStatus::Token("login-token".into()));
    }

    #[test]
    fn code_response_returns_password_challenge() {
        let status = parse_code_status(&json!({
            "passwordChallenge": {"trackId": "track-1", "hint": "pet"}
        }))
        .unwrap();
        assert_eq!(
            status,
            CodeStatus::Password {
                track_id: "track-1".into(),
                hint: Some("pet".into()),
            }
        );
    }

    #[test]
    fn code_response_returns_register_token() {
        let status = parse_code_status(&json!({
            "tokenAttrs": {"REGISTER": {"token": "register-token"}}
        }))
        .unwrap();
        assert_eq!(status, CodeStatus::Unregistered);
    }

    #[test]
    fn auth_mode_is_three_sha256_digests() {
        let mode = auth_mode("0011223344556677", 7);
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(mode)
            .unwrap();
        assert_eq!(bytes.len(), 96);
        let other = base64::engine::general_purpose::STANDARD
            .decode(auth_mode("0011223344556677", 8))
            .unwrap();
        assert_ne!(bytes, other);
    }

    #[test]
    fn phone_login_uses_the_android_user_agent() {
        let payload = login_payload("token", -1, -1, -1, -1, false);
        assert_eq!(payload["userAgent"]["deviceType"], "ANDROID");
        assert_eq!(payload["userAgent"]["appVersion"], "26.25.0");
    }

    #[test]
    fn call_wait_is_reported_in_seconds() {
        let payload = json!({
            "token": "token",
            "codeLength": 6,
            "requestMaxDuration": 60000,
        });
        let challenge = parse_code_challenge(&payload).unwrap();
        assert_eq!(challenge.wait_secs, 60);
        assert_eq!(challenge.code_length, 6);
    }
}
