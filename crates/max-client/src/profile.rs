// Chrome/home session extract stays for tests and recovery; the bridge signs in via the web page.
#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};

use crate::crypto::{decode_local_state_key, decrypt_chrome, decrypt_with_key};
use crate::error::{Error, Result};

#[derive(Clone, Serialize, Deserialize)]
pub struct Session {
    pub token: String,
    pub device_id: String,
    #[serde(default)]
    pub instance_id: String,
    #[serde(default = "sync_default")]
    pub chats_sync: i64,
    #[serde(default = "sync_default")]
    pub contacts_sync: i64,
    #[serde(default = "sync_default")]
    pub drafts_sync: i64,
    #[serde(default = "sync_default")]
    pub presence_sync: i64,
    #[serde(default)]
    pub desktop: bool,
}

fn sync_default() -> i64 {
    -1
}

impl Session {
    pub fn signed_in(token: String, device_id: String, instance_id: String) -> Self {
        Self {
            token,
            device_id,
            instance_id,
            chats_sync: sync_default(),
            contacts_sync: sync_default(),
            drafts_sync: sync_default(),
            presence_sync: sync_default(),
            desktop: false,
        }
    }
}

pub fn new_device_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0);
    let mixed = nanos ^ u64::from(std::process::id()).wrapping_mul(0x9E37_79B9);
    format!("{mixed:016x}")
}

impl std::fmt::Debug for Session {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Session")
            .field("device_id", &self.device_id)
            .field("chats_sync", &self.chats_sync)
            .finish()
    }
}

pub fn load_or_extract(store: &crate::store::Store, home: &Path) -> Result<Session> {
    if let Some(session) = store.read_session()? {
        return Ok(session);
    }
    let session = extract_from_home(home)?;
    store.write_session(&session)?;
    Ok(session)
}

pub fn extract_from_home(home: &Path) -> Result<Session> {
    let mut roots = Vec::new();
    for relative in [".config", ".local/share"] {
        let path = home.join(relative);
        if path.is_dir() {
            roots.push(path);
        }
    }
    let mut profiles = Vec::new();
    for root in &roots {
        collect_profiles(root, 0, &mut profiles);
    }
    profiles.sort_by_key(|path| score_profile(path));
    profiles.reverse();

    let mut passwords = vec![b"peanuts".to_vec()];
    passwords.extend(keyring_passwords());

    for profile in profiles {
        if let Some(session) = extract_profile(&profile, &passwords) {
            return Ok(session);
        }
    }
    if let Some(session) = extract_oneme(home) {
        return Ok(session);
    }
    Err(Error::SessionNotFound)
}

fn extract_oneme(home: &Path) -> Option<Session> {
    let token = keyring_access_token()?;
    let device_id = device_id_from_logs(home)?;
    if !valid_token(&token) || !valid_device(&device_id) {
        return None;
    }
    Some(Session {
        token,
        device_id,
        instance_id: String::new(),
        chats_sync: 0,
        contacts_sync: 0,
        drafts_sync: 0,
        presence_sync: 0,
        desktop: true,
    })
}

fn device_id_from_logs(home: &Path) -> Option<String> {
    let dir = home.join(".local/share/ONEME/logs");
    let mut files: Vec<PathBuf> = fs::read_dir(dir)
        .ok()?
        .flatten()
        .map(|entry| entry.path())
        .collect();
    files.sort_by_key(|path| {
        fs::metadata(path)
            .and_then(|meta| meta.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
    });
    let mut found = None;
    for path in files {
        let Ok(bytes) = fs::read(&path) else {
            continue;
        };
        let text = String::from_utf8_lossy(&bytes);
        if let Some(device_id) = last_device_id(&text) {
            found = Some(device_id);
        }
    }
    found
}

fn last_device_id(text: &str) -> Option<String> {
    let mut rest = text;
    let mut found = None;
    while let Some(at) = rest.find("\"deviceId\"") {
        let after = &rest[at + "\"deviceId\"".len()..];
        if let Some(value) = read_json_string(after) {
            if valid_device(&value) {
                found = Some(value);
            }
        }
        rest = &rest[at + "\"deviceId\"".len()..];
    }
    found
}

fn score_profile(path: &Path) -> i32 {
    let text = path.to_string_lossy().to_ascii_lowercase();
    let mut score = 0;
    if text.contains("max") {
        score += 10;
    }
    if text.contains("oneme") {
        score += 5;
    }
    score
}

fn collect_profiles(dir: &Path, depth: usize, out: &mut Vec<PathBuf>) {
    if depth > 5 {
        return;
    }
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    let mut saw_state = false;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_profiles(&path, depth + 1, out);
            continue;
        }
        if entry.file_name() == "Local State" || entry.file_name() == "Cookies" {
            saw_state = true;
        }
    }
    if saw_state {
        out.push(dir.to_path_buf());
    }
}

fn extract_profile(profile: &Path, passwords: &[Vec<u8>]) -> Option<Session> {
    let mut blobs = Vec::new();
    collect_text_files(profile, 0, &mut blobs);
    let mut best: Option<(usize, String, String)> = None;
    for (path, bytes) in &blobs {
        let texts = decode_texts(bytes);
        for text in texts {
            if let Some((distance, device_id, token)) = nearest_pair(&text) {
                let bonus = if path_bonus(path) { 0 } else { 10_000 };
                let rank = distance + bonus;
                if best.as_ref().map(|(old, _, _)| rank < *old).unwrap_or(true) {
                    best = Some((rank, device_id, token));
                }
            }
        }
    }
    if let Some((_, device_id, token)) = best {
        return Some(fresh(token, device_id));
    }

    let cookies = profile.join("Cookies");
    let network_cookies = profile.join("Network").join("Cookies");
    for cookie_db in [cookies, network_cookies] {
        if let Some(session) = session_from_cookies(&cookie_db, passwords) {
            return Some(session);
        }
    }
    None
}

fn path_bonus(path: &Path) -> bool {
    let text = path.to_string_lossy().to_ascii_lowercase();
    text.contains("max") || text.contains("oneme")
}

fn fresh(token: String, device_id: String) -> Session {
    Session {
        token,
        device_id,
        instance_id: String::new(),
        chats_sync: -1,
        contacts_sync: -1,
        drafts_sync: -1,
        presence_sync: -1,
        desktop: false,
    }
}

fn collect_text_files(dir: &Path, depth: usize, out: &mut Vec<(PathBuf, Vec<u8>)>) {
    if depth > 4 {
        return;
    }
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            let name = entry.file_name().to_string_lossy().to_ascii_lowercase();
            if name.contains("cache") || name == "code cache" || name == "gpuCache" {
                continue;
            }
            collect_text_files(&path, depth + 1, out);
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_ascii_lowercase();
        let interesting = name.contains("local storage")
            || name.ends_with(".ldb")
            || name.ends_with(".log")
            || name == "local state"
            || name == "preferences"
            || name.contains("session storage")
            || name.contains("indexeddb");
        if !interesting && !path_bonus(&path) {
            continue;
        }
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        if meta.len() == 0 || meta.len() > 32 * 1024 * 1024 {
            continue;
        }
        if let Ok(bytes) = fs::read(&path) {
            out.push((path, bytes));
        }
    }
}

fn decode_texts(bytes: &[u8]) -> Vec<String> {
    let mut texts = vec![String::from_utf8_lossy(bytes).into_owned()];
    if looks_utf16(bytes) {
        texts.push(decode_utf16_le(bytes));
    }
    texts
}

fn looks_utf16(bytes: &[u8]) -> bool {
    if bytes.starts_with(&[0xFF, 0xFE]) {
        return true;
    }
    let sample = &bytes[..bytes.len().min(400)];
    if sample.len() < 20 {
        return false;
    }
    let zeros = sample
        .iter()
        .skip(1)
        .step_by(2)
        .filter(|byte| **byte == 0)
        .count();
    zeros * 4 > sample.len()
}

fn decode_utf16_le(bytes: &[u8]) -> String {
    let start = if bytes.starts_with(&[0xFF, 0xFE]) {
        2
    } else {
        0
    };
    let units: Vec<u16> = bytes[start..]
        .chunks_exact(2)
        .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
        .collect();
    String::from_utf16_lossy(&units)
}

fn nearest_pair(text: &str) -> Option<(usize, String, String)> {
    let devices = json_strings(text, &["deviceId", "device_id"]);
    let tokens = json_strings(text, &["token", "authToken", "auth_token"]);
    let mut best: Option<(usize, String, String)> = None;
    for (device_at, device_id) in &devices {
        if !valid_device(device_id) {
            continue;
        }
        for (token_at, token) in &tokens {
            if !valid_token(token) {
                continue;
            }
            let distance = device_at.abs_diff(*token_at);
            if distance > 8192 {
                continue;
            }
            if best
                .as_ref()
                .map(|(old, _, _)| distance < *old)
                .unwrap_or(true)
            {
                best = Some((distance, device_id.clone(), token.clone()));
            }
        }
    }
    best
}

fn valid_device(value: &str) -> bool {
    let len = value.len();
    (8..=80).contains(&len)
        && !value.contains('*')
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '+' | '/' | '='))
}

fn valid_token(value: &str) -> bool {
    let len = value.len();
    (16..=4096).contains(&len)
        && !value.contains("http")
        && value.chars().all(|ch| {
            ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '~' | '+' | '/' | '=')
        })
}

fn json_strings(text: &str, keys: &[&str]) -> Vec<(usize, String)> {
    let mut found = Vec::new();
    let bytes = text.as_bytes();
    for key in keys {
        let pattern = format!("\"{key}\"");
        let mut from = 0;
        while let Some(rel) = text[from..].find(&pattern) {
            let at = from + rel;
            let after = at + pattern.len();
            if let Some(value) = read_json_string(&text[after..]) {
                found.push((at, value));
            }
            from = after;
            if from >= bytes.len() {
                break;
            }
        }
    }
    found
}

fn read_json_string(after_key: &str) -> Option<String> {
    let trimmed = after_key.trim_start();
    let rest = trimmed.strip_prefix(':')?.trim_start();
    let rest = rest.strip_prefix('"')?;
    let mut out = String::new();
    let mut chars = rest.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            if let Some(escaped) = chars.next() {
                out.push(escaped);
            }
            continue;
        }
        if ch == '"' {
            return Some(out);
        }
        out.push(ch);
    }
    None
}

fn session_from_cookies(db: &Path, passwords: &[Vec<u8>]) -> Option<Session> {
    if !db.is_file() {
        return None;
    }
    let rows = read_cookie_rows(db)?;
    let mut keys = Vec::new();
    if let Some(state) = local_state_key(db.parent()?, passwords) {
        keys.push(state);
    }
    for password in passwords {
        keys.push(crate::crypto::derive_key(password));
    }
    let mut blob = String::new();
    for row in rows {
        if !row.value.is_empty() {
            blob.push_str(&row.name);
            blob.push('=');
            blob.push_str(&row.value);
            blob.push('\n');
        }
        if row.encrypted.is_empty() {
            continue;
        }
        for key in &keys {
            if let Some(plain) = decrypt_with_key(key, &row.encrypted) {
                if let Ok(text) = String::from_utf8(plain) {
                    blob.push_str(&row.name);
                    blob.push('=');
                    blob.push_str(&text);
                    blob.push('\n');
                }
            }
        }
    }
    let (_, device_id, token) = nearest_pair(&blob).or_else(|| pair_from_cookie_names(&blob))?;
    Some(fresh(token, device_id))
}

fn pair_from_cookie_names(blob: &str) -> Option<(usize, String, String)> {
    let mut device_id = None;
    let mut token = None;
    for line in blob.lines() {
        let Some((name, value)) = line.split_once('=') else {
            continue;
        };
        let name = name.to_ascii_lowercase();
        if device_id.is_none() && name.contains("device") && valid_device(value) {
            device_id = Some(value.to_owned());
        }
        if token.is_none() && name.contains("token") && valid_token(value) {
            token = Some(value.to_owned());
        }
    }
    Some((0, device_id?, token?))
}

struct CookieRow {
    name: String,
    value: String,
    encrypted: Vec<u8>,
}

fn read_cookie_rows(db: &Path) -> Option<Vec<CookieRow>> {
    let script = r#"
import json, sqlite3, sys
con = sqlite3.connect(sys.argv[1])
rows = []
try:
    query = "select name, value, hex(encrypted_value) from cookies"
    for name, value, enc in con.execute(query):
        rows.append({"name": name or "", "value": value or "", "enc": enc or ""})
except sqlite3.Error:
    pass
print(json.dumps(rows))
"#;
    let output = Command::new("python3")
        .arg("-c")
        .arg(script)
        .arg(db)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let parsed: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
    let items = parsed.as_array()?;
    let mut rows = Vec::new();
    for item in items {
        let name = item.get("name")?.as_str()?.to_owned();
        let value = item.get("value")?.as_str()?.to_owned();
        let enc = item.get("enc")?.as_str().unwrap_or("");
        let encrypted = hex_decode(enc);
        rows.push(CookieRow {
            name,
            value,
            encrypted,
        });
    }
    Some(rows)
}

fn hex_decode(text: &str) -> Vec<u8> {
    if text.len() % 2 != 0 {
        return Vec::new();
    }
    (0..text.len())
        .step_by(2)
        .filter_map(|index| u8::from_str_radix(&text[index..index + 2], 16).ok())
        .collect()
}

fn local_state_key(profile: &Path, passwords: &[Vec<u8>]) -> Option<[u8; 16]> {
    let raw = fs::read_to_string(profile.join("Local State")).ok()?;
    let value: serde_json::Value = serde_json::from_str(&raw).ok()?;
    let encoded = value.get("os_crypt")?.get("encrypted_key")?.as_str()?;
    for password in passwords {
        if let Some(key) = decode_local_state_key(password, encoded) {
            return Some(key);
        }
        let _ = decrypt_chrome(password, &[]);
    }
    None
}

fn keyring_access_token() -> Option<String> {
    let script = r#"
import sys
try:
    from gi.repository import Gio, GLib
except Exception:
    sys.exit(0)
bus = Gio.bus_get_sync(Gio.BusType.SESSION, None)
def call(path, interface, method, params, reply):
    return bus.call_sync(
        "org.freedesktop.secrets",
        path,
        interface,
        method,
        params,
        reply,
        Gio.DBusCallFlags.NONE,
        8000,
        None,
    )
opened = call(
    "/org/freedesktop/secrets",
    "org.freedesktop.Secret.Service",
    "OpenSession",
    GLib.Variant("(sv)", ("plain", GLib.Variant("s", ""))),
    GLib.VariantType("(vo)"),
).unpack()
session = opened[1]
found = call(
    "/org/freedesktop/secrets",
    "org.freedesktop.Secret.Service",
    "SearchItems",
    GLib.Variant("(a{ss})", ({"key": "access_token"},)),
    GLib.VariantType("(aoao)"),
).unpack()
paths = list(found[0]) + list(found[1])
if not paths:
    sys.exit(0)
secrets = call(
    "/org/freedesktop/secrets",
    "org.freedesktop.Secret.Service",
    "GetSecrets",
    GLib.Variant("(aoo)", (paths, session)),
    GLib.VariantType("(a{o(oayays)})"),
).unpack()[0]
for secret in secrets.values():
    raw = bytes(secret[2])
    if raw:
        sys.stdout.buffer.write(raw)
        break
"#;
    let output = Command::new("python3")
        .arg("-c")
        .arg(script)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let token = String::from_utf8(output.stdout).ok()?;
    let token = token.trim();
    if valid_token(token) {
        Some(token.to_owned())
    } else {
        None
    }
}

fn keyring_passwords() -> Vec<Vec<u8>> {
    let script = r#"
import sys
try:
    from gi.repository import Gio, GLib
except Exception:
    sys.exit(0)
bus = Gio.bus_get_sync(Gio.BusType.SESSION, None)
def call(path, interface, method, params, reply):
    return bus.call_sync(
        "org.freedesktop.secrets",
        path,
        interface,
        method,
        params,
        reply,
        Gio.DBusCallFlags.NONE,
        8000,
        None,
    )
opened = call(
    "/org/freedesktop/secrets",
    "org.freedesktop.Secret.Service",
    "OpenSession",
    GLib.Variant("(sv)", ("plain", GLib.Variant("s", ""))),
    GLib.VariantType("(vo)"),
).unpack()
session = opened[1]
found = call(
    "/org/freedesktop/secrets",
    "org.freedesktop.Secret.Service",
    "SearchItems",
    GLib.Variant("(a{ss})", ({},)),
    GLib.VariantType("(aoao)"),
).unpack()
paths = list(found[0]) + list(found[1])
if not paths:
    sys.exit(0)
secrets = call(
    "/org/freedesktop/secrets",
    "org.freedesktop.Secret.Service",
    "GetSecrets",
    GLib.Variant("(aoo)", (paths, session)),
    GLib.VariantType("(a{o(oayays)})"),
).unpack()[0]
for item, secret in secrets.items():
    label = ""
    try:
        props = call(
            item,
            "org.freedesktop.DBus.Properties",
            "Get",
            GLib.Variant("(ss)", ("org.freedesktop.Secret.Item", "Label")),
            GLib.VariantType("(v)"),
        ).unpack()[0]
        label = str(props)
    except Exception:
        pass
    if "Safe Storage" not in label and "safe storage" not in label.lower():
        continue
    raw = bytes(secret[2])
    sys.stdout.buffer.write(raw + b"\n")
"#;
    let output = Command::new("python3").arg("-c").arg(script).output();
    let Ok(output) = output else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    output
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| line.to_vec())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn reads_token_next_to_device_id() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("max-profile-{nanos}"));
        let profile = dir.join(".config").join("max");
        let storage = profile.join("Local Storage").join("leveldb");
        fs::create_dir_all(&storage).unwrap();
        fs::write(profile.join("Local State"), "{}").unwrap();
        fs::write(
            storage.join("000001.log"),
            r#"{"deviceId":"aabbccddeeff0011","token":"login-token-value-1"}"#,
        )
        .unwrap();
        let session = extract_from_home(&dir).unwrap();
        assert_eq!(session.device_id, "aabbccddeeff0011");
        assert_eq!(session.token, "login-token-value-1");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn reads_desktop_device_id_from_a_log_line() {
        let text = r#"Code: 0x06 Parameters: {"deviceId":"QUJDREVGR0hJSktMTU5PUFFSU1Q=","clientSessionId":1}"#;
        assert_eq!(
            last_device_id(text).as_deref(),
            Some("QUJDREVGR0hJSktMTU5PUFFSU1Q=")
        );
    }
}
