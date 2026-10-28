use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::profile::Session;

const SCHEMA_VERSION: &str = "1";

pub struct Store {
    conn: Mutex<Connection>,
    path: PathBuf,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Cursor {
    pub time: i64,
    pub id: i64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Topic {
    pub thread_id: i64,
    pub name: String,
    #[serde(default)]
    pub photo: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Links {
    #[serde(default)]
    pub tg: HashMap<i64, i64>,
    #[serde(default)]
    pub max: HashMap<i64, i64>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ReceiptStatus {
    #[default]
    Sent,
    Arrived,
    Read,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OutReceipt {
    pub tg_chat: i64,
    pub tg_message: i64,
    pub max_chat: i64,
    pub time: i64,
    #[serde(default)]
    pub status: ReceiptStatus,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct InReceipt {
    pub max_chat: i64,
    pub max_message: i64,
    pub time: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PendingMark {
    pub max_chat: i64,
    pub max_message: i64,
    pub mark: i64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Receipts {
    #[serde(default)]
    pub out: HashMap<i64, OutReceipt>,
    #[serde(default)]
    pub inbound: HashMap<i64, InReceipt>,
    #[serde(default)]
    pub pending_marks: Vec<PendingMark>,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(&path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        let store = Self {
            conn: Mutex::new(conn),
            path,
        };
        store.init_schema()?;
        store.migrate_legacy_json()?;
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn with_conn<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| Error::Protocol("store lock poisoned".into()))?;
        f(&conn)
    }

    fn init_schema(&self) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY NOT NULL,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS session (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    token TEXT NOT NULL,
                    device_id TEXT NOT NULL,
                    instance_id TEXT NOT NULL DEFAULT '',
                    chats_sync INTEGER NOT NULL DEFAULT -1,
                    contacts_sync INTEGER NOT NULL DEFAULT -1,
                    drafts_sync INTEGER NOT NULL DEFAULT -1,
                    presence_sync INTEGER NOT NULL DEFAULT -1,
                    desktop INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS names (
                    user_id INTEGER PRIMARY KEY NOT NULL,
                    name TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS contacts (
                    user_id INTEGER PRIMARY KEY NOT NULL
                );
                CREATE TABLE IF NOT EXISTS chat_map (
                    tg_chat INTEGER PRIMARY KEY NOT NULL,
                    max_chat INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS links_tg (
                    tg_message INTEGER PRIMARY KEY NOT NULL,
                    max_message INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS links_max (
                    max_message INTEGER PRIMARY KEY NOT NULL,
                    tg_message INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS topics (
                    key TEXT PRIMARY KEY NOT NULL,
                    thread_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    photo INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS seen (
                    chat_id INTEGER PRIMARY KEY NOT NULL,
                    time INTEGER NOT NULL,
                    id INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS receipts_out (
                    max_message INTEGER PRIMARY KEY NOT NULL,
                    tg_chat INTEGER NOT NULL,
                    tg_message INTEGER NOT NULL,
                    max_chat INTEGER NOT NULL,
                    time INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'sent'
                );
                CREATE TABLE IF NOT EXISTS receipts_inbound (
                    tg_message INTEGER PRIMARY KEY NOT NULL,
                    max_chat INTEGER NOT NULL,
                    max_message INTEGER NOT NULL,
                    time INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS pending_marks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    max_chat INTEGER NOT NULL,
                    max_message INTEGER NOT NULL,
                    mark INTEGER NOT NULL
                );
                "#,
            )?;
            Ok(())
        })
    }

    fn meta_get(conn: &Connection, key: &str) -> Result<Option<String>> {
        let value = conn
            .query_row(
                "SELECT value FROM meta WHERE key = ?1",
                params![key],
                |row| row.get(0),
            )
            .optional()?;
        Ok(value)
    }

    fn meta_set(conn: &Connection, key: &str, value: &str) -> Result<()> {
        conn.execute(
            "INSERT INTO meta(key, value) VALUES(?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn read_session(&self) -> Result<Option<Session>> {
        self.with_conn(|conn| {
            let row = conn
                .query_row(
                    "SELECT token, device_id, instance_id, chats_sync, contacts_sync,
                            drafts_sync, presence_sync, desktop
                     FROM session WHERE id = 1",
                    [],
                    |row| {
                        Ok(Session {
                            token: row.get(0)?,
                            device_id: row.get(1)?,
                            instance_id: row.get(2)?,
                            chats_sync: row.get(3)?,
                            contacts_sync: row.get(4)?,
                            drafts_sync: row.get(5)?,
                            presence_sync: row.get(6)?,
                            desktop: row.get::<_, i64>(7)? != 0,
                        })
                    },
                )
                .optional()?;
            Ok(match row {
                Some(session) if !session.token.is_empty() && !session.device_id.is_empty() => {
                    Some(session)
                }
                _ => None,
            })
        })
    }

    pub fn write_session(&self, session: &Session) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO session(
                    id, token, device_id, instance_id, chats_sync, contacts_sync,
                    drafts_sync, presence_sync, desktop
                 ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(id) DO UPDATE SET
                    token = excluded.token,
                    device_id = excluded.device_id,
                    instance_id = excluded.instance_id,
                    chats_sync = excluded.chats_sync,
                    contacts_sync = excluded.contacts_sync,
                    drafts_sync = excluded.drafts_sync,
                    presence_sync = excluded.presence_sync,
                    desktop = excluded.desktop",
                params![
                    session.token,
                    session.device_id,
                    session.instance_id,
                    session.chats_sync,
                    session.contacts_sync,
                    session.drafts_sync,
                    session.presence_sync,
                    if session.desktop { 1 } else { 0 },
                ],
            )?;
            Ok(())
        })
    }

    pub fn clear_session(&self) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute("DELETE FROM session", [])?;
            Ok(())
        })
    }

    pub fn load_names(&self) -> Result<HashMap<i64, String>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT user_id, name FROM names")?;
            let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
            let mut names = HashMap::new();
            for row in rows {
                let (id, name) = row?;
                names.insert(id, name);
            }
            Ok(names)
        })
    }

    pub fn save_names(&self, names: &HashMap<i64, String>) -> Result<()> {
        self.with_conn(|conn| {
            let tx = conn.unchecked_transaction()?;
            tx.execute("DELETE FROM names", [])?;
            {
                let mut stmt = tx.prepare("INSERT INTO names(user_id, name) VALUES(?1, ?2)")?;
                for (user_id, name) in names {
                    if name.is_empty() {
                        continue;
                    }
                    stmt.execute(params![user_id, name])?;
                }
            }
            tx.commit()?;
            Ok(())
        })
    }

    pub fn merge_names_from_users(&self, users: &HashMap<i64, crate::models::User>) -> Result<()> {
        let mut names = self.load_names()?;
        let mut changed = false;
        for user in users.values() {
            let name = user.display_name();
            if name.is_empty() {
                continue;
            }
            if names.get(&user.id).map(String::as_str) != Some(name.as_str()) {
                names.insert(user.id, name);
                changed = true;
            }
        }
        if !changed {
            return Ok(());
        }
        self.save_names(&names)
    }

    pub fn load_contacts(&self) -> Result<HashSet<i64>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT user_id FROM contacts")?;
            let rows = stmt.query_map([], |row| row.get(0))?;
            let mut ids = HashSet::new();
            for row in rows {
                let id: i64 = row?;
                if id != 0 {
                    ids.insert(id);
                }
            }
            Ok(ids)
        })
    }

    pub fn save_contacts(&self, ids: &HashSet<i64>) -> Result<()> {
        let mut list: Vec<i64> = ids.iter().copied().filter(|id| *id != 0).collect();
        list.sort_unstable();
        let current = {
            let mut loaded: Vec<i64> = self.load_contacts()?.into_iter().collect();
            loaded.sort_unstable();
            loaded
        };
        if list == current {
            return Ok(());
        }
        self.with_conn(|conn| {
            let tx = conn.unchecked_transaction()?;
            tx.execute("DELETE FROM contacts", [])?;
            {
                let mut stmt = tx.prepare("INSERT INTO contacts(user_id) VALUES(?1)")?;
                for id in &list {
                    stmt.execute(params![id])?;
                }
            }
            tx.commit()?;
            Ok(())
        })
    }

    pub fn load_map(&self) -> HashMap<i64, i64> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT tg_chat, max_chat FROM chat_map")?;
            let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
            let mut map = HashMap::new();
            for row in rows {
                let (tg, max) = row?;
                map.insert(tg, max);
            }
            Ok(map)
        })
        .unwrap_or_default()
    }

    pub fn save_map(&self, map: &HashMap<i64, i64>) {
        let _ = self.with_conn(|conn| {
            let tx = conn.unchecked_transaction()?;
            tx.execute("DELETE FROM chat_map", [])?;
            {
                let mut stmt =
                    tx.prepare("INSERT INTO chat_map(tg_chat, max_chat) VALUES(?1, ?2)")?;
                for (tg, max) in map {
                    stmt.execute(params![tg, max])?;
                }
            }
            tx.commit()?;
            Ok(())
        });
    }

    pub fn load_links(&self) -> Links {
        self.with_conn(|conn| {
            let mut links = Links::default();
            {
                let mut stmt = conn.prepare("SELECT tg_message, max_message FROM links_tg")?;
                let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
                for row in rows {
                    let (tg, max) = row?;
                    links.tg.insert(tg, max);
                }
            }
            {
                let mut stmt = conn.prepare("SELECT max_message, tg_message FROM links_max")?;
                let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
                for row in rows {
                    let (max, tg) = row?;
                    links.max.insert(max, tg);
                }
            }
            Ok(links)
        })
        .unwrap_or_default()
    }

    pub fn save_links(&self, links: &Links) {
        let _ = self.with_conn(|conn| {
            let tx = conn.unchecked_transaction()?;
            tx.execute("DELETE FROM links_tg", [])?;
            tx.execute("DELETE FROM links_max", [])?;
            {
                let mut stmt =
                    tx.prepare("INSERT INTO links_tg(tg_message, max_message) VALUES(?1, ?2)")?;
                for (tg, max) in &links.tg {
                    stmt.execute(params![tg, max])?;
                }
            }
            {
                let mut stmt =
                    tx.prepare("INSERT INTO links_max(max_message, tg_message) VALUES(?1, ?2)")?;
                for (max, tg) in &links.max {
                    stmt.execute(params![max, tg])?;
                }
            }
            tx.commit()?;
            Ok(())
        });
    }

    pub fn load_topics(&self) -> HashMap<String, Topic> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT key, thread_id, name, photo FROM topics")?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    Topic {
                        thread_id: row.get(1)?,
                        name: row.get(2)?,
                        photo: row.get::<_, i64>(3)? != 0,
                    },
                ))
            })?;
            let mut topics = HashMap::new();
            for row in rows {
                let (key, topic) = row?;
                topics.insert(key, topic);
            }
            Ok(topics)
        })
        .unwrap_or_default()
    }

    pub fn save_topics(&self, topics: &HashMap<String, Topic>) {
        let _ = self.with_conn(|conn| {
            let tx = conn.unchecked_transaction()?;
            tx.execute("DELETE FROM topics", [])?;
            {
                let mut stmt = tx.prepare(
                    "INSERT INTO topics(key, thread_id, name, photo) VALUES(?1, ?2, ?3, ?4)",
                )?;
                for (key, topic) in topics {
                    stmt.execute(params![
                        key,
                        topic.thread_id,
                        topic.name,
                        if topic.photo { 1 } else { 0 },
                    ])?;
                }
            }
            tx.commit()?;
            Ok(())
        });
    }

    pub fn load_seen(&self) -> HashMap<i64, Cursor> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT chat_id, time, id FROM seen")?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get(0)?,
                    Cursor {
                        time: row.get(1)?,
                        id: row.get(2)?,
                    },
                ))
            })?;
            let mut seen = HashMap::new();
            for row in rows {
                let (chat_id, cursor) = row?;
                seen.insert(chat_id, cursor);
            }
            Ok(seen)
        })
        .unwrap_or_default()
    }

    pub fn save_seen(&self, seen: &HashMap<i64, Cursor>) {
        let _ = self.with_conn(|conn| {
            let tx = conn.unchecked_transaction()?;
            tx.execute("DELETE FROM seen", [])?;
            {
                let mut stmt =
                    tx.prepare("INSERT INTO seen(chat_id, time, id) VALUES(?1, ?2, ?3)")?;
                for (chat_id, cursor) in seen {
                    stmt.execute(params![chat_id, cursor.time, cursor.id])?;
                }
            }
            tx.commit()?;
            Ok(())
        });
    }

    pub fn load_offset(&self) -> i64 {
        self.with_conn(|conn| {
            Ok(Self::meta_get(conn, "tg_offset")?
                .and_then(|raw| raw.trim().parse().ok())
                .unwrap_or(0))
        })
        .unwrap_or(0)
    }

    pub fn save_offset(&self, offset: i64) {
        let _ = self.with_conn(|conn| Self::meta_set(conn, "tg_offset", &offset.to_string()));
    }

    pub fn load_receipts(&self) -> Receipts {
        self.with_conn(|conn| {
            let mut receipts = Receipts::default();
            {
                let mut stmt = conn.prepare(
                    "SELECT max_message, tg_chat, tg_message, max_chat, time, status
                     FROM receipts_out",
                )?;
                let rows = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        OutReceipt {
                            tg_chat: row.get(1)?,
                            tg_message: row.get(2)?,
                            max_chat: row.get(3)?,
                            time: row.get(4)?,
                            status: parse_status(&row.get::<_, String>(5)?),
                        },
                    ))
                })?;
                for row in rows {
                    let (key, value) = row?;
                    receipts.out.insert(key, value);
                }
            }
            {
                let mut stmt = conn.prepare(
                    "SELECT tg_message, max_chat, max_message, time FROM receipts_inbound",
                )?;
                let rows = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        InReceipt {
                            max_chat: row.get(1)?,
                            max_message: row.get(2)?,
                            time: row.get(3)?,
                        },
                    ))
                })?;
                for row in rows {
                    let (key, value) = row?;
                    receipts.inbound.insert(key, value);
                }
            }
            {
                let mut stmt = conn
                    .prepare("SELECT max_chat, max_message, mark FROM pending_marks ORDER BY id")?;
                let rows = stmt.query_map([], |row| {
                    Ok(PendingMark {
                        max_chat: row.get(0)?,
                        max_message: row.get(1)?,
                        mark: row.get(2)?,
                    })
                })?;
                for row in rows {
                    receipts.pending_marks.push(row?);
                }
            }
            Ok(receipts)
        })
        .unwrap_or_default()
    }

    pub fn save_receipts(&self, receipts: &Receipts) {
        let _ = self.with_conn(|conn| {
            let tx = conn.unchecked_transaction()?;
            tx.execute("DELETE FROM receipts_out", [])?;
            tx.execute("DELETE FROM receipts_inbound", [])?;
            tx.execute("DELETE FROM pending_marks", [])?;
            {
                let mut stmt = tx.prepare(
                    "INSERT INTO receipts_out(
                        max_message, tg_chat, tg_message, max_chat, time, status
                     ) VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
                )?;
                for (max_message, item) in &receipts.out {
                    stmt.execute(params![
                        max_message,
                        item.tg_chat,
                        item.tg_message,
                        item.max_chat,
                        item.time,
                        status_name(item.status),
                    ])?;
                }
            }
            {
                let mut stmt = tx.prepare(
                    "INSERT INTO receipts_inbound(
                        tg_message, max_chat, max_message, time
                     ) VALUES(?1, ?2, ?3, ?4)",
                )?;
                for (tg_message, item) in &receipts.inbound {
                    stmt.execute(params![
                        tg_message,
                        item.max_chat,
                        item.max_message,
                        item.time,
                    ])?;
                }
            }
            {
                let mut stmt = tx.prepare(
                    "INSERT INTO pending_marks(max_chat, max_message, mark) VALUES(?1, ?2, ?3)",
                )?;
                for item in &receipts.pending_marks {
                    stmt.execute(params![item.max_chat, item.max_message, item.mark])?;
                }
            }
            tx.commit()?;
            Ok(())
        });
    }

    fn migrate_legacy_json(&self) -> Result<()> {
        let already = self.with_conn(|conn| Self::meta_get(conn, "schema_version"))?;
        if already.as_deref() == Some(SCHEMA_VERSION) {
            return Ok(());
        }
        let dir = self
            .path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();

        let session_path = legacy_path(&dir, "MAX_SESSION_PATH", "session.json");
        let map_path = legacy_path(&dir, "MAX_MAP_PATH", "map.json");
        let links_path = legacy_path(&dir, "MAX_LINKS_PATH", "links.json");
        let receipts_path = legacy_path(&dir, "MAX_RECEIPTS_PATH", "receipts.json");
        let topics_path = legacy_path(&dir, "MAX_TOPICS_PATH", "topics.json");
        let offset_path = legacy_path(&dir, "MAX_OFFSET_PATH", "tg-offset");
        let seen_path = legacy_path(&dir, "MAX_SEEN_PATH", "seen.json");
        let names_path = dir.join("names.json");
        let contacts_path = dir.join("contacts.json");

        let mut imported_any = false;

        if let Some(session) = read_json_file::<Session>(&session_path) {
            if !session.token.is_empty() && !session.device_id.is_empty() {
                self.write_session(&session)?;
                imported_any = true;
            }
        }
        if let Some(names) = read_json_file::<HashMap<i64, String>>(&names_path) {
            if !names.is_empty() {
                self.save_names(&names)?;
                imported_any = true;
            }
        }
        if let Some(ids) = read_json_file::<Vec<i64>>(&contacts_path) {
            let set: HashSet<i64> = ids.into_iter().filter(|id| *id != 0).collect();
            if !set.is_empty() {
                self.save_contacts(&set)?;
                imported_any = true;
            }
        }
        if let Some(map) = read_json_file::<HashMap<i64, i64>>(&map_path) {
            if !map.is_empty() {
                self.save_map(&map);
                imported_any = true;
            }
        }
        if let Some(links) = read_json_file::<Links>(&links_path) {
            if !links.tg.is_empty() || !links.max.is_empty() {
                self.save_links(&links);
                imported_any = true;
            }
        }
        if let Some(topics) = read_json_file::<HashMap<String, Topic>>(&topics_path) {
            if !topics.is_empty() {
                self.save_topics(&topics);
                imported_any = true;
            }
        }
        if let Some(seen) = read_json_file::<HashMap<i64, Cursor>>(&seen_path) {
            if !seen.is_empty() {
                self.save_seen(&seen);
                imported_any = true;
            }
        }
        if let Some(receipts) = read_json_file::<Receipts>(&receipts_path) {
            if !receipts.out.is_empty()
                || !receipts.inbound.is_empty()
                || !receipts.pending_marks.is_empty()
            {
                self.save_receipts(&receipts);
                imported_any = true;
            }
        }
        if let Ok(raw) = fs::read_to_string(&offset_path) {
            if let Ok(offset) = raw.trim().parse::<i64>() {
                if offset != 0 {
                    self.save_offset(offset);
                    imported_any = true;
                }
            }
        }

        self.with_conn(|conn| Self::meta_set(conn, "schema_version", SCHEMA_VERSION))?;

        if imported_any {
            for path in [
                session_path,
                map_path,
                links_path,
                receipts_path,
                topics_path,
                offset_path,
                seen_path,
                names_path,
                contacts_path,
            ] {
                rename_bak(&path);
            }
        }
        Ok(())
    }
}

fn legacy_path(dir: &Path, env_key: &str, name: &str) -> PathBuf {
    env::var(env_key)
        .ok()
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| dir.join(name))
}

fn read_json_file<T: for<'de> Deserialize<'de>>(path: &Path) -> Option<T> {
    let raw = fs::read_to_string(path).ok()?;
    serde_json::from_str(&raw).ok()
}

fn rename_bak(path: &Path) {
    if !path.exists() {
        return;
    }
    let bak = PathBuf::from(format!("{}.bak", path.display()));
    let _ = fs::rename(path, &bak);
}

fn status_name(status: ReceiptStatus) -> &'static str {
    match status {
        ReceiptStatus::Sent => "sent",
        ReceiptStatus::Arrived => "arrived",
        ReceiptStatus::Read => "read",
    }
}

fn parse_status(raw: &str) -> ReceiptStatus {
    match raw {
        "arrived" => ReceiptStatus::Arrived,
        "read" => ReceiptStatus::Read,
        _ => ReceiptStatus::Sent,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Name, User};

    fn temp_db(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "max-store-{label}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir.join("max.db")
    }

    #[test]
    fn session_and_links_round_trip() {
        let path = temp_db("round");
        let store = Store::open(&path).unwrap();
        store
            .write_session(&Session::signed_in(
                "tok".into(),
                "dev".into(),
                "inst".into(),
            ))
            .unwrap();
        let mut links = Links::default();
        links.tg.insert(1, 2);
        links.max.insert(2, 1);
        store.save_links(&links);
        drop(store);

        let store = Store::open(&path).unwrap();
        let session = store.read_session().unwrap().unwrap();
        assert_eq!(session.token, "tok");
        assert_eq!(session.device_id, "dev");
        let loaded = store.load_links();
        assert_eq!(loaded.tg.get(&1), Some(&2));
        assert_eq!(loaded.max.get(&2), Some(&1));
        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn migrates_legacy_json_once() {
        let dir = temp_db("migrate").parent().unwrap().to_path_buf();
        let session = Session::signed_in("legacy".into(), "device".into(), String::new());
        fs::write(
            dir.join("session.json"),
            serde_json::to_string_pretty(&session).unwrap(),
        )
        .unwrap();
        fs::write(dir.join("names.json"), r#"{"7":"Маня Н"}"#).unwrap();
        fs::write(dir.join("contacts.json"), "[7,0]").unwrap();
        fs::write(dir.join("map.json"), r#"{"10":20}"#).unwrap();
        fs::write(dir.join("links.json"), r#"{"tg":{"1":2},"max":{"2":1}}"#).unwrap();
        fs::write(
            dir.join("topics.json"),
            r#"{"1:9":{"thread_id":44,"name":"Маня Н","photo":true}}"#,
        )
        .unwrap();
        fs::write(dir.join("seen.json"), r#"{"9":{"time":1,"id":2}}"#).unwrap();
        fs::write(
            dir.join("receipts.json"),
            r#"{"out":{},"inbound":{"8":{"max_chat":55,"max_message":42,"time":100}},"pending_marks":[]}"#,
        )
        .unwrap();
        fs::write(dir.join("tg-offset"), "99").unwrap();

        let db = dir.join("max.db");
        let store = Store::open(&db).unwrap();
        assert_eq!(store.read_session().unwrap().unwrap().token, "legacy");
        assert_eq!(
            store.load_names().unwrap().get(&7).map(String::as_str),
            Some("Маня Н")
        );
        assert!(store.load_contacts().unwrap().contains(&7));
        assert_eq!(store.load_map().get(&10), Some(&20));
        assert_eq!(store.load_offset(), 99);
        assert_eq!(
            store
                .load_receipts()
                .inbound
                .get(&8)
                .map(|item| item.max_message),
            Some(42)
        );
        assert!(dir.join("session.json.bak").exists());
        assert!(!dir.join("session.json").exists());
        drop(store);

        // Re-open must not require JSON and must keep data.
        fs::write(dir.join("session.json"), r#"{"token":"x","device_id":"y"}"#).unwrap();
        let store = Store::open(&db).unwrap();
        assert_eq!(store.read_session().unwrap().unwrap().token, "legacy");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn name_merge_updates_rows() {
        let path = temp_db("names");
        let store = Store::open(&path).unwrap();
        let mut users = HashMap::new();
        users.insert(
            7,
            User {
                id: 7,
                names: vec![Name {
                    name: Some("Маня Н".into()),
                    first_name: None,
                    last_name: None,
                    kind: "CUSTOM".into(),
                }],
                photo: None,
            },
        );
        store.merge_names_from_users(&users).unwrap();
        assert_eq!(
            store.load_names().unwrap().get(&7).map(String::as_str),
            Some("Маня Н")
        );
        let _ = fs::remove_dir_all(path.parent().unwrap());
    }
}
