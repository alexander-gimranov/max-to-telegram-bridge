mod auth_http;
mod text;

use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use max_client::{lookup_v4, read_session, Attach, ChatFace, Client, Event, Media, Message};
use reqwest::multipart::{Form, Part};
use reqwest::Client as Http;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const TELEGRAM_HOST: &str = "api.telegram.org";
const MAX_MEDIA_BYTES: usize = 20 * 1024 * 1024;

fn main() {
    let config = match Config::load() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("bridge: {err}");
            std::process::exit(1);
        }
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    if let Err(err) = runtime.block_on(run(config)) {
        eprintln!("bridge: {err}");
        std::process::exit(1);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Chat,
    Thread,
}

struct Config {
    token: String,
    allowed: Vec<i64>,
    mode: Mode,
    session_path: PathBuf,
    map_path: PathBuf,
    links_path: PathBuf,
    topics_path: PathBuf,
    offset_path: PathBuf,
    seen_path: PathBuf,
}

impl Config {
    fn load() -> Result<Self, String> {
        let mut file_values = read_env_file(&env_path());
        let token = env::var("TELEGRAM_BOT_TOKEN")
            .ok()
            .filter(|value| !value.is_empty())
            .or_else(|| file_values.remove("TELEGRAM_BOT_TOKEN"))
            .ok_or_else(|| {
                "TELEGRAM_BOT_TOKEN is missing. Set it in var/bridge.env or the environment."
                    .to_owned()
            })?;
        let ids_raw = env::var("TELEGRAM_ALLOWED_USER_IDS")
            .ok()
            .filter(|value| !value.is_empty())
            .or_else(|| file_values.remove("TELEGRAM_ALLOWED_USER_IDS"))
            .ok_or_else(|| {
                "TELEGRAM_ALLOWED_USER_IDS is missing. Set it in var/bridge.env or the environment."
                    .to_owned()
            })?;
        let allowed = parse_allowed_ids(&ids_raw)?;
        let mode_raw = env::var("MODE")
            .ok()
            .filter(|value| !value.is_empty())
            .or_else(|| file_values.remove("MODE"))
            .unwrap_or_default();
        let mode = parse_mode(&mode_raw)?;
        Ok(Config {
            token,
            allowed,
            mode,
            session_path: data_path("MAX_SESSION_PATH", "session.json"),
            map_path: data_path("MAX_MAP_PATH", "map.json"),
            links_path: data_path("MAX_LINKS_PATH", "links.json"),
            topics_path: data_path("MAX_TOPICS_PATH", "topics.json"),
            offset_path: data_path("MAX_OFFSET_PATH", "tg-offset"),
            seen_path: data_path("MAX_SEEN_PATH", "seen.json"),
        })
    }
}

fn parse_mode(raw: &str) -> Result<Mode, String> {
    match raw.trim() {
        "" | "Chat" => Ok(Mode::Chat),
        "Thread" => Ok(Mode::Thread),
        other => Err(format!("MODE must be Chat or Thread, got {other}")),
    }
}

fn var_dir() -> PathBuf {
    if let Ok(dir) = env::var("MAX_VAR") {
        if !dir.is_empty() {
            return PathBuf::from(dir);
        }
    }
    if let Some(root) = workspace_root() {
        return root.join("var");
    }
    PathBuf::from("var")
}

fn workspace_root() -> Option<PathBuf> {
    let mut dir = env::current_exe().ok()?;
    dir.pop();
    for _ in 0..6 {
        let marker = dir.join("Cargo.toml");
        if marker.is_file() {
            if let Ok(text) = fs::read_to_string(&marker) {
                if text.contains("max-bridge") {
                    return Some(dir);
                }
            }
        }
        if !dir.pop() {
            break;
        }
    }
    None
}

fn env_path() -> PathBuf {
    env::var("MAX_ENV_PATH")
        .ok()
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| var_dir().join("bridge.env"))
}

fn data_path(key: &str, name: &str) -> PathBuf {
    env::var(key)
        .ok()
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| var_dir().join(name))
}

fn parse_allowed_ids(raw: &str) -> Result<Vec<i64>, String> {
    let mut ids = Vec::new();
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let id: i64 = part.parse().map_err(|_| {
            format!("TELEGRAM_ALLOWED_USER_IDS has a value that is not a number: {part}")
        })?;
        ids.push(id);
    }
    if ids.is_empty() {
        return Err("TELEGRAM_ALLOWED_USER_IDS is empty".into());
    }
    Ok(ids)
}

fn read_env_file(path: &std::path::Path) -> HashMap<String, String> {
    let mut values = HashMap::new();
    let Ok(raw) = fs::read_to_string(path) else {
        return values;
    };
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let value = value.trim().trim_matches('"').trim_matches('\'');
        values.insert(key.trim().to_owned(), value.to_owned());
    }
    values
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LinkState {
    Off,
    Connecting,
    Ready,
}

fn history_allowed(link: LinkState) -> bool {
    link == LinkState::Ready
}

fn is_setup_command(text: &str) -> bool {
    let text = text.trim().to_lowercase();
    text == "/setup" || text.starts_with("/setup@") || text == text::SETUP.to_lowercase()
}

fn is_logout_command(text: &str) -> bool {
    let text = text.trim().to_lowercase();
    text == "/logout" || text.starts_with("/logout@") || text == text::SIGN_OUT.to_lowercase()
}

fn session_rejected(note: &str) -> bool {
    note.contains("saved MAX session was rejected")
        || note.contains("FAIL_LOGIN_TOKEN")
        || note.contains("FAIL_WRONG_PASSWORD")
        || note.contains("login.token")
        || note.contains("login.cred")
}

fn panel_text(link: LinkState) -> &'static str {
    match link {
        LinkState::Off => text::PANEL_OFF,
        LinkState::Connecting => text::PANEL_CONNECTING,
        LinkState::Ready => text::PANEL_READY,
    }
}

fn panel_markup(link: LinkState) -> Value {
    match link {
        LinkState::Off => json!({
            "inline_keyboard": [[{
                "text": text::SETUP,
                "url": auth_http::AUTH_URL
            }]]
        }),
        LinkState::Connecting => json!({
            "inline_keyboard": []
        }),
        LinkState::Ready => json!({
            "inline_keyboard": [
                [{"text": text::LOAD_HISTORY, "callback_data": "history"}],
                [{"text": text::RELOAD_HISTORY, "callback_data": "reread"}],
                [{"text": text::SIGN_OUT, "callback_data": "logout"}]
            ]
        }),
    }
}

fn command_list(link: LinkState) -> Value {
    match link {
        LinkState::Ready => json!([
            {"command": "history", "description": text::LOAD_HISTORY.to_lowercase()},
            {"command": "reread", "description": text::RELOAD_HISTORY.to_lowercase()},
            {"command": "logout", "description": text::SIGN_OUT.to_lowercase()}
        ]),
        LinkState::Connecting => json!([]),
        LinkState::Off => json!([
            {"command": "setup", "description": text::SETUP.to_lowercase()}
        ]),
    }
}

fn sent_message_id(payload: &Value) -> Option<i64> {
    payload
        .get("result")
        .and_then(|result| result.get("message_id"))
        .and_then(Value::as_i64)
}

async fn max_event(client: Option<&mut Client>) -> Option<Event> {
    match client {
        Some(client) => client.next_event().await,
        None => std::future::pending().await,
    }
}

enum Wake {
    Max(Option<Event>),
    Telegram(Result<Vec<Update>, String>),
    Auth { changed: bool },
}

async fn sync_controls(
    http: &Http,
    config: &Config,
    link: LinkState,
    panels: &mut HashMap<i64, i64>,
) {
    let text = panel_text(link);
    let markup = panel_markup(link);
    let commands = command_list(link);
    for user_id in &config.allowed {
        let _ = telegram(
            http,
            &config.token,
            "setMyCommands",
            json!({
                "commands": commands.clone(),
                "scope": {"type": "chat", "chat_id": user_id}
            }),
        )
        .await;
        if let Some(message_id) = panels.get(user_id).copied() {
            let edited = telegram(
                http,
                &config.token,
                "editMessageText",
                json!({
                    "chat_id": user_id,
                    "message_id": message_id,
                    "text": text,
                    "reply_markup": markup.clone(),
                }),
            )
            .await;
            if edited.is_ok() {
                continue;
            }
        }
        if let Ok(payload) = telegram(
            http,
            &config.token,
            "sendMessage",
            json!({
                "chat_id": user_id,
                "text": text,
                "reply_markup": markup.clone(),
            }),
        )
        .await
        {
            if let Some(message_id) = sent_message_id(&payload) {
                panels.insert(*user_id, message_id);
            }
        }
    }
}

async fn clear_reply_keyboard(http: &Http, config: &Config) {
    for user_id in &config.allowed {
        let Ok(payload) = telegram(
            http,
            &config.token,
            "sendMessage",
            json!({
                "chat_id": user_id,
                "text": text::PANEL_UPDATED,
                "reply_markup": {"remove_keyboard": true}
            }),
        )
        .await
        else {
            continue;
        };
        let Some(message_id) = sent_message_id(&payload) else {
            continue;
        };
        let _ = telegram(
            http,
            &config.token,
            "deleteMessage",
            json!({"chat_id": user_id, "message_id": message_id}),
        )
        .await;
    }
}

fn forget_session(path: &Path) {
    let _ = fs::remove_file(path);
}

async fn ask_logout(http: &Http, config: &Config, asks: &mut HashMap<i64, i64>, user_id: i64) {
    if let Some(message_id) = asks.remove(&user_id) {
        let _ = telegram(
            http,
            &config.token,
            "deleteMessage",
            json!({"chat_id": user_id, "message_id": message_id}),
        )
        .await;
    }
    let Ok(payload) = telegram(
        http,
        &config.token,
        "sendMessage",
        json!({
            "chat_id": user_id,
            "text": text::LOGOUT_ASK,
            "reply_markup": {
                "inline_keyboard": [[
                    {"text": text::YES, "callback_data": "logout_yes"},
                    {"text": text::NO, "callback_data": "logout_no"}
                ]]
            }
        }),
    )
    .await
    else {
        return;
    };
    if let Some(message_id) = sent_message_id(&payload) {
        asks.insert(user_id, message_id);
    }
}

async fn begin_logout(
    config: &Config,
    http: &Http,
    client: &mut Option<Client>,
    link: &mut LinkState,
    me: &mut Option<i64>,
    panels: &mut HashMap<i64, i64>,
    logout_asks: &mut HashMap<i64, i64>,
    user_id: i64,
) {
    *client = None;
    *link = LinkState::Off;
    *me = None;
    forget_session(&config.session_path);
    sync_controls(http, config, *link, panels).await;
    ask_logout(http, config, logout_asks, user_id).await;
}

async fn close_logout_ask(
    http: &Http,
    config: &Config,
    asks: &mut HashMap<i64, i64>,
    user_id: i64,
) {
    let Some(message_id) = asks.remove(&user_id) else {
        return;
    };
    let _ = telegram(
        http,
        &config.token,
        "deleteMessage",
        json!({"chat_id": user_id, "message_id": message_id}),
    )
    .await;
}

async fn run(config: Config) -> Result<(), String> {
    let mut map = load_map(&config.map_path);
    let mut links = load_links(&config.links_path);
    let mut topics = load_topics(&config.topics_path);
    let mut offset = load_offset(&config.offset_path);
    let mut seen = load_seen(&config.seen_path);
    let mut ready_count = 0u32;
    eprintln!(
        "bridge: mode is {}",
        match config.mode {
            Mode::Chat => "Chat",
            Mode::Thread => "Thread",
        }
    );
    let auth = auth_http::AuthHub::new(config.session_path.clone());
    let mut auth_notes = auth.subscribe();
    let auth_page = auth.clone();
    tokio::spawn(async move {
        if let Err(err) = auth_page.listen().await {
            eprintln!("bridge: auth page failed: {err}");
        }
    });
    let mut http = telegram_http().await?;
    clear_reply_keyboard(&http, &config).await;
    let mut client = None;
    let mut link = LinkState::Off;
    if read_session(&config.session_path)
        .map_err(|err| err.to_string())?
        .is_some()
    {
        match Client::start(config.session_path.clone()).await {
            Ok(started) => {
                client = Some(started);
                link = LinkState::Connecting;
            }
            Err(err) => eprintln!("bridge: {err}"),
        }
    }
    let mut panels = HashMap::new();
    let mut logout_asks = HashMap::new();
    sync_controls(&http, &config, link, &mut panels).await;
    let mut me: Option<i64> = None;
    let mut echoes = HashSet::<i64>::new();
    loop {
        let wake = tokio::select! {
            event = max_event(client.as_mut()) => Wake::Max(event),
            polled = poll_telegram(&http, &config.token, offset) => Wake::Telegram(polled),
            changed = auth_notes.changed() => Wake::Auth { changed: changed.is_ok() },
        };
        match wake {
            Wake::Auth { changed: false } => {}
            Wake::Auth { changed: true } => {
                if client.is_none() && read_session(&config.session_path).ok().flatten().is_some() {
                    match Client::start(config.session_path.clone()).await {
                        Ok(started) => {
                            client = Some(started);
                            link = LinkState::Connecting;
                            sync_controls(&http, &config, link, &mut panels).await;
                        }
                        Err(err) => eprintln!("bridge: {err}"),
                    }
                }
            }
            Wake::Telegram(Err(err)) => {
                eprintln!("bridge: telegram poll failed: {err}");
                tokio::time::sleep(Duration::from_secs(3)).await;
                if let Ok(fresh) = telegram_http().await {
                    http = fresh;
                }
            }
            Wake::Telegram(Ok(updates)) => {
                for update in updates {
                    offset = update.update_id.saturating_add(1);
                    save_offset(&config.offset_path, offset);
                    if let Err(err) = handle_update(
                        &config,
                        &mut http,
                        &mut client,
                        &mut link,
                        &mut map,
                        &mut links,
                        &mut topics,
                        &mut seen,
                        &mut echoes,
                        &mut me,
                        &mut panels,
                        &mut logout_asks,
                        &update,
                    )
                    .await
                    {
                        eprintln!("bridge: {err}");
                    }
                }
            }
            Wake::Max(None) => {
                client = None;
                if link != LinkState::Off {
                    link = LinkState::Off;
                    me = None;
                    sync_controls(&http, &config, link, &mut panels).await;
                }
            }
            Wake::Max(Some(event)) => match event {
                Event::Ready { me: id } => {
                    me = Some(id);
                    let restored = ready_count > 0;
                    ready_count = ready_count.saturating_add(1);
                    let became_ready = link != LinkState::Ready;
                    link = LinkState::Ready;
                    if restored {
                        eprintln!("bridge: MAX connection restored");
                    } else {
                        eprintln!("bridge: MAX session is ready");
                    }
                    if became_ready {
                        sync_controls(&http, &config, link, &mut panels).await;
                    }
                    if let Some(active) = client.as_mut() {
                        match catch_up(
                            &config,
                            &mut http,
                            active,
                            &mut map,
                            &mut links,
                            &mut topics,
                            me,
                            &mut seen,
                            &mut echoes,
                        )
                        .await
                        {
                            Ok(0) if restored => eprintln!("bridge: no missed messages"),
                            Ok(0) => {}
                            Ok(count) => eprintln!("bridge: forwarded {count} missed messages"),
                            Err(err) => eprintln!("bridge: catch-up failed: {err}"),
                        }
                    }
                }
                Event::Logged { note } => {
                    eprintln!("bridge: {note}");
                    if session_rejected(&note) {
                        client = None;
                        link = LinkState::Off;
                        me = None;
                        sync_controls(&http, &config, link, &mut panels).await;
                    }
                }
                Event::Message(message) => {
                    let Some(active) = client.as_mut() else {
                        continue;
                    };
                    if echoes.remove(&message.id) {
                        mark_seen(&mut seen, &config.seen_path, &message);
                        continue;
                    }
                    if !is_unseen(&message, cursor_of(&seen, message.chat_id)) {
                        continue;
                    }
                    if !forward_message(&message, me) || skipped_status(&message) {
                        mark_seen(&mut seen, &config.seen_path, &message);
                        continue;
                    }
                    if let Err(err) = forward_max(
                        &config,
                        &mut http,
                        active,
                        &mut map,
                        &mut links,
                        &mut topics,
                        &message,
                        me,
                        false,
                    )
                    .await
                    {
                        eprintln!("bridge: telegram send failed: {err}");
                        if let Ok(fresh) = telegram_http().await {
                            http = fresh;
                        }
                    } else {
                        mark_seen(&mut seen, &config.seen_path, &message);
                    }
                }
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
struct Cursor {
    time: i64,
    id: i64,
}

fn cursor_of(seen: &HashMap<i64, Cursor>, chat_id: Option<i64>) -> Option<Cursor> {
    chat_id.and_then(|chat_id| seen.get(&chat_id).copied())
}

fn is_unseen(message: &Message, cursor: Option<Cursor>) -> bool {
    let Some(cursor) = cursor else {
        return true;
    };
    if message.time == 0 || cursor.time == 0 {
        return message.id > cursor.id;
    }
    message.time > cursor.time || (message.time == cursor.time && message.id > cursor.id)
}

fn skipped_status(message: &Message) -> bool {
    matches!(message.status.as_deref(), Some("REMOVED") | Some("EDITED"))
}

fn take_missed(messages: Vec<Message>, cursor: Option<Cursor>) -> Vec<Message> {
    let mut missed: Vec<Message> = messages
        .into_iter()
        .filter(|message| is_unseen(message, cursor))
        .collect();
    missed.sort_by_key(|message| (message.time, message.id));
    missed.dedup_by(|left, right| left.id == right.id);
    missed
}

fn baseline_cursor(messages: &[Message]) -> Option<Cursor> {
    messages
        .iter()
        .max_by_key(|message| (message.time, message.id))
        .map(|message| Cursor {
            time: message.time,
            id: message.id,
        })
}

fn mark_seen(seen: &mut HashMap<i64, Cursor>, path: &std::path::Path, message: &Message) {
    let Some(chat_id) = message.chat_id else {
        return;
    };
    if !is_unseen(message, seen.get(&chat_id).copied()) {
        return;
    }
    seen.insert(
        chat_id,
        Cursor {
            time: message.time,
            id: message.id,
        },
    );
    save_seen(path, seen);
}

fn unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

async fn catch_up(
    config: &Config,
    http: &mut Http,
    client: &mut Client,
    map: &mut HashMap<i64, i64>,
    links: &mut Links,
    topics: &mut HashMap<String, Topic>,
    me: Option<i64>,
    seen: &mut HashMap<i64, Cursor>,
    echoes: &mut HashSet<i64>,
) -> Result<usize, String> {
    let mut chats = client.chat_ids().await.map_err(|err| err.to_string())?;
    for chat_id in seen.keys().copied() {
        if !chats.contains(&chat_id) {
            chats.push(chat_id);
        }
    }
    chats.sort_unstable();
    let mut forwarded = 0usize;
    for chat_id in chats {
        let cursor = seen.get(&chat_id).copied();
        let mut from = unix_ms();
        let mut missed = Vec::new();
        for _ in 0..5 {
            let batch = match client.fetch_history(chat_id, 40, from).await {
                Ok(batch) => batch,
                Err(err) => {
                    eprintln!("bridge: history read failed for chat {chat_id}: {err}");
                    if err.to_string().contains("too.many.requests") {
                        return Ok(forwarded);
                    }
                    break;
                }
            };
            if batch.is_empty() {
                break;
            }
            if cursor.is_none() {
                if let Some(head) = baseline_cursor(&batch) {
                    seen.insert(chat_id, head);
                    save_seen(&config.seen_path, seen);
                }
                break;
            }
            let oldest = batch
                .iter()
                .filter_map(|message| (message.time > 0).then_some(message.time))
                .min()
                .unwrap_or(from);
            let hit_old = batch.iter().any(|message| !is_unseen(message, cursor));
            missed.extend(take_missed(batch, cursor));
            if hit_old || oldest >= from {
                break;
            }
            from = oldest;
        }
        for message in take_missed(missed, seen.get(&chat_id).copied()) {
            if echoes.remove(&message.id) {
                mark_seen(seen, &config.seen_path, &message);
                continue;
            }
            if !forward_message(&message, me) || skipped_status(&message) {
                mark_seen(seen, &config.seen_path, &message);
                continue;
            }
            if let Err(err) = forward_max(
                config, http, client, map, links, topics, &message, me, false,
            )
            .await
            {
                eprintln!("bridge: telegram send failed: {err}");
                if let Ok(fresh) = telegram_http().await {
                    *http = fresh;
                }
                break;
            }
            mark_seen(seen, &config.seen_path, &message);
            forwarded += 1;
        }
    }
    Ok(forwarded)
}

fn forward_message(message: &Message, me: Option<i64>) -> bool {
    // Chat 0 is the saved-messages dialog. A note there is from this account,
    // and that is the one outgoing chat the bridge copies.
    if message.chat_id.is_none() {
        return false;
    }
    let own = me.is_some_and(|id| message.sender == Some(id));
    !own || message.chat_id == Some(0)
}

async fn forward_author(client: &Client, message: &Message) -> Option<String> {
    let source = message.forward.as_ref()?;
    let looked = lookup_name(client, source.sender).await;
    if !looked.is_empty() {
        return Some(looked);
    }
    source.name.clone().filter(|name| !name.is_empty())
}

fn header_prefix(
    forward_name: Option<&str>,
    own: bool,
    group: bool,
    chat_id: i64,
    sender: &str,
) -> String {
    if let Some(name) = forward_name.map(str::trim).filter(|name| !name.is_empty()) {
        return format!("{}{name}", text::FORWARDED_BY);
    }
    if own {
        return text::YOU.to_owned();
    }
    if group && chat_id != 0 && !sender.is_empty() {
        return sender.to_owned();
    }
    String::new()
}

async fn lookup_name(client: &Client, user_id: Option<i64>) -> String {
    let Some(user_id) = user_id else {
        return String::new();
    };
    for attempt in 0..2 {
        let name = client.user_name(user_id).await;
        if !name.is_empty() {
            return name;
        }
        if attempt == 0 {
            tokio::time::sleep(Duration::from_millis(1500)).await;
        }
    }
    String::new()
}

async fn forward_max(
    config: &Config,
    http: &mut Http,
    client: &mut Client,
    map: &mut HashMap<i64, i64>,
    links: &mut Links,
    topics: &mut HashMap<String, Topic>,
    message: &Message,
    me: Option<i64>,
    history: bool,
) -> Result<(), String> {
    let Some(chat_id) = message.chat_id else {
        return Ok(());
    };
    let sender = lookup_name(client, message.sender).await;
    let face = client.chat_face(chat_id).await;
    let own = history && me.is_some_and(|id| message.sender == Some(id));
    let forward_name = forward_author(client, message).await;
    let bold_prefix = header_prefix(
        forward_name.as_deref(),
        own,
        face.group,
        chat_id,
        &sender,
    );
    let title = if chat_id == 0 {
        text::FAVORITES.to_owned()
    } else {
        face.label.clone()
    };
    let topic_sender = if own { "" } else { sender.as_str() };
    if history {
        if let Some(max_id) = message.reply_to {
            if !links.max.contains_key(&max_id) {
                eprintln!("bridge: reply target {max_id} has no Telegram message");
            }
        }
    }
    if !message.media.is_empty() {
        let label = topic_label(chat_id, &face, topic_sender);
        let topic_caption = if !bold_prefix.is_empty() {
            clip_caption(&join_name(&bold_prefix, &message.text))
        } else {
            media_caption(
                Mode::Thread,
                face.group && chat_id != 0,
                &sender,
                &title,
                &message.text,
            )
        };
        let feed_caption = if !bold_prefix.is_empty() {
            clip_caption(&join_name(&bold_prefix, &message.text))
        } else {
            media_caption(Mode::Chat, false, &sender, &title, &message.text)
        };
        let mut files = Vec::new();
        for item in &message.media {
            match download_media(client, message, item).await {
                Ok(bytes) => files.push((item, bytes)),
                Err(err) => eprintln!("bridge: media download failed: {err}"),
            }
        }
        if !files.is_empty() {
            if label == "MAX" {
                eprintln!("bridge: contact name is missing for chat {chat_id}");
            }
            let mut sent_any = false;
            for user_id in &config.allowed {
                let mut caption_left = true;
                for (item, bytes) in files.iter() {
                    let caption = if caption_left {
                        if config.mode == Mode::Thread {
                            topic_caption.as_str()
                        } else {
                            feed_caption.as_str()
                        }
                    } else {
                        ""
                    };
                    if let Err(err) = send_media(
                        http,
                        config,
                        client,
                        map,
                        links,
                        topics,
                        *user_id,
                        chat_id,
                        message.id,
                        message.reply_to,
                        &label,
                        caption,
                        &feed_caption,
                        item,
                        bytes,
                        history,
                        &bold_prefix,
                    )
                    .await
                    {
                        eprintln!("bridge: media send failed: {err}");
                        continue;
                    }
                    caption_left = false;
                    sent_any = true;
                }
            }
            if sent_any || message.text.is_empty() {
                return Ok(());
            }
        }
        if message.text.is_empty() {
            return Ok(());
        }
    }
    let body = if message.text.is_empty() {
        text::NO_TEXT.to_owned()
    } else {
        message.text.clone()
    };
    match config.mode {
        Mode::Chat => {
            let who = if !bold_prefix.is_empty() {
                bold_prefix.clone()
            } else {
                chat_heading(&sender, &title)
            };
            if who == "MAX" {
                eprintln!("bridge: contact name is missing for chat {chat_id}");
            }
            let text = clip(&format!("{who}\n\n{body}"), 4000);
            for user_id in &config.allowed {
                send_feed(http, config, map, *user_id, &text, chat_id, &bold_prefix).await?;
            }
        }
        Mode::Thread => {
            let label = topic_label(chat_id, &face, topic_sender);
            if label == "MAX" {
                eprintln!("bridge: contact name is missing for chat {chat_id}");
            }
            let text = if !bold_prefix.is_empty() {
                clip(&join_name(&bold_prefix, &body), 4000)
            } else {
                clip(&body, 4000)
            };
            let reply_to = message
                .reply_to
                .and_then(|max_id| links.max.get(&max_id).copied());
            for user_id in &config.allowed {
                if let Err(err) = send_in_topic(
                    http,
                    config,
                    client,
                    topics,
                    links,
                    *user_id,
                    chat_id,
                    message.id,
                    &label,
                    &text,
                    reply_to,
                    &bold_prefix,
                )
                .await
                {
                    if history {
                        return Err(err);
                    }
                    eprintln!("bridge: topic send failed: {err}");
                    let fallback = if !bold_prefix.is_empty() {
                        clip(&join_name(&bold_prefix, &body), 4000)
                    } else {
                        let who = chat_heading(&sender, &title);
                        clip(&format!("{who}\n\n{body}"), 4000)
                    };
                    send_feed(
                        http,
                        config,
                        map,
                        *user_id,
                        &fallback,
                        chat_id,
                        &bold_prefix,
                    )
                    .await?;
                }
            }
        }
    }
    Ok(())
}

async fn send_feed(
    http: &mut Http,
    config: &Config,
    map: &mut HashMap<i64, i64>,
    user_id: i64,
    text: &str,
    chat_id: i64,
    bold_prefix: &str,
) -> Result<(), String> {
    let message_id = send_text(http, config, user_id, None, None, text, bold_prefix).await?;
    if config.mode == Mode::Chat {
        map.insert(message_id, chat_id);
        save_map(&config.map_path, map);
    }
    Ok(())
}

async fn send_in_topic(
    http: &mut Http,
    config: &Config,
    client: &mut Client,
    topics: &mut HashMap<String, Topic>,
    links: &mut Links,
    tg_user: i64,
    max_chat: i64,
    max_message: i64,
    label: &str,
    text: &str,
    reply_to: Option<i64>,
    bold_prefix: &str,
) -> Result<(), String> {
    let name = topic_name(label);
    let key = topic_key(tg_user, max_chat);
    let thread_id =
        ensure_topic(http, config, client, topics, tg_user, max_chat, &key, &name).await?;
    let tg_message = match send_thread(
        http,
        config,
        tg_user,
        thread_id,
        text,
        reply_to,
        bold_prefix,
    )
    .await
    {
        Ok(id) => id,
        Err(err) if thread_missing(&err) => {
            topics.remove(&key);
            save_topics(&config.topics_path, topics);
            let thread_id =
                ensure_topic(http, config, client, topics, tg_user, max_chat, &key, &name).await?;
            send_thread(
                http,
                config,
                tg_user,
                thread_id,
                text,
                reply_to,
                bold_prefix,
            )
            .await?
        }
        Err(err) => return Err(err),
    };
    remember_link(links, tg_message, max_message);
    save_links(&config.links_path, links);
    Ok(())
}

async fn ensure_topic(
    http: &mut Http,
    config: &Config,
    client: &mut Client,
    topics: &mut HashMap<String, Topic>,
    tg_user: i64,
    max_chat: i64,
    key: &str,
    name: &str,
) -> Result<i64, String> {
    if let Some(topic) = topics.get(key).cloned() {
        if topic.name != name {
            match telegram(
                http,
                &config.token,
                "editForumTopic",
                json!({
                    "chat_id": tg_user,
                    "message_thread_id": topic.thread_id,
                    "name": name,
                }),
            )
            .await
            {
                Ok(_) => {
                    topics.insert(
                        key.to_owned(),
                        Topic {
                            thread_id: topic.thread_id,
                            name: name.to_owned(),
                            photo: topic.photo,
                        },
                    );
                    save_topics(&config.topics_path, topics);
                }
                Err(err) => eprintln!("bridge: rename topic failed: {err}"),
            }
        }
        pin_topic_photo(
            http,
            config,
            client,
            topics,
            tg_user,
            max_chat,
            key,
            topic.thread_id,
        )
        .await;
        return Ok(topic.thread_id);
    }
    let created = telegram(
        http,
        &config.token,
        "createForumTopic",
        json!({"chat_id": tg_user, "name": name}),
    )
    .await?;
    let thread_id = created
        .get("result")
        .and_then(|result| result.get("message_thread_id"))
        .and_then(Value::as_i64)
        .ok_or_else(|| "telegram createForumTopic did not return a topic".to_owned())?;
    topics.insert(
        key.to_owned(),
        Topic {
            thread_id,
            name: name.to_owned(),
            photo: false,
        },
    );
    save_topics(&config.topics_path, topics);
    pin_topic_photo(
        http, config, client, topics, tg_user, max_chat, key, thread_id,
    )
    .await;
    Ok(thread_id)
}

async fn pin_topic_photo(
    http: &Http,
    config: &Config,
    client: &mut Client,
    topics: &mut HashMap<String, Topic>,
    tg_user: i64,
    max_chat: i64,
    key: &str,
    thread_id: i64,
) {
    if topics.get(key).is_some_and(|topic| topic.photo) {
        return;
    }
    if !note_photo_try(key) {
        return;
    }
    let url = match client.chat_photo(max_chat).await {
        Ok(url) => url,
        Err(()) => {
            eprintln!("bridge: topic photo failed");
            return;
        }
    };
    let Some(url) = url else {
        mark_topic_photo(config, topics, key);
        return;
    };
    let bytes = match download_public(&url).await {
        Ok(bytes) => bytes,
        Err(err) => {
            eprintln!("bridge: topic photo failed: {err}");
            return;
        }
    };
    let mime = image_mime(&bytes);
    let fields = vec![
        ("chat_id".to_owned(), tg_user.to_string()),
        ("message_thread_id".to_owned(), thread_id.to_string()),
    ];
    let sent = match telegram_file(
        http,
        &config.token,
        "sendPhoto",
        fields,
        "photo",
        image_name(mime),
        mime,
        &bytes,
    )
    .await
    {
        Ok(sent) => sent,
        Err(err) => {
            eprintln!("bridge: topic photo failed: {err}");
            return;
        }
    };
    let Some(message_id) = sent
        .get("result")
        .and_then(|result| result.get("message_id"))
        .and_then(Value::as_i64)
    else {
        eprintln!("bridge: topic photo failed");
        return;
    };
    if let Err(err) = telegram(
        http,
        &config.token,
        "pinChatMessage",
        json!({
            "chat_id": tg_user,
            "message_id": message_id,
            "disable_notification": true
        }),
    )
    .await
    {
        eprintln!("bridge: topic photo pin failed: {err}");
    }
    mark_topic_photo(config, topics, key);
}

fn mark_topic_photo(config: &Config, topics: &mut HashMap<String, Topic>, key: &str) {
    if let Some(topic) = topics.get_mut(key) {
        topic.photo = true;
        save_topics(&config.topics_path, topics);
    }
}

fn note_photo_try(key: &str) -> bool {
    static TRIED: std::sync::OnceLock<std::sync::Mutex<HashSet<String>>> =
        std::sync::OnceLock::new();
    let tried = TRIED.get_or_init(|| std::sync::Mutex::new(HashSet::new()));
    let mut tried = tried.lock().unwrap_or_else(|err| err.into_inner());
    tried.insert(key.to_owned())
}

async fn send_thread(
    http: &mut Http,
    config: &Config,
    tg_user: i64,
    thread_id: i64,
    text: &str,
    reply_to: Option<i64>,
    bold_prefix: &str,
) -> Result<i64, String> {
    send_text(
        http,
        config,
        tg_user,
        Some(thread_id),
        reply_to,
        text,
        bold_prefix,
    )
    .await
}

async fn send_text(
    http: &mut Http,
    config: &Config,
    tg_user: i64,
    thread_id: Option<i64>,
    reply_to: Option<i64>,
    text: &str,
    bold_prefix: &str,
) -> Result<i64, String> {
    if let Some(rich) = named_rich(bold_prefix, text) {
        let mut body = json!({
            "chat_id": tg_user,
            "rich_message": rich,
        });
        if let Some(thread_id) = thread_id {
            body["message_thread_id"] = json!(thread_id);
        }
        if let Some(reply_to) = reply_to {
            body["reply_parameters"] = json!({"message_id": reply_to});
        }
        for attempt in 0..3 {
            match telegram(http, &config.token, "sendRichMessage", body.clone()).await {
                Ok(sent) => return telegram_message_id(&sent, "sendRichMessage"),
                Err(err) => {
                    if let Some(wait) = retry_after(&err) {
                        if attempt < 2 {
                            tokio::time::sleep(Duration::from_secs(wait.max(1))).await;
                            continue;
                        }
                        return Err(err);
                    }
                    eprintln!("bridge: rich message failed: {err}");
                    break;
                }
            }
        }
    }
    let mut body = json!({"chat_id": tg_user, "text": text});
    if let Some(thread_id) = thread_id {
        body["message_thread_id"] = json!(thread_id);
    }
    if let Some(entities) = bold_prefix_entities(bold_prefix, text) {
        body["entities"] = entities;
    }
    if let Some(reply_to) = reply_to {
        body["reply_to_message_id"] = json!(reply_to);
    }
    let sent = telegram(http, &config.token, "sendMessage", body).await?;
    telegram_message_id(&sent, "sendMessage")
}

fn telegram_message_id(sent: &Value, method: &str) -> Result<i64, String> {
    sent.get("result")
        .and_then(|result| result.get("message_id"))
        .and_then(Value::as_i64)
        .ok_or_else(|| format!("telegram {method} did not return a message"))
}

async fn send_media(
    http: &mut Http,
    config: &Config,
    client: &mut Client,
    map: &mut HashMap<i64, i64>,
    links: &mut Links,
    topics: &mut HashMap<String, Topic>,
    tg_user: i64,
    max_chat: i64,
    max_message: i64,
    reply_to_max: Option<i64>,
    label: &str,
    caption: &str,
    feed_caption: &str,
    item: &Media,
    bytes: &[u8],
    history: bool,
    bold_prefix: &str,
) -> Result<(), String> {
    let reply_to = reply_to_max.and_then(|max_id| links.max.get(&max_id).copied());
    let tg_id = match config.mode {
        Mode::Chat => {
            send_telegram_media(
                http,
                config,
                tg_user,
                None,
                None,
                caption,
                item,
                bytes,
                bold_prefix,
            )
            .await?
        }
        Mode::Thread => {
            match send_topic_media(
                http,
                config,
                client,
                topics,
                tg_user,
                max_chat,
                &label,
                reply_to,
                caption,
                item,
                bytes,
                bold_prefix,
            )
            .await
            {
                Ok(id) => id,
                Err(err) if history => return Err(err),
                Err(err) => {
                    eprintln!("bridge: topic send failed: {err}");
                    send_telegram_media(
                        http,
                        config,
                        tg_user,
                        None,
                        None,
                        feed_caption,
                        item,
                        bytes,
                        bold_prefix,
                    )
                    .await?
                }
            }
        }
    };
    note_telegram_message(config, map, links, tg_id, max_chat, max_message);
    Ok(())
}

async fn send_topic_media(
    http: &mut Http,
    config: &Config,
    client: &mut Client,
    topics: &mut HashMap<String, Topic>,
    tg_user: i64,
    max_chat: i64,
    label: &str,
    reply_to: Option<i64>,
    caption: &str,
    item: &Media,
    bytes: &[u8],
    bold_prefix: &str,
) -> Result<i64, String> {
    let name = topic_name(label);
    let key = topic_key(tg_user, max_chat);
    let thread_id =
        ensure_topic(http, config, client, topics, tg_user, max_chat, &key, &name).await?;
    match send_telegram_media(
        http,
        config,
        tg_user,
        Some(thread_id),
        reply_to,
        caption,
        item,
        bytes,
        bold_prefix,
    )
    .await
    {
        Ok(id) => Ok(id),
        Err(err) if thread_missing(&err) => {
            topics.remove(&key);
            save_topics(&config.topics_path, topics);
            let thread_id =
                ensure_topic(http, config, client, topics, tg_user, max_chat, &key, &name).await?;
            send_telegram_media(
                http,
                config,
                tg_user,
                Some(thread_id),
                reply_to,
                caption,
                item,
                bytes,
                bold_prefix,
            )
            .await
        }
        Err(err) => Err(err),
    }
}

async fn send_telegram_media(
    http: &Http,
    config: &Config,
    tg_user: i64,
    thread_id: Option<i64>,
    reply_to: Option<i64>,
    caption: &str,
    item: &Media,
    bytes: &[u8],
    bold_prefix: &str,
) -> Result<i64, String> {
    let mut fields = vec![("chat_id".to_owned(), tg_user.to_string())];
    if let Some(thread_id) = thread_id {
        fields.push(("message_thread_id".to_owned(), thread_id.to_string()));
    }
    if let Some(reply_to) = reply_to {
        fields.push(("reply_to_message_id".to_owned(), reply_to.to_string()));
    }
    if !caption.is_empty() {
        fields.push(("caption".to_owned(), caption.to_owned()));
        if let Some(entities) = bold_prefix_entities(bold_prefix, caption) {
            fields.push(("caption_entities".to_owned(), entities.to_string()));
        }
    }
    let sent = match item {
        Media::Photo { .. } => {
            let mime = image_mime(bytes);
            telegram_file(
                http,
                &config.token,
                "sendPhoto",
                fields,
                "photo",
                image_name(mime),
                mime,
                bytes,
            )
            .await?
        }
        Media::Voice { .. } => {
            match telegram_file(
                http,
                &config.token,
                "sendVoice",
                fields.clone(),
                "voice",
                "voice.ogg",
                "audio/ogg",
                bytes,
            )
            .await
            {
                Ok(sent) => sent,
                Err(err) => {
                    eprintln!("bridge: voice send failed: {err}");
                    send_voice_fallback(http, &config.token, fields, bytes, &err).await?
                }
            }
        }
        Media::Video { video_type, .. } if video_type == &Some(1) => {
            let note_fields: Vec<(String, String)> = fields
                .into_iter()
                .filter(|(key, _)| key != "caption" && key != "caption_entities")
                .collect();
            if !caption.is_empty() {
                let mut body = json!({"chat_id": tg_user, "text": caption});
                if let Some(thread_id) = thread_id {
                    body["message_thread_id"] = json!(thread_id);
                }
                if let Some(entities) = bold_prefix_entities(bold_prefix, caption) {
                    body["entities"] = entities;
                }
                let _ = telegram(http, &config.token, "sendMessage", body).await;
            }
            telegram_file(
                http,
                &config.token,
                "sendVideoNote",
                note_fields,
                "video_note",
                "note.mp4",
                "video/mp4",
                bytes,
            )
            .await?
        }
        Media::Video { .. } => {
            telegram_file(
                http,
                &config.token,
                "sendVideo",
                fields,
                "video",
                "video.mp4",
                "video/mp4",
                bytes,
            )
            .await?
        }
    };
    sent.get("result")
        .and_then(|result| result.get("message_id"))
        .and_then(Value::as_i64)
        .ok_or_else(|| "telegram send did not return a message".to_owned())
}

async fn send_voice_fallback(
    http: &Http,
    token: &str,
    fields: Vec<(String, String)>,
    bytes: &[u8],
    voice_error: &str,
) -> Result<Value, String> {
    if !voice_error.contains("VOICE_MESSAGES_FORBIDDEN") {
        match telegram_file(
            http,
            token,
            "sendAudio",
            fields.clone(),
            "audio",
            "voice.ogg",
            "audio/ogg",
            bytes,
        )
        .await
        {
            Ok(sent) => return Ok(sent),
            Err(err) => eprintln!("bridge: audio send failed: {err}"),
        }
    }
    let wav = ogg_to_wav(bytes)?;
    match telegram_file(
        http,
        token,
        "sendAudio",
        fields.clone(),
        "audio",
        "voice.wav",
        "audio/wav",
        &wav,
    )
    .await
    {
        Ok(sent) => {
            eprintln!("bridge: voice sent as audio");
            Ok(sent)
        }
        Err(err) => {
            eprintln!("bridge: audio send failed: {err}");
            let sent = telegram_file(
                http,
                token,
                "sendDocument",
                fields,
                "document",
                "voice.wav",
                "audio/wav",
                &wav,
            )
            .await?;
            eprintln!("bridge: voice sent as audio");
            Ok(sent)
        }
    }
}

struct MemorySource {
    cursor: std::io::Cursor<Vec<u8>>,
}

impl Read for MemorySource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.cursor.read(buf)
    }
}

impl Seek for MemorySource {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.cursor.seek(pos)
    }
}

impl symphonia::core::io::MediaSource for MemorySource {
    fn is_seekable(&self) -> bool {
        true
    }

    fn byte_len(&self) -> Option<u64> {
        Some(self.cursor.get_ref().len() as u64)
    }
}

fn ogg_to_wav(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let (rate, channels, pcm) = ogg_pcm(bytes)?;
    Ok(wav_bytes(rate, channels, &pcm))
}

fn ogg_pcm(bytes: &[u8]) -> Result<(u32, u16, Vec<i16>), String> {
    use symphonia::core::audio::SampleBuffer;
    use symphonia::core::codecs::DecoderOptions;
    use symphonia::core::formats::FormatOptions;
    use symphonia::core::io::MediaSourceStream;
    use symphonia::core::meta::MetadataOptions;
    use symphonia::core::probe::Hint;

    let source = MemorySource {
        cursor: std::io::Cursor::new(bytes.to_vec()),
    };
    let stream = MediaSourceStream::new(Box::new(source), Default::default());
    let mut hint = Hint::new();
    hint.with_extension("ogg");
    let probed = symphonia::default::get_probe()
        .format(
            &hint,
            stream,
            &FormatOptions::default(),
            &MetadataOptions::default(),
        )
        .map_err(|_| "voice convert failed".to_owned())?;
    let mut format = probed.format;
    let track = format
        .default_track()
        .ok_or_else(|| "voice convert failed".to_owned())?;
    let track_id = track.id;
    let mut codecs = symphonia::core::codecs::CodecRegistry::new();
    codecs.register_all::<symphonia_adapter_libopus::OpusDecoder>();
    let mut decoder = codecs
        .make(&track.codec_params, &DecoderOptions::default())
        .map_err(|_| "voice convert failed".to_owned())?;
    let mut pcm = Vec::<i16>::new();
    let mut rate = 48_000u32;
    let mut channels = 1u16;
    while let Ok(packet) = format.next_packet() {
        if packet.track_id() != track_id {
            continue;
        }
        let decoded = match decoder.decode(&packet) {
            Ok(decoded) => decoded,
            Err(_) => continue,
        };
        let spec = *decoded.spec();
        rate = spec.rate;
        channels = spec.channels.count() as u16;
        let mut samples = SampleBuffer::<i16>::new(decoded.capacity() as u64, spec);
        samples.copy_interleaved_ref(decoded);
        pcm.extend_from_slice(samples.samples());
    }
    if pcm.is_empty() || rate == 0 || channels == 0 {
        return Err("voice convert failed".to_owned());
    }
    Ok((rate, channels, pcm))
}

fn voice_wave(bytes: &[u8]) -> Vec<u8> {
    match ogg_pcm(bytes) {
        Ok((_, _, pcm)) => peak_wave(&pcm),
        Err(_) => vec![0; 80],
    }
}

fn peak_wave(pcm: &[i16]) -> Vec<u8> {
    let mut wave = vec![0u8; 80];
    if pcm.is_empty() {
        return wave;
    }
    let bucket = (pcm.len() / wave.len()).max(1);
    let mut levels = vec![0i32; wave.len()];
    let mut peak = 1i32;
    for (index, sample) in pcm.iter().enumerate() {
        let bar = (index / bucket).min(wave.len() - 1);
        let level = i32::from(sample.saturating_abs());
        if level > levels[bar] {
            levels[bar] = level;
        }
        if level > peak {
            peak = level;
        }
    }
    for (bar, level) in levels.iter().enumerate() {
        wave[bar] = ((*level * 127) / peak).clamp(0, 127) as u8;
    }
    wave
}

fn wav_bytes(rate: u32, channels: u16, pcm: &[i16]) -> Vec<u8> {
    let data_len = pcm.len() * 2;
    let mut out = Vec::with_capacity(44 + data_len);
    out.extend_from_slice(b"RIFF");
    out.extend_from_slice(&((36 + data_len) as u32).to_le_bytes());
    out.extend_from_slice(b"WAVEfmt ");
    out.extend_from_slice(&16u32.to_le_bytes());
    out.extend_from_slice(&1u16.to_le_bytes());
    out.extend_from_slice(&channels.to_le_bytes());
    out.extend_from_slice(&rate.to_le_bytes());
    out.extend_from_slice(&(rate * u32::from(channels) * 2).to_le_bytes());
    out.extend_from_slice(&(channels * 2).to_le_bytes());
    out.extend_from_slice(&16u16.to_le_bytes());
    out.extend_from_slice(b"data");
    out.extend_from_slice(&(data_len as u32).to_le_bytes());
    for sample in pcm {
        out.extend_from_slice(&sample.to_le_bytes());
    }
    out
}

fn note_telegram_message(
    config: &Config,
    map: &mut HashMap<i64, i64>,
    links: &mut Links,
    tg_id: i64,
    max_chat: i64,
    max_message: i64,
) {
    if config.mode == Mode::Chat {
        map.insert(tg_id, max_chat);
        save_map(&config.map_path, map);
    } else {
        remember_link(links, tg_id, max_message);
        save_links(&config.links_path, links);
    }
}

fn image_mime(bytes: &[u8]) -> &'static str {
    if bytes.starts_with(&[0x89, b'P', b'N', b'G']) {
        "image/png"
    } else if bytes.starts_with(b"GIF8") {
        "image/gif"
    } else if bytes.len() > 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
        "image/webp"
    } else {
        "image/jpeg"
    }
}

fn image_name(mime: &str) -> &'static str {
    match mime {
        "image/png" => "image.png",
        "image/gif" => "image.gif",
        "image/webp" => "image.webp",
        _ => "image.jpg",
    }
}

fn thread_missing(err: &str) -> bool {
    err.to_ascii_lowercase()
        .contains("message thread not found")
}

fn chat_heading(sender: &str, title: &str) -> String {
    match (sender.is_empty(), title.is_empty()) {
        (false, false) if sender != title => format!("{sender} · {title}"),
        (false, _) => sender.to_owned(),
        (_, false) => title.to_owned(),
        _ => "MAX".to_owned(),
    }
}

fn topic_label(chat_id: i64, face: &ChatFace, sender: &str) -> String {
    if chat_id == 0 {
        return text::FAVORITES.to_owned();
    }
    if face.group {
        if face.label.is_empty() {
            "MAX".to_owned()
        } else {
            face.label.clone()
        }
    } else if !sender.is_empty() {
        sender.to_owned()
    } else if !face.label.is_empty() {
        face.label.clone()
    } else {
        "MAX".to_owned()
    }
}

fn named_rich(prefix: &str, text: &str) -> Option<Value> {
    if prefix.is_empty() {
        return None;
    }
    let body = if text == prefix {
        ""
    } else {
        text.strip_prefix(&format!("{prefix}\n\n"))?
    };
    let name = json!({"type": "marked", "text": {"type": "bold", "text": prefix}});
    let pad = clock_pad(if body.is_empty() {
        prefix
    } else {
        last_line(body)
    });
    let mut parts = vec![name];
    if !body.is_empty() {
        parts.push(json!("\n\n"));
        parts.push(json!(body));
    }
    if !pad.is_empty() {
        parts.push(json!(pad));
    }
    Some(json!({"blocks": [{"type": "paragraph", "text": parts}]}))
}

fn last_line(text: &str) -> &str {
    text.rsplit('\n').next().unwrap_or(text)
}

fn clock_pad(line: &str) -> String {
    const ROOM: usize = 36;
    let width = line.chars().count();
    if width >= ROOM {
        String::new()
    } else {
        "\u{2800}".repeat(ROOM - width)
    }
}

fn join_name(name: &str, body: &str) -> String {
    let body = body.trim();
    if body.is_empty() {
        name.to_owned()
    } else {
        format!("{name}\n\n{body}")
    }
}

#[cfg(test)]
fn own_header(body: &str) -> String {
    let body = body.trim();
    if body.is_empty() {
        text::YOU.to_owned()
    } else {
        format!("{}\n\n{body}", text::YOU)
    }
}

fn bold_prefix_entities(prefix: &str, text: &str) -> Option<Value> {
    if prefix.is_empty() {
        return None;
    }
    if text != prefix && !text.starts_with(&format!("{prefix}\n\n")) {
        return None;
    }
    let length = prefix.encode_utf16().count();
    if length == 0 {
        return None;
    }
    Some(json!([{"type": "bold", "offset": 0, "length": length}]))
}

#[cfg(test)]
fn thread_text(group: bool, sender: &str, body: &str) -> String {
    if group && !sender.is_empty() {
        format!("{sender}\n\n{body}")
    } else {
        body.to_owned()
    }
}

fn topic_name(raw: &str) -> String {
    let name: String = raw.chars().take(128).collect();
    if name.is_empty() {
        "MAX".to_owned()
    } else {
        name
    }
}

fn topic_key(tg_user: i64, max_chat: i64) -> String {
    format!("{tg_user}:{max_chat}")
}

async fn download_media(
    client: &Client,
    message: &Message,
    item: &Media,
) -> Result<Vec<u8>, String> {
    match item {
        Media::Photo { url } | Media::Voice { url, .. } => download_public(url).await,
        Media::Video {
            video_id,
            source_chat,
            source_message,
            ..
        } => {
            let own_chat = message
                .chat_id
                .ok_or_else(|| "video chat is missing".to_owned())?;
            let play_chat = (*source_chat).unwrap_or(own_chat);
            let play_message = (*source_message).unwrap_or(message.id);
            let url = match client
                .video_play_url(play_chat, play_message, *video_id)
                .await
            {
                Ok(url) => url,
                Err(err) if play_chat != own_chat || play_message != message.id => client
                    .video_play_url(own_chat, message.id, *video_id)
                    .await
                    .map_err(|fallback| {
                        format!("video play failed: {err}; forward copy failed: {fallback}")
                    })?,
                Err(err) => return Err(err.to_string()),
            };
            download_public(&url).await
        }
    }
}

fn media_caption(mode: Mode, group: bool, sender: &str, title: &str, text: &str) -> String {
    let body = text.trim();
    match mode {
        Mode::Chat => {
            let who = chat_heading(sender, title);
            if body.is_empty() {
                clip_caption(&who)
            } else {
                clip_caption(&format!("{who}\n\n{body}"))
            }
        }
        Mode::Thread => {
            if group && !sender.is_empty() {
                if body.is_empty() {
                    clip_caption(sender)
                } else {
                    clip_caption(&format!("{sender}\n\n{body}"))
                }
            } else {
                clip_caption(body)
            }
        }
    }
}

fn clip_caption(text: &str) -> String {
    const LIMIT: usize = 1024;
    if text.chars().count() <= LIMIT {
        return text.to_owned();
    }
    let mut text: String = text.chars().take(LIMIT - 1).collect();
    text.push('…');
    text
}

fn clip(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        return text.to_owned();
    }
    let mut text: String = text.chars().take(limit).collect();
    text.push_str("\n…");
    text
}

fn topic_chat(topics: &HashMap<String, Topic>, tg_user: i64, thread_id: i64) -> Option<i64> {
    topics.iter().find_map(|(key, topic)| {
        if topic.thread_id != thread_id {
            return None;
        }
        let (user, chat) = key.split_once(':')?;
        if user.parse::<i64>().ok()? != tg_user {
            return None;
        }
        chat.parse().ok()
    })
}

fn topic_reply_target(message: &TgMessage) -> Option<i64> {
    let reply = message.reply_to.as_ref()?;
    if reply.topic_created.is_some() {
        return None;
    }
    if message
        .thread_id
        .is_some_and(|thread| reply.message_id == thread)
    {
        return None;
    }
    Some(reply.message_id)
}

fn is_history_command(text: &str) -> bool {
    let text = text.trim().to_lowercase();
    text == "/history" || text.starts_with("/history@") || text == text::LOAD_HISTORY.to_lowercase()
}

fn is_reread_command(text: &str) -> bool {
    let text = text.trim().to_lowercase();
    text == "/reread" || text.starts_with("/reread@") || text == text::RELOAD_HISTORY.to_lowercase()
}

fn history_body(message: &Message) -> String {
    if !message.text.trim().is_empty() {
        return message.text.clone();
    }
    if !message.media.is_empty() {
        return String::new();
    }
    if message.status.as_deref() == Some("REMOVED") {
        text::DELETED.to_owned()
    } else {
        text::NOT_FORWARDED.to_owned()
    }
}

fn earliest_linked<'a>(messages: &'a [Message], links: &Links) -> Option<&'a Message> {
    messages
        .iter()
        .filter(|message| links.max.contains_key(&message.id))
        .min_by_key(|message| (message.time, message.id))
}

fn needs_topic_rebuild(messages: &[Message], links: &Links) -> bool {
    let Some(earliest) = earliest_linked(messages, links) else {
        return false;
    };
    messages
        .iter()
        .any(|message| (message.time, message.id) < (earliest.time, earliest.id))
}

fn drop_chat_links(links: &mut Links, messages: &[Message]) {
    for message in messages {
        if let Some(tg_id) = links.max.remove(&message.id) {
            links.tg.remove(&tg_id);
        }
    }
}

fn retry_after(err: &str) -> Option<u64> {
    let lower = err.to_ascii_lowercase();
    let rest = lower.split("retry after ").nth(1)?;
    rest.split_whitespace().next()?.parse().ok()
}

async fn read_chat_history(client: &Client, chat_id: i64) -> Result<Vec<Message>, String> {
    let mut from = unix_ms();
    let mut all: Vec<Message> = Vec::new();
    let mut capped = true;
    for _ in 0..100 {
        let batch = client
            .fetch_history(chat_id, 40, from)
            .await
            .map_err(|err| err.to_string())?;
        if batch.is_empty() {
            capped = false;
            break;
        }
        let known: std::collections::HashSet<i64> = all.iter().map(|message| message.id).collect();
        let fresh = batch.iter().any(|message| !known.contains(&message.id));
        let oldest = batch
            .iter()
            .filter_map(|message| (message.time > 0).then_some(message.time))
            .min()
            .unwrap_or(from);
        all.extend(batch);
        if !fresh || oldest >= from {
            capped = false;
            break;
        }
        from = oldest;
    }
    if capped {
        eprintln!("bridge: history stopped after 100 pages for chat {chat_id}");
    }
    all.sort_by_key(|message| (message.time, message.id));
    all.dedup_by(|left, right| left.id == right.id);
    stamp_history_chat(&mut all, chat_id);
    Ok(all)
}

fn stamp_history_chat(messages: &mut [Message], chat_id: i64) {
    for message in messages {
        if message.chat_id.is_none() {
            message.chat_id = Some(chat_id);
        }
    }
}

fn stray_command_topic(
    message: &TgMessage,
    topics: &HashMap<String, Topic>,
    tg_user: i64,
) -> Option<i64> {
    let thread_id = message.thread_id?;
    if thread_id <= 1 || topic_chat(topics, tg_user, thread_id).is_some() {
        return None;
    }
    Some(thread_id)
}

async fn wipe_bot_chat(
    config: &Config,
    http: &mut Http,
    map: &mut HashMap<i64, i64>,
    links: &mut Links,
    topics: &mut HashMap<String, Topic>,
    tg_user: i64,
) {
    let prefix = format!("{tg_user}:");
    let owned: Vec<(String, i64)> = topics
        .iter()
        .filter(|(key, _)| key.starts_with(&prefix))
        .map(|(key, topic)| (key.clone(), topic.thread_id))
        .collect();
    for (key, thread_id) in owned {
        if let Err(err) = delete_topic(http, config, tg_user, thread_id).await {
            eprintln!("bridge: topic delete failed: {err}");
        }
        topics.remove(&key);
    }
    save_topics(&config.topics_path, topics);
    let mut message_ids: Vec<i64> = links.tg.keys().copied().collect();
    message_ids.extend(map.keys().copied());
    message_ids.sort_unstable();
    message_ids.dedup();
    for message_id in message_ids {
        let mut attempt = 0;
        loop {
            match telegram(
                http,
                &config.token,
                "deleteMessage",
                json!({"chat_id": tg_user, "message_id": message_id}),
            )
            .await
            {
                Ok(_) => break,
                Err(err) => {
                    if let Some(wait) = retry_after(&err) {
                        if attempt < 2 {
                            attempt += 1;
                            tokio::time::sleep(Duration::from_secs(wait.max(1))).await;
                            continue;
                        }
                    }
                    break;
                }
            }
        }
    }
    links.tg.clear();
    links.max.clear();
    save_links(&config.links_path, links);
    map.clear();
    save_map(&config.map_path, map);
    eprintln!("bridge: cleared topics and bot history");
}

async fn delete_topic(
    http: &Http,
    config: &Config,
    tg_user: i64,
    thread_id: i64,
) -> Result<(), String> {
    telegram(
        http,
        &config.token,
        "deleteForumTopic",
        json!({
            "chat_id": tg_user,
            "message_thread_id": thread_id
        }),
    )
    .await
    .map(|_| ())
}

async fn open_history_topic(
    config: &Config,
    http: &mut Http,
    client: &mut Client,
    topics: &mut HashMap<String, Topic>,
    tg_user: i64,
    chat_id: i64,
) -> Result<(), String> {
    let face = client.chat_face(chat_id).await;
    let name = topic_name(&topic_label(chat_id, &face, ""));
    let key = topic_key(tg_user, chat_id);
    for attempt in 0..3 {
        match ensure_topic(http, config, client, topics, tg_user, chat_id, &key, &name).await {
            Ok(_) => return Ok(()),
            Err(err) => {
                if let Some(wait) = retry_after(&err) {
                    if attempt < 2 {
                        tokio::time::sleep(Duration::from_secs(wait.max(1))).await;
                        continue;
                    }
                }
                return Err(err);
            }
        }
    }
    Err("history topic was not created".into())
}

async fn load_topic_history(
    config: &Config,
    http: &mut Http,
    client: &mut Client,
    map: &mut HashMap<i64, i64>,
    links: &mut Links,
    topics: &mut HashMap<String, Topic>,
    seen: &mut HashMap<i64, Cursor>,
    me: Option<i64>,
    tg_user: i64,
    chat_id: i64,
    fresh: bool,
) -> Result<(), String> {
    let history = read_chat_history(client, chat_id).await?;
    let mut rewrite = fresh;
    if !fresh && needs_topic_rebuild(&history, links) {
        let key = topic_key(tg_user, chat_id);
        if let Some(topic) = topics.remove(&key) {
            if let Err(err) = delete_topic(http, config, tg_user, topic.thread_id).await {
                topics.insert(key, topic);
                save_topics(&config.topics_path, topics);
                return Err(err);
            }
            save_topics(&config.topics_path, topics);
            drop_chat_links(links, &history);
            save_links(&config.links_path, links);
            open_history_topic(config, http, client, topics, tg_user, chat_id).await?;
            eprintln!("bridge: topic recreated for chat {chat_id}");
            rewrite = true;
        }
    }
    let mut wrote = 0u32;
    for message in &history {
        if !rewrite && links.max.contains_key(&message.id) {
            continue;
        }
        let mut outgoing = message.clone();
        outgoing.text = history_body(message);
        let mut sent = false;
        for attempt in 0..3 {
            match forward_max(
                config, http, client, map, links, topics, &outgoing, me, true,
            )
            .await
            {
                Ok(()) => {
                    sent = true;
                    break;
                }
                Err(err) => {
                    if let Some(wait) = retry_after(&err) {
                        if attempt < 2 {
                            tokio::time::sleep(Duration::from_secs(wait.max(1))).await;
                            continue;
                        }
                    }
                    eprintln!("bridge: history send failed: {err}");
                    if let Ok(fresh) = telegram_http().await {
                        *http = fresh;
                    }
                    break;
                }
            }
        }
        if sent {
            wrote += 1;
            mark_seen(seen, &config.seen_path, message);
        }
    }
    if wrote > 0 {
        eprintln!("bridge: history wrote {wrote} messages for chat {chat_id}");
    }
    Ok(())
}

fn history_chat_ids(
    known: &[i64],
    seen: &HashMap<i64, Cursor>,
    topics: &HashMap<String, Topic>,
    tg_user: i64,
) -> Vec<i64> {
    let mut ids = known.to_vec();
    for chat_id in seen.keys().copied() {
        if !ids.contains(&chat_id) {
            ids.push(chat_id);
        }
    }
    let prefix = format!("{tg_user}:");
    for key in topics.keys() {
        let Some(chat) = key.strip_prefix(&prefix) else {
            continue;
        };
        let Ok(chat_id) = chat.parse::<i64>() else {
            continue;
        };
        if !ids.contains(&chat_id) {
            ids.push(chat_id);
        }
    }
    ids.sort_unstable();
    ids
}

async fn load_all_history(
    config: &Config,
    http: &mut Http,
    client: &mut Client,
    map: &mut HashMap<i64, i64>,
    links: &mut Links,
    topics: &mut HashMap<String, Topic>,
    seen: &mut HashMap<i64, Cursor>,
    me: Option<i64>,
    tg_user: i64,
) -> Result<(), String> {
    let known = client.chat_ids().await.map_err(|err| err.to_string())?;
    let chats = history_chat_ids(&known, seen, topics, tg_user);
    eprintln!("bridge: history found {} chats", chats.len());
    let mut ready = Vec::new();
    for chat_id in chats {
        let fresh = !topics.contains_key(&topic_key(tg_user, chat_id));
        if let Err(err) = open_history_topic(config, http, client, topics, tg_user, chat_id).await {
            eprintln!("bridge: history topic failed for chat {chat_id}: {err}");
            continue;
        }
        ready.push((chat_id, fresh));
    }
    eprintln!("bridge: history opened {} topics", ready.len());
    for (chat_id, fresh) in ready {
        if let Err(err) = load_topic_history(
            config, http, client, map, links, topics, seen, me, tg_user, chat_id, fresh,
        )
        .await
        {
            eprintln!("bridge: history load failed for chat {chat_id}: {err}");
        }
    }
    Ok(())
}

async fn run_history(
    config: &Config,
    http: &mut Http,
    client: &mut Client,
    map: &mut HashMap<i64, i64>,
    links: &mut Links,
    topics: &mut HashMap<String, Topic>,
    seen: &mut HashMap<i64, Cursor>,
    me: Option<i64>,
    tg_user: i64,
    status_thread: Option<i64>,
) -> Result<(), String> {
    if config.mode != Mode::Thread {
        let _ = telegram(
            http,
            &config.token,
            "sendMessage",
            json!({
                "chat_id": tg_user,
                "text": text::HISTORY_NEEDS_THREAD,
            }),
        )
        .await;
        return Ok(());
    }
    let mut note = json!({
        "chat_id": tg_user,
        "text": text::HISTORY_LOADING,
    });
    if let Some(thread_id) = status_thread {
        note["message_thread_id"] = json!(thread_id);
    }
    let _ = telegram(http, &config.token, "sendMessage", note).await;
    load_all_history(config, http, client, map, links, topics, seen, me, tg_user).await?;
    let mut done = json!({
        "chat_id": tg_user,
        "text": text::HISTORY_DONE,
    });
    if let Some(thread_id) = status_thread {
        done["message_thread_id"] = json!(thread_id);
    }
    let _ = telegram(http, &config.token, "sendMessage", done).await;
    Ok(())
}

async fn handle_update(
    config: &Config,
    http: &mut Http,
    client: &mut Option<Client>,
    link: &mut LinkState,
    map: &mut HashMap<i64, i64>,
    links: &mut Links,
    topics: &mut HashMap<String, Topic>,
    seen: &mut HashMap<i64, Cursor>,
    echoes: &mut HashSet<i64>,
    me: &mut Option<i64>,
    panels: &mut HashMap<i64, i64>,
    logout_asks: &mut HashMap<i64, i64>,
    update: &Update,
) -> Result<(), String> {
    if let Some(callback) = &update.callback {
        let _ = telegram(
            http,
            &config.token,
            "answerCallbackQuery",
            json!({"callback_query_id": callback.id}),
        )
        .await;
        let action = callback.data.as_deref();
        if !config.allowed.contains(&callback.from.id) {
            return Ok(());
        }
        let user_id = callback.from.id;
        match action {
            Some("logout") => {
                begin_logout(config, http, client, link, me, panels, logout_asks, user_id).await;
                return Ok(());
            }
            Some("logout_yes") => {
                wipe_bot_chat(config, http, map, links, topics, user_id).await;
                seen.clear();
                save_seen(&config.seen_path, seen);
                close_logout_ask(http, config, logout_asks, user_id).await;
                sync_controls(http, config, *link, panels).await;
                return Ok(());
            }
            Some("logout_no") => {
                close_logout_ask(http, config, logout_asks, user_id).await;
                sync_controls(http, config, *link, panels).await;
                return Ok(());
            }
            Some("history") | Some("reread") if history_allowed(*link) => {}
            _ => return Ok(()),
        }
        let Some(active) = client.as_mut() else {
            return Ok(());
        };
        if action == Some("reread") {
            wipe_bot_chat(config, http, map, links, topics, user_id).await;
        }
        let status_thread = if action == Some("reread") {
            None
        } else {
            callback.message.as_ref().and_then(|message| {
                message
                    .thread_id
                    .filter(|thread| topic_chat(topics, user_id, *thread).is_some())
            })
        };
        return run_history(
            config,
            http,
            active,
            map,
            links,
            topics,
            seen,
            *me,
            user_id,
            status_thread,
        )
        .await;
    }
    let Some(message) = &update.message else {
        return Ok(());
    };
    let Some(from) = message.from.as_ref().map(|user| user.id) else {
        return Ok(());
    };
    if !config.allowed.contains(&from) {
        return Ok(());
    }
    let text = inbound_text(message);
    if is_setup_command(&text) {
        if *link != LinkState::Off {
            return Ok(());
        }
        forget_session(&config.session_path);
        sync_controls(http, config, *link, panels).await;
        return Ok(());
    }
    if is_logout_command(&text) {
        begin_logout(config, http, client, link, me, panels, logout_asks, from).await;
        return Ok(());
    }
    if is_history_command(&text) || is_reread_command(&text) {
        if !history_allowed(*link) {
            return Ok(());
        }
        let Some(active) = client.as_mut() else {
            return Ok(());
        };
        let reread = is_reread_command(&text);
        let status_thread = message
            .thread_id
            .filter(|thread| topic_chat(topics, from, *thread).is_some());
        if let Some(thread_id) = stray_command_topic(message, topics, from) {
            if let Err(err) = delete_topic(http, config, from, thread_id).await {
                eprintln!("bridge: history command topic delete failed: {err}");
            } else {
                eprintln!("bridge: removed the history command topic");
            }
        }
        if reread {
            wipe_bot_chat(config, http, map, links, topics, from).await;
        }
        return run_history(
            config,
            http,
            active,
            map,
            links,
            topics,
            seen,
            *me,
            from,
            if reread { None } else { status_thread },
        )
        .await;
    }
    if !history_allowed(*link) {
        return Ok(());
    }
    let Some(client) = client.as_mut() else {
        return Ok(());
    };
    let photo_id = largest_file_id(&message.photo);
    let voice = message
        .voice
        .as_ref()
        .filter(|voice| !voice.file_id.is_empty());
    let video_clip = inbound_video(message);
    if text.is_empty() && photo_id.is_none() && voice.is_none() && video_clip.is_none() {
        return Ok(());
    }
    let (chat_id, reply_to) = if config.mode == Mode::Thread {
        let Some(thread_id) = message.thread_id else {
            return Ok(());
        };
        let Some(chat_id) = topic_chat(topics, from, thread_id) else {
            return Ok(());
        };
        let reply_to = match topic_reply_target(message) {
            Some(tg_id) => match links.tg.get(&tg_id).copied() {
                Some(max_id) => Some(max_id),
                None => {
                    eprintln!("bridge: reply target {tg_id} has no MAX message");
                    None
                }
            },
            None => None,
        };
        (chat_id, reply_to)
    } else {
        let Some(reply_id) = message.reply_to.as_ref().map(|reply| reply.message_id) else {
            let _ = telegram(
                http,
                &config.token,
                "sendMessage",
                json!({
                    "chat_id": from,
                    "text": "Reply to a MAX message to send the answer back."
                }),
            )
            .await;
            return Ok(());
        };
        let Some(chat_id) = map.get(&reply_id).copied() else {
            return Ok(());
        };
        (chat_id, None)
    };
    let voice_file = match voice {
        Some(voice) => match fetch_telegram_file(http, &config.token, &voice.file_id).await {
            Ok(bytes) => Some((bytes, voice.duration.saturating_mul(1000).max(1000))),
            Err(err) => {
                eprintln!("bridge: voice download failed: {err}");
                None
            }
        },
        None => None,
    };
    let video_file = match video_clip {
        Some((file_id, video_type)) => {
            match fetch_telegram_file(http, &config.token, file_id).await {
                Ok(bytes) => Some((bytes, video_type)),
                Err(err) => {
                    eprintln!("bridge: video download failed: {err}");
                    None
                }
            }
        }
        None => None,
    };
    let attaches = incoming_attaches(http, client, &config.token, photo_id.as_deref()).await;
    if attaches.is_empty() && text.is_empty() && voice_file.is_none() && video_file.is_none() {
        return Ok(());
    }
    let sent = if let Some((bytes, duration_ms)) = voice_file {
        let sent = send_voice_on_tcp(
            client,
            chat_id,
            &text,
            reply_to,
            &bytes,
            duration_ms,
            attaches,
        )
        .await?;
        if let Some(max_id) = sent {
            echoes.insert(max_id);
        }
        sent
    } else if let Some((bytes, video_type)) = video_file {
        let body = if video_type == Some(1) {
            ""
        } else {
            text.as_str()
        };
        let sent = send_video_on_tcp(
            client, chat_id, body, reply_to, &bytes, video_type, attaches,
        )
        .await?;
        if let Some(max_id) = sent {
            echoes.insert(max_id);
        }
        sent
    } else {
        send_with_voice_retry(client, chat_id, &text, reply_to, attaches).await?
    };
    if config.mode == Mode::Thread && message.message_id != 0 {
        if let Some(max_id) = sent {
            remember_link(links, message.message_id, max_id);
            save_links(&config.links_path, links);
        }
    }
    Ok(())
}

fn inbound_text(message: &TgMessage) -> String {
    let caption = message.caption.as_deref().unwrap_or("");
    if !caption.is_empty() {
        return caption.to_owned();
    }
    message.text.clone().unwrap_or_default()
}

fn inbound_video(message: &TgMessage) -> Option<(&str, Option<i64>)> {
    if let Some(note) = message
        .video_note
        .as_ref()
        .filter(|note| !note.file_id.is_empty())
    {
        return Some((note.file_id.as_str(), Some(1)));
    }
    if let Some(animation) = message
        .animation
        .as_ref()
        .filter(|animation| !animation.file_id.is_empty())
    {
        return Some((animation.file_id.as_str(), None));
    }
    message
        .video
        .as_ref()
        .filter(|video| !video.file_id.is_empty())
        .map(|video| (video.file_id.as_str(), None))
}

fn largest_file_id(photos: &[PhotoSize]) -> Option<String> {
    photos
        .iter()
        .filter(|photo| !photo.file_id.is_empty())
        .max_by_key(|photo| {
            (
                photo.file_size.unwrap_or(0),
                photo.width.saturating_mul(photo.height),
            )
        })
        .map(|photo| photo.file_id.clone())
}

async fn incoming_attaches(
    http: &Http,
    client: &Client,
    token: &str,
    photo_id: Option<&str>,
) -> Vec<Attach> {
    let mut attaches = Vec::new();
    if let Some(file_id) = photo_id {
        match fetch_telegram_file(http, token, file_id).await {
            Ok(bytes) => match upload_photo(client, &bytes).await {
                Ok(token) => attaches.push(Attach::Photo { token }),
                Err(err) => eprintln!("bridge: photo upload failed: {err}"),
            },
            Err(err) => eprintln!("bridge: photo download failed: {err}"),
        }
    }
    attaches
}

async fn telegram_http() -> Result<Http, String> {
    let addr = lookup_v4(TELEGRAM_HOST, 443)
        .await
        .map_err(|err| err.to_string())?;
    Http::builder()
        .resolve(TELEGRAM_HOST, SocketAddr::new(addr.ip(), 443))
        .timeout(Duration::from_secs(40))
        .build()
        .map_err(|err| err.to_string())
}

async fn poll_telegram(http: &Http, token: &str, offset: i64) -> Result<Vec<Update>, String> {
    let url = format!("https://api.telegram.org/bot{token}/getUpdates");
    let response = http
        .get(url)
        .query(&[("timeout", "25"), ("offset", &offset.to_string())])
        .send()
        .await
        .map_err(|err| err.to_string())?;
    let status = response.status();
    let body: Value = response.json().await.map_err(|err| err.to_string())?;
    if status.as_u16() == 409 {
        return Err(
            "telegram getUpdates returned 409 Conflict: another bridge is already polling this bot"
                .into(),
        );
    }
    if !status.is_success() || body.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err(format!("telegram getUpdates returned {status}"));
    }
    let updates = serde_json::from_value(body.get("result").cloned().unwrap_or(json!([])))
        .map_err(|err| err.to_string())?;
    Ok(updates)
}

async fn download_public(raw: &str) -> Result<Vec<u8>, String> {
    let url = reqwest::Url::parse(raw).map_err(|_| "media download failed".to_owned())?;
    if url.scheme() != "https" {
        return Err("media url is not https".into());
    }
    let host = url
        .host_str()
        .ok_or_else(|| "media download failed".to_owned())?;
    let port = url.port_or_known_default().unwrap_or(443);
    let http = public_http(host, port).await?;
    let response = http
        .get(url)
        .send()
        .await
        .map_err(|_| "media download failed".to_owned())?;
    if !response.status().is_success() {
        return Err(format!("media download failed: {}", response.status()));
    }
    read_limited(response).await
}

async fn public_http(host: &str, port: u16) -> Result<Http, String> {
    let addr = lookup_v4(host, port)
        .await
        .map_err(|_| "media host has no IPv4 address".to_owned())?;
    Http::builder()
        .resolve(host, SocketAddr::new(addr.ip(), port))
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|_| "media download failed".to_owned())
}

async fn read_limited(mut response: reqwest::Response) -> Result<Vec<u8>, String> {
    if response.content_length().unwrap_or(0) > MAX_MEDIA_BYTES as u64 {
        return Err("media is larger than 20 MB".into());
    }
    let mut out = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| "media download failed".to_owned())?
    {
        if out.len().saturating_add(chunk.len()) > MAX_MEDIA_BYTES {
            return Err("media is larger than 20 MB".into());
        }
        out.extend_from_slice(&chunk);
    }
    if out.is_empty() {
        return Err("media is empty".into());
    }
    Ok(out)
}

async fn fetch_telegram_file(http: &Http, token: &str, file_id: &str) -> Result<Vec<u8>, String> {
    let sent = telegram(http, token, "getFile", json!({"file_id": file_id})).await?;
    let path = sent
        .get("result")
        .and_then(|result| result.get("file_path"))
        .and_then(Value::as_str)
        .filter(|path| !path.is_empty())
        .ok_or_else(|| "telegram getFile did not return a file".to_owned())?;
    let url = format!("https://api.telegram.org/file/bot{token}/{path}");
    let response = http
        .get(url)
        .send()
        .await
        .map_err(|_| "media download failed".to_owned())?;
    if !response.status().is_success() {
        return Err(format!("media download failed: {}", response.status()));
    }
    read_limited(response).await
}

async fn upload_photo(client: &Client, bytes: &[u8]) -> Result<String, String> {
    if bytes.is_empty() || bytes.len() > MAX_MEDIA_BYTES {
        return Err(media_size_error(bytes.len()));
    }
    let url = client
        .photo_upload_url()
        .await
        .map_err(|_| "photo upload failed".to_owned())?;
    let photo_id = photo_id_from_url(&url);
    let mime = image_mime(bytes);
    let part = Part::bytes(bytes.to_vec())
        .file_name(image_name(mime))
        .mime_str(mime)
        .map_err(|_| "photo upload failed".to_owned())?;
    let response = post_https(&url, "photo upload failed", |builder| {
        builder.multipart(Form::new().part("file", part))
    })
    .await?;
    let body: Value = response
        .json()
        .await
        .map_err(|_| "photo upload failed".to_owned())?;
    let photos = body
        .get("photos")
        .and_then(Value::as_object)
        .ok_or_else(|| "photo upload did not return a token".to_owned())?;
    photos
        .get(photo_id.as_deref().unwrap_or(""))
        .or_else(|| photos.values().next())
        .and_then(|item| item.get("token"))
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| "photo upload did not return a token".to_owned())
}

fn photo_id_from_url(raw: &str) -> Option<String> {
    let url = reqwest::Url::parse(raw).ok()?;
    url.query_pairs()
        .find(|(key, _)| key == "photoIds")
        .map(|(_, value)| value.into_owned())
        .filter(|value| !value.is_empty())
}

async fn send_voice_on_tcp(
    client: &Client,
    chat_id: i64,
    text: &str,
    reply_to: Option<i64>,
    bytes: &[u8],
    duration_ms: i64,
    photos: Vec<Attach>,
) -> Result<Option<i64>, String> {
    let mut link = client.open_voice().await.map_err(|err| err.to_string())?;
    let ticket = link.ticket().await.map_err(|err| err.to_string())?;
    let (audio_id, token) = upload_voice(&ticket.url, ticket.audio_id, ticket.token, bytes).await?;
    let wave = voice_wave(bytes);
    link.finish(
        chat_id,
        text,
        reply_to,
        audio_id,
        &token,
        duration_ms,
        &wave,
        &photos,
    )
    .await
    .map(|stamp| stamp.map(|(id, _)| id))
    .map_err(|err| err.to_string())
}

async fn send_video_on_tcp(
    client: &Client,
    chat_id: i64,
    text: &str,
    reply_to: Option<i64>,
    bytes: &[u8],
    video_type: Option<i64>,
    photos: Vec<Attach>,
) -> Result<Option<i64>, String> {
    let mut link = client.open_voice().await.map_err(|err| err.to_string())?;
    let ticket = link.ticket_video().await.map_err(|err| err.to_string())?;
    let (video_id, token) = upload_video(&ticket.url, ticket.video_id, ticket.token, bytes).await?;
    link.finish_video(
        chat_id, text, reply_to, video_id, &token, video_type, &photos,
    )
    .await
    .map(|stamp| stamp.map(|(id, _)| id))
    .map_err(|err| err.to_string())
}

async fn upload_video(
    url: &str,
    video_id: i64,
    fallback_token: String,
    bytes: &[u8],
) -> Result<(i64, String), String> {
    if bytes.is_empty() || bytes.len() > MAX_MEDIA_BYTES {
        return Err(media_size_error(bytes.len()));
    }
    let len = bytes.len();
    eprintln!(
        "bridge: video upload bytes={len} place={}",
        upload_place(url)
    );
    let body = bytes.to_vec();
    let range = content_range_header(len);
    let response = post_https(url, "video upload failed", |builder| {
        builder
            .header(
                "User-Agent",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
            )
            .header("Content-Disposition", "attachment; filename=video.mp4")
            .header("Content-Type", "application/x-binary")
            .header("Content-Length", len.to_string())
            .header("Content-Range", range)
            .header("X-Uploading-Mode", "parallel")
            .header("Connection", "keep-alive")
            .body(body)
    })
    .await?;
    let body = response
        .text()
        .await
        .map_err(|_| "video upload failed".to_owned())?;
    eprintln!("bridge: video upload body {}", upload_body_note(&body));
    if let Some(err) = upload_error(&body) {
        return Err(err);
    }
    let token = uploaded_token(&body).unwrap_or(fallback_token);
    Ok((video_id, token))
}

async fn upload_voice(
    url: &str,
    audio_id: i64,
    fallback_token: String,
    bytes: &[u8],
) -> Result<(i64, String), String> {
    if bytes.is_empty() || bytes.len() > MAX_MEDIA_BYTES {
        return Err(media_size_error(bytes.len()));
    }
    let ticket_url = url;
    let ticket_token = fallback_token;
    let len = bytes.len();
    eprintln!(
        "bridge: voice upload bytes={len} ogg={} place={}",
        if bytes.starts_with(b"OggS") {
            "yes"
        } else {
            "no"
        },
        upload_place(ticket_url)
    );
    let body = bytes.to_vec();
    let range = content_range_header(len);
    let response = post_https(ticket_url, "voice upload failed", |builder| {
        builder
            .header(
                "User-Agent",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
            )
            .header("Content-Disposition", "attachment; filename=voice.ogg")
            .header("Content-Type", "application/octet-stream")
            .header("Content-Length", len.to_string())
            .header("Content-Range", range)
            .header("Connection", "keep-alive")
            .body(body)
    })
    .await?;
    let body = response
        .text()
        .await
        .map_err(|_| "voice upload failed".to_owned())?;
    eprintln!("bridge: voice upload body {}", upload_body_note(&body));
    if let Some(err) = upload_error(&body) {
        return Err(err);
    }
    let token = uploaded_token(&body).unwrap_or(ticket_token);
    Ok((audio_id, token))
}

async fn send_with_voice_retry(
    client: &Client,
    chat_id: i64,
    text: &str,
    reply_to: Option<i64>,
    attaches: Vec<Attach>,
) -> Result<Option<i64>, String> {
    match client
        .send_text(chat_id, text, reply_to, attaches.clone())
        .await
    {
        Ok(sent) => Ok(sent),
        Err(err) if voice_rejected(&err) => {
            if let Some(id) = voice_audio_id(&attaches) {
                let _ = client.wait_voice(id).await;
                return client
                    .send_text(chat_id, text, reply_to, attaches)
                    .await
                    .map_err(|err| err.to_string());
            }
            Err(err.to_string())
        }
        Err(err) => Err(err.to_string()),
    }
}

fn voice_audio_id(attaches: &[Attach]) -> Option<i64> {
    attaches.iter().find_map(|attach| match attach {
        Attach::Voice { audio_id, .. } => Some(*audio_id),
        Attach::Photo { .. } | Attach::Video { .. } => None,
    })
}

fn voice_rejected(err: &impl std::fmt::Display) -> bool {
    let text = err.to_string();
    text.contains("Invalid attachment") || text.contains("not.ready")
}

fn upload_error(body: &str) -> Option<String> {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.starts_with('<') {
        return Some("voice upload rejected".into());
    }
    let value: Value = serde_json::from_str(trimmed).ok()?;
    let failed = value.get("error").is_some()
        || value.get("errorCode").is_some()
        || value.get("error_code").is_some();
    if !failed {
        return None;
    }
    let message = value
        .get("message")
        .or_else(|| value.get("error"))
        .and_then(Value::as_str)
        .filter(|text| {
            text.len() < 80
                && !text.contains('=')
                && !text.contains('/')
                && !text.contains('?')
                && !text.contains('&')
        });
    Some(match message {
        Some(text) => format!("voice upload rejected: {text}"),
        None => "voice upload rejected".into(),
    })
}

fn upload_place(url: &str) -> &'static str {
    let path = url.split('?').next().unwrap_or(url);
    if path.ends_with("/uploadAudio") {
        "audio"
    } else if path.ends_with("/uploadVideo") {
        "video"
    } else if url.contains("okcdn") {
        "cdn"
    } else {
        "other"
    }
}

fn content_range_header(len: usize) -> String {
    let end = len.saturating_sub(1);
    format!("bytes 0-{end}/{len}")
}

fn upload_body_note(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return "empty".into();
    }
    let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
        return format!("len={}", trimmed.len());
    };
    let Some(object) = value.as_object() else {
        return format!("len={}", trimmed.len());
    };
    let keys = object.keys().cloned().collect::<Vec<_>>().join(",");
    if keys.is_empty() {
        "keys".into()
    } else {
        format!("keys={keys}")
    }
}

fn uploaded_token(body: &str) -> Option<String> {
    let value: Value = serde_json::from_str(body.trim()).ok()?;
    value
        .get("token")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
}

fn media_size_error(len: usize) -> String {
    if len == 0 {
        "media is empty".into()
    } else {
        "media is larger than 20 MB".into()
    }
}

async fn post_https(
    raw: &str,
    failure: &str,
    build: impl FnOnce(reqwest::RequestBuilder) -> reqwest::RequestBuilder,
) -> Result<reqwest::Response, String> {
    let url = reqwest::Url::parse(raw).map_err(|_| failure.to_owned())?;
    if url.scheme() != "https" {
        return Err("media url is not https".into());
    }
    let host = url.host_str().ok_or_else(|| failure.to_owned())?;
    let port = url.port_or_known_default().unwrap_or(443);
    let http = public_http(host, port)
        .await
        .map_err(|_| failure.to_owned())?;
    let response = build(http.post(url))
        .send()
        .await
        .map_err(|_| failure.to_owned())?;
    if !response.status().is_success() {
        return Err(format!("{failure}: {}", response.status()));
    }
    Ok(response)
}

async fn telegram_file(
    http: &Http,
    token: &str,
    method: &str,
    fields: Vec<(String, String)>,
    field: &str,
    filename: &str,
    mime: &str,
    bytes: &[u8],
) -> Result<Value, String> {
    let part = Part::bytes(bytes.to_vec())
        .file_name(filename.to_owned())
        .mime_str(mime)
        .map_err(|err| err.to_string())?;
    let mut form = Form::new();
    for (key, value) in fields {
        form = form.text(key, value);
    }
    form = form.part(field.to_owned(), part);
    let url = format!("https://api.telegram.org/bot{token}/{method}");
    let response = http
        .post(url)
        .multipart(form)
        .send()
        .await
        .map_err(|err| err.to_string())?;
    let status = response.status();
    let payload: Value = response.json().await.map_err(|err| err.to_string())?;
    if !status.is_success() || payload.get("ok").and_then(Value::as_bool) != Some(true) {
        let description = payload
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("request failed");
        return Err(format!(
            "telegram {method} returned {status}: {description}"
        ));
    }
    Ok(payload)
}

async fn telegram(http: &Http, token: &str, method: &str, body: Value) -> Result<Value, String> {
    let url = format!("https://api.telegram.org/bot{token}/{method}");
    let response = http
        .post(url)
        .json(&body)
        .send()
        .await
        .map_err(|err| err.to_string())?;
    let status = response.status();
    let payload: Value = response.json().await.map_err(|err| err.to_string())?;
    if !status.is_success() || payload.get("ok").and_then(Value::as_bool) != Some(true) {
        let description = payload
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("request failed");
        return Err(format!(
            "telegram {method} returned {status}: {description}"
        ));
    }
    Ok(payload)
}

#[derive(Debug, Deserialize)]
struct Update {
    update_id: i64,
    #[serde(default)]
    message: Option<TgMessage>,
    #[serde(default, rename = "callback_query")]
    callback: Option<CallbackQuery>,
}

#[derive(Debug, Deserialize)]
struct CallbackQuery {
    id: String,
    from: TgUser,
    #[serde(default)]
    data: Option<String>,
    #[serde(default)]
    message: Option<TgMessage>,
}

#[derive(Debug, Deserialize)]
struct TgMessage {
    #[serde(default)]
    message_id: i64,
    #[serde(default)]
    from: Option<TgUser>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    caption: Option<String>,
    #[serde(default)]
    photo: Vec<PhotoSize>,
    #[serde(default)]
    voice: Option<TgVoice>,
    #[serde(default)]
    video: Option<TgFile>,
    #[serde(default)]
    video_note: Option<TgFile>,
    #[serde(default)]
    animation: Option<TgFile>,
    #[serde(default, rename = "message_thread_id")]
    thread_id: Option<i64>,
    #[serde(default, rename = "reply_to_message")]
    reply_to: Option<Reply>,
}

#[derive(Debug, Deserialize)]
struct TgUser {
    id: i64,
}

#[derive(Debug, Deserialize)]
struct PhotoSize {
    file_id: String,
    #[serde(default)]
    width: i64,
    #[serde(default)]
    height: i64,
    #[serde(default)]
    file_size: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct TgVoice {
    file_id: String,
    #[serde(default)]
    duration: i64,
}

#[derive(Debug, Deserialize)]
struct TgFile {
    file_id: String,
}

#[derive(Debug, Deserialize)]
struct Reply {
    message_id: i64,
    #[serde(default, rename = "forum_topic_created")]
    topic_created: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
struct Topic {
    thread_id: i64,
    name: String,
    #[serde(default)]
    photo: bool,
}

fn load_topics(path: &std::path::Path) -> HashMap<String, Topic> {
    let Ok(raw) = fs::read_to_string(path) else {
        return HashMap::new();
    };
    serde_json::from_str(&raw).unwrap_or_default()
}

fn save_topics(path: &std::path::Path, topics: &HashMap<String, Topic>) {
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(raw) = serde_json::to_string(topics) {
        let _ = fs::write(path, raw);
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct Links {
    #[serde(default)]
    tg: HashMap<i64, i64>,
    #[serde(default)]
    max: HashMap<i64, i64>,
}

fn load_links(path: &std::path::Path) -> Links {
    let Ok(raw) = fs::read_to_string(path) else {
        return Links::default();
    };
    serde_json::from_str(&raw).unwrap_or_default()
}

fn save_links(path: &std::path::Path, links: &Links) {
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(raw) = serde_json::to_string(links) {
        let _ = fs::write(path, raw);
    }
}

fn remember_link(links: &mut Links, tg_message: i64, max_message: i64) {
    links.tg.insert(tg_message, max_message);
    links.max.insert(max_message, tg_message);
}

fn load_seen(path: &std::path::Path) -> HashMap<i64, Cursor> {
    let Ok(raw) = fs::read_to_string(path) else {
        return HashMap::new();
    };
    serde_json::from_str(&raw).unwrap_or_default()
}

fn save_seen(path: &std::path::Path, seen: &HashMap<i64, Cursor>) {
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(raw) = serde_json::to_string(seen) {
        let _ = fs::write(path, raw);
    }
}

fn load_map(path: &std::path::Path) -> HashMap<i64, i64> {
    let Ok(raw) = fs::read_to_string(path) else {
        return HashMap::new();
    };
    serde_json::from_str(&raw).unwrap_or_default()
}

fn save_map(path: &std::path::Path, map: &HashMap<i64, i64>) {
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(raw) = serde_json::to_string(map) {
        let _ = fs::write(path, raw);
    }
}

fn load_offset(path: &std::path::Path) -> i64 {
    fs::read_to_string(path)
        .ok()
        .and_then(|raw| raw.trim().parse().ok())
        .unwrap_or(0)
}

fn save_offset(path: &std::path::Path, offset: i64) {
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let _ = fs::write(path, offset.to_string());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_forward_uses_the_original_sender_as_its_header() {
        assert_eq!(
            header_prefix(Some("Аня Н"), false, true, 9, "Петр"),
            format!("{}Аня Н", text::FORWARDED_BY)
        );
        assert_eq!(header_prefix(None, true, true, 9, "Петр"), text::YOU);
        assert_eq!(header_prefix(None, false, true, 9, "Петр"), "Петр");
        assert_eq!(header_prefix(None, false, false, 9, "Петр"), "");
    }

    #[test]
    fn flood_wait_is_read_from_the_telegram_error() {
        let err = "telegram sendRichMessage returned 429 Too Many Requests: Too Many Requests: retry after 10";
        assert_eq!(retry_after(err), Some(10));
        assert_eq!(
            retry_after("telegram sendMessage returned 400 Bad Request"),
            None
        );
    }

    #[test]
    fn voice_upload_error_body_is_rejected() {
        assert!(upload_error("").is_none());
        assert_eq!(
            upload_error(r#"{"error":"bad file"}"#).as_deref(),
            Some("voice upload rejected: bad file")
        );
        assert_eq!(
            upload_error("<html>no</html>").as_deref(),
            Some("voice upload rejected")
        );
        assert!(upload_error(r#"{"token":"abc"}"#).is_none());
        assert_eq!(uploaded_token(r#"{"token":"abc"}"#).as_deref(), Some("abc"));
        assert!(voice_rejected(&"proto.payload: Invalid attachment"));
        assert!(!voice_rejected(&"photo upload failed"));
        assert_eq!(content_range_header(12), "bytes 0-11/12");
        assert_eq!(
            upload_place("https://au.oneme.ru/uploadAudio?cid=1"),
            "audio"
        );
        assert_eq!(upload_place("https://vu.okcdn.ru/upload?x=1"), "cdn");
        assert_eq!(upload_body_note(""), "empty");
        assert_eq!(upload_body_note(r#"{"token":"abc"}"#), "keys=token");
    }

    #[test]
    fn favorites_from_me_is_forwarded() {
        let favorites = Message {
            id: 1,
            chat_id: Some(0),
            sender: Some(5),
            text: "note".into(),
            status: None,
            cid: None,
            reply_to: None,
            forward: None,
            time: 0,
            media: Vec::new(),
        };
        let dialog = Message {
            id: 1,
            chat_id: Some(9),
            sender: Some(5),
            text: "note".into(),
            status: None,
            cid: None,
            reply_to: None,
            forward: None,
            time: 0,
            media: Vec::new(),
        };
        assert!(forward_message(&favorites, Some(5)));
        assert!(!forward_message(&dialog, Some(5)));
        assert!(forward_message(&dialog, Some(8)));
    }

    #[test]
    fn parses_one_allowed_id() {
        assert_eq!(parse_allowed_ids("158795336").unwrap(), vec![158795336]);
    }

    #[test]
    fn rejects_an_empty_allow_list() {
        assert!(parse_allowed_ids(" , ").is_err());
    }

    #[test]
    fn mode_defaults_to_chat_and_rejects_other_values() {
        assert_eq!(parse_mode("").unwrap(), Mode::Chat);
        assert_eq!(parse_mode("Chat").unwrap(), Mode::Chat);
        assert_eq!(parse_mode("Thread").unwrap(), Mode::Thread);
        assert!(parse_mode("Forum").is_err());
    }

    #[test]
    fn topic_name_stops_at_128_characters() {
        let long = "я".repeat(200);
        assert_eq!(topic_name(&long).chars().count(), 128);
        assert_eq!(topic_name(""), "MAX");
    }

    #[test]
    fn thread_text_keeps_the_sender_only_for_a_group() {
        assert_eq!(thread_text(false, "Маня Н", "привет"), "привет");
        assert_eq!(thread_text(true, "Маня Н", "привет"), "Маня Н\n\nпривет");
        let face = ChatFace {
            label: "Kitchen".into(),
            group: true,
        };
        assert_eq!(topic_label(4, &face, "Маня Н"), "Kitchen");
        let dialog = ChatFace {
            label: "Маня Н".into(),
            group: false,
        };
        assert_eq!(topic_label(4, &dialog, "Маня Н"), "Маня Н");
        assert_eq!(topic_label(0, &dialog, "я"), text::FAVORITES);
    }

    #[test]
    fn a_topic_message_is_not_a_reply_to_itself() {
        let plain = TgMessage {
            message_id: 8,
            from: None,
            text: None,
            caption: None,
            photo: Vec::new(),
            voice: None,
            video: None,
            video_note: None,
            animation: None,
            thread_id: Some(3),
            reply_to: None,
        };
        assert_eq!(topic_reply_target(&plain), None);
        let anchored = TgMessage {
            message_id: 8,
            from: None,
            text: None,
            caption: None,
            photo: Vec::new(),
            voice: None,
            video: None,
            video_note: None,
            animation: None,
            thread_id: Some(3),
            reply_to: Some(Reply {
                message_id: 3,
                topic_created: None,
            }),
        };
        assert_eq!(topic_reply_target(&anchored), None);
        let opened = TgMessage {
            message_id: 8,
            from: None,
            text: None,
            caption: None,
            photo: Vec::new(),
            voice: None,
            video: None,
            video_note: None,
            animation: None,
            thread_id: Some(3),
            reply_to: Some(Reply {
                message_id: 3,
                topic_created: Some(json!({"name": "Маня Н"})),
            }),
        };
        assert_eq!(topic_reply_target(&opened), None);
        let reply = TgMessage {
            message_id: 9,
            from: None,
            text: None,
            caption: None,
            photo: Vec::new(),
            voice: None,
            video: None,
            video_note: None,
            animation: None,
            thread_id: Some(3),
            reply_to: Some(Reply {
                message_id: 8,
                topic_created: None,
            }),
        };
        assert_eq!(topic_reply_target(&reply), Some(8));
    }

    #[test]
    fn link_pairs_work_in_both_directions() {
        let mut links = Links::default();
        remember_link(&mut links, 8, 42);
        assert_eq!(links.tg.get(&8).copied(), Some(42));
        assert_eq!(links.max.get(&42).copied(), Some(8));
    }

    #[test]
    fn topic_map_finds_the_max_chat() {
        let mut topics = HashMap::new();
        topics.insert(
            topic_key(158795336, 9),
            Topic {
                thread_id: 44,
                name: "Маня Н".into(),
                photo: false,
            },
        );
        assert_eq!(topic_chat(&topics, 158795336, 44), Some(9));
        assert_eq!(topic_chat(&topics, 1, 44), None);
        let loaded: Topic = serde_json::from_str(r#"{"thread_id":3,"name":"Маня Н"}"#).unwrap();
        assert!(!loaded.photo);
    }

    #[test]
    fn reads_env_lines() {
        let dir = std::env::temp_dir().join(format!(
            "bridge-env-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("max-bridge.env");
        fs::write(
            &path,
            "TELEGRAM_BOT_TOKEN=123:abc\nTELEGRAM_ALLOWED_USER_IDS=158795336\n",
        )
        .unwrap();
        let values = read_env_file(&path);
        assert_eq!(values.get("TELEGRAM_BOT_TOKEN").unwrap(), "123:abc");
        assert_eq!(
            values.get("TELEGRAM_ALLOWED_USER_IDS").unwrap(),
            "158795336"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn caption_keeps_the_message_text() {
        assert_eq!(
            media_caption(Mode::Thread, false, "Маня Н", "Маня Н", ""),
            ""
        );
        assert_eq!(
            media_caption(Mode::Thread, true, "Маня Н", "Семья", ""),
            "Маня Н"
        );
        let chat = media_caption(Mode::Chat, false, "Маня Н", "Маня Н", "");
        assert!(!chat.contains("(no text)"));
        assert!(chat.contains("Маня Н"));
        assert_eq!(
            media_caption(Mode::Thread, false, "Маня Н", "Маня Н", "привет"),
            "привет"
        );
    }

    #[test]
    fn telegram_photo_and_voice_parse_without_text() {
        let photo: Update = serde_json::from_str(
            r#"{"update_id":1,"message":{"message_id":2,"from":{"id":158795336},"caption":"hi","photo":[{"file_id":"small","file_size":1,"width":10,"height":10},{"file_id":"large","file_size":9,"width":20,"height":20}]}}"#,
        )
        .unwrap();
        let message = photo.message.unwrap();
        assert_eq!(inbound_text(&message), "hi");
        assert_eq!(largest_file_id(&message.photo).as_deref(), Some("large"));
        let voice: Update = serde_json::from_str(
            r#"{"update_id":3,"message":{"message_id":4,"from":{"id":158795336},"voice":{"file_id":"v","duration":3},"message_thread_id":9}}"#,
        )
        .unwrap();
        let message = voice.message.unwrap();
        assert!(inbound_text(&message).is_empty());
        assert_eq!(message.voice.unwrap().duration.saturating_mul(1000), 3000);
        let text: Update = serde_json::from_str(
            r#"{"update_id":5,"message":{"message_id":6,"from":{"id":158795336},"text":"plain","message_thread_id":9}}"#,
        )
        .unwrap();
        assert_eq!(inbound_text(&text.message.unwrap()), "plain");
    }

    #[test]
    fn largest_photo_is_the_one_we_forward() {
        let photos = vec![
            PhotoSize {
                file_id: "small".into(),
                width: 90,
                height: 90,
                file_size: Some(100),
            },
            PhotoSize {
                file_id: "large".into(),
                width: 800,
                height: 600,
                file_size: Some(9000),
            },
        ];
        assert_eq!(largest_file_id(&photos).as_deref(), Some("large"));
    }

    #[test]
    fn voice_seconds_become_milliseconds() {
        let voice = TgVoice {
            file_id: "voice".into(),
            duration: 3,
        };
        assert_eq!(voice.duration.saturating_mul(1000), 3000);
    }

    fn sample(id: i64, time: i64) -> Message {
        Message {
            id,
            chat_id: Some(9),
            sender: Some(8),
            text: String::new(),
            status: None,
            cid: None,
            reply_to: None,
            forward: None,
            time,
            media: Vec::new(),
        }
    }

    #[test]
    fn missed_messages_follow_the_cursor() {
        let cursor = Cursor { time: 100, id: 1 };
        let missed = take_missed(
            vec![
                sample(1, 100),
                sample(2, 90),
                sample(4, 110),
                sample(3, 110),
            ],
            Some(cursor),
        );
        assert_eq!(
            missed.iter().map(|message| message.id).collect::<Vec<_>>(),
            vec![3, 4]
        );
        assert!(!is_unseen(&sample(1, 100), Some(cursor)));
        assert!(is_unseen(&sample(9, 0), Some(Cursor { time: 0, id: 4 })));
    }

    #[test]
    fn first_cursor_is_the_newest_message() {
        let cursor = baseline_cursor(&[sample(2, 50), sample(5, 80), sample(4, 80)]).unwrap();
        assert_eq!(cursor.time, 80);
        assert_eq!(cursor.id, 5);
        assert!(baseline_cursor(&[]).is_none());
    }

    #[test]
    fn history_covers_every_known_contact() {
        let mut seen = HashMap::new();
        seen.insert(4, Cursor { time: 1, id: 1 });
        let mut topics = HashMap::new();
        topics.insert(
            topic_key(7, 9),
            Topic {
                thread_id: 3,
                name: "Маня Н".into(),
                photo: false,
            },
        );
        topics.insert(
            topic_key(8, 11),
            Topic {
                thread_id: 4,
                name: "Другой".into(),
                photo: false,
            },
        );
        assert_eq!(
            history_chat_ids(&[0, 2], &seen, &topics, 7),
            vec![0, 2, 4, 9]
        );
        let mut bare = sample(1, 10);
        bare.chat_id = None;
        let mut stamped = vec![bare];
        stamp_history_chat(&mut stamped, 9);
        assert_eq!(stamped[0].chat_id, Some(9));
        let command = TgMessage {
            message_id: 40,
            from: None,
            text: Some("/history".into()),
            caption: None,
            photo: Vec::new(),
            voice: None,
            video: None,
            video_note: None,
            animation: None,
            thread_id: Some(40),
            reply_to: Some(Reply {
                message_id: 40,
                topic_created: Some(json!({"name": "/history"})),
            }),
        };
        assert_eq!(stray_command_topic(&command, &topics, 7), Some(40));
        let inside = TgMessage {
            message_id: 41,
            from: None,
            text: Some("Загрузить историю".into()),
            caption: None,
            photo: Vec::new(),
            voice: None,
            video: None,
            video_note: None,
            animation: None,
            thread_id: Some(3),
            reply_to: None,
        };
        assert_eq!(stray_command_topic(&inside, &topics, 7), None);
    }

    #[test]
    fn history_command_stays_out_of_max() {
        assert!(is_history_command("/history"));
        assert!(is_history_command("/history@bridge_bot"));
        assert!(is_history_command(text::LOAD_HISTORY));
        assert!(!is_history_command("загрузить историю чата"));
        let update: Update = serde_json::from_str(
            r#"{"update_id":1,"callback_query":{"id":"c","from":{"id":7},"data":"history"}}"#,
        )
        .unwrap();
        assert_eq!(update.callback.unwrap().data.as_deref(), Some("history"));
    }

    #[test]
    fn earlier_message_rebuilds_the_topic() {
        let mut links = Links::default();
        remember_link(&mut links, 8, 20);
        assert!(needs_topic_rebuild(
            &[sample(10, 50), sample(20, 80)],
            &links
        ));
        assert!(!needs_topic_rebuild(
            &[sample(20, 80), sample(30, 90)],
            &links
        ));
        assert!(!needs_topic_rebuild(
            &[sample(10, 50), sample(20, 80)],
            &Links::default()
        ));
        drop_chat_links(&mut links, &[sample(20, 80)]);
        assert!(links.max.is_empty());
        assert!(links.tg.is_empty());
    }

    #[test]
    fn own_history_keeps_the_contact_topic_name() {
        assert_eq!(own_header(""), text::YOU);
        assert_eq!(own_header("привет"), format!("{}\n\nпривет", text::YOU));
        let rich = named_rich("Ната К", "Ната К\n\nтекст").unwrap();
        let parts = rich["blocks"][0]["text"].as_array().unwrap();
        let expected = json!([
            {"type": "marked", "text": {"type": "bold", "text": "Ната К"}},
            "\n\n",
            "текст"
        ]);
        assert_eq!(&parts[..3], expected.as_array().unwrap().as_slice());
        assert_eq!(parts[3].as_str().unwrap().chars().count(), 36 - 5);
        assert!(parts[3]
            .as_str()
            .unwrap()
            .chars()
            .all(|ch| ch == '\u{2800}'));
        let wide = "н".repeat(36);
        let long = named_rich("Ната К", &format!("Ната К\n\n{wide}")).unwrap();
        assert_eq!(long["blocks"][0]["text"].as_array().unwrap().len(), 3);
        assert_eq!(
            bold_prefix_entities("Ната К", "Ната К\n\nтекст"),
            Some(json!([{"type": "bold", "offset": 0, "length": 6}]))
        );
        assert_eq!(bold_prefix_entities("ты", "привет"), None);
        assert_eq!(named_rich("", "привет"), None);
        assert!(ogg_to_wav(b"not-audio").is_err());
        let mut pcm = vec![0i16; 160];
        pcm[0] = 32767;
        pcm[159] = -32767;
        let wave = peak_wave(&pcm);
        assert_eq!(wave.len(), 80);
        assert_eq!(wave[0], 127);
        assert_eq!(wave[79], 127);
        let wav = wav_bytes(8_000, 1, &[1, -1]);
        assert!(wav.starts_with(b"RIFF"));
        assert_eq!(&wav[8..12], b"WAVE");
        assert_eq!(wav.len(), 48);
        assert!(is_reread_command("/reread"));
        assert!(is_reread_command(text::RELOAD_HISTORY));
        assert!(!is_reread_command("/history"));
        let face = ChatFace {
            label: "Маня Н".into(),
            group: false,
        };
        assert_eq!(topic_label(9, &face, ""), "Маня Н");
        assert_eq!(topic_label(0, &face, ""), text::FAVORITES);
        let removed = Message {
            id: 1,
            chat_id: Some(9),
            sender: Some(5),
            text: "  ".into(),
            status: Some("REMOVED".into()),
            cid: None,
            reply_to: None,
            forward: None,
            time: 1,
            media: Vec::new(),
        };
        assert_eq!(history_body(&removed), text::DELETED);
        let file = Message {
            status: None,
            text: String::new(),
            ..removed
        };
        assert_eq!(history_body(&file), text::NOT_FORWARDED);
    }

    #[test]
    fn setup_stays_hidden_until_the_session_is_ready() {
        assert!(!history_allowed(LinkState::Off));
        assert!(!history_allowed(LinkState::Connecting));
        assert!(history_allowed(LinkState::Ready));
        let off = panel_markup(LinkState::Off);
        assert_eq!(off["inline_keyboard"][0][0]["text"], text::SETUP);
        assert_eq!(off["inline_keyboard"][0][0]["url"], auth_http::AUTH_URL);
        assert!(off["inline_keyboard"][0][0].get("callback_data").is_none());
        let connecting = panel_markup(LinkState::Connecting);
        assert!(connecting["inline_keyboard"].as_array().unwrap().is_empty());
        let ready = panel_markup(LinkState::Ready);
        assert_eq!(ready["inline_keyboard"][0][0]["callback_data"], "history");
        assert_eq!(ready["inline_keyboard"][1][0]["callback_data"], "reread");
        assert_eq!(ready["inline_keyboard"][2][0]["callback_data"], "logout");
        assert!(!ready.to_string().contains("127.0.0.1"));
        assert_eq!(command_list(LinkState::Off)[0]["command"], "setup");
        assert_eq!(command_list(LinkState::Ready)[2]["command"], "logout");
        assert!(command_list(LinkState::Connecting)
            .as_array()
            .unwrap()
            .is_empty());
        assert!(is_setup_command("/setup"));
        assert!(is_setup_command("/setup@bridge"));
        assert!(is_setup_command(text::SETUP));
        assert!(is_logout_command("/logout"));
        assert!(is_logout_command(text::SIGN_OUT));
        assert!(!is_history_command("/setup"));
        assert!(!is_logout_command("/history"));
        assert!(!session_rejected("logged in"));
        assert!(session_rejected(
            "saved MAX session was rejected. Run the program again and sign in"
        ));
    }
}
