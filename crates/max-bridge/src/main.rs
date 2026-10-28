mod auth_http;
mod text;

use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::panic::{self, AssertUnwindSafe};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use max_client::{
    beat, beat_age, contact_error_kind, lookup_v4, lookup_v4_all, max_beat, max_beat_age, Attach,
    ChatFace, Client, ContactErrorKind, Cursor, Event, InReceipt, Links, Media, Message,
    OutReceipt, PendingMark, ReceiptStatus, Receipts, SessionLink, SpecialAttach, Store, Topic,
};
use reqwest::multipart::{Form, Part};
use reqwest::Client as Http;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{mpsc, watch};

const TELEGRAM_HOST: &str = "api.telegram.org";
static TELEGRAM_ADDR: AtomicUsize = AtomicUsize::new(0);
static TELEGRAM_BEAT: AtomicU64 = AtomicU64::new(0);
static ERRORS: AtomicU64 = AtomicU64::new(0);
static ERRORS_TOLD: AtomicU64 = AtomicU64::new(0);
static ALERT_CONSOLE: AtomicU64 = AtomicU64::new(0);
static MAX_EXPECTED: AtomicBool = AtomicBool::new(false);
static ERROR_LOG: Mutex<Option<std::fs::File>> = Mutex::new(None);
static UNTYPED_LOG: Mutex<Option<std::fs::File>> = Mutex::new(None);
static TELEGRAM_ABORT: Mutex<Option<tokio::task::AbortHandle>> = Mutex::new(None);
static MAX_ABORT: Mutex<Option<tokio::task::AbortHandle>> = Mutex::new(None);
const MAX_MEDIA_BYTES: usize = 20 * 1024 * 1024;
const STALL_AFTER_SECS: u64 = 180;
const WORKER_STALL_SECS: u64 = 90;
const ERROR_ALERT: u64 = 100;
// Telegram bots may only set reactions from the Bot API allowlist.
const REACT_SENT: &str = "⚡";
const REACT_ARRIVED: &str = "👍";
const REACT_READ: &str = "👀";

fn main() {
    let mut pause = Duration::from_secs(2);
    loop {
        let config = match Config::load() {
            Ok(config) => config,
            Err(err) => {
                eprintln!("bridge: {err}");
                std::process::exit(1);
            }
        };
        match guard(config) {
            Ok(()) => break,
            Err(reason) => {
                eprintln!("bridge: {reason}; restarting");
                thread::sleep(pause);
                pause = (pause * 2).min(Duration::from_secs(30));
            }
        }
    }
}

fn stalled(age: Option<u64>) -> bool {
    age.is_some_and(|age| age > STALL_AFTER_SECS)
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
}

fn note_telegram() {
    TELEGRAM_BEAT.store(unix_secs(), Ordering::Relaxed);
}

fn telegram_age() -> Option<u64> {
    let beat = TELEGRAM_BEAT.load(Ordering::Relaxed);
    if beat == 0 {
        None
    } else {
        Some(unix_secs().saturating_sub(beat))
    }
}

fn expect_max(on: bool) {
    MAX_EXPECTED.store(on, Ordering::Relaxed);
    if on {
        max_beat();
    }
}

fn console_error(text: &str) {
    eprintln!(": E-R-R-O-R {text}");
}

fn console_alert(text: &str) {
    let now = unix_secs();
    let prev = ALERT_CONSOLE.load(Ordering::Relaxed);
    if now.saturating_sub(prev) < 60 {
        return;
    }
    if ALERT_CONSOLE
        .compare_exchange(prev, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
    {
        console_error(text);
    }
}

fn alert_mark(count: u64) -> Option<u64> {
    if count <= ERROR_ALERT {
        None
    } else {
        Some((count / ERROR_ALERT) * ERROR_ALERT)
    }
}

fn error_line(stamp: &str, source: &str, detail: &str) -> String {
    format!("{stamp} {source} {}", redact_telegram(detail))
}

fn error_stamp(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let time = secs % 86_400;
    let hour = time / 3_600;
    let min = (time % 3_600) / 60;
    let sec = time % 60;
    let (year, month, day) = civil_date(days);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
}

fn civil_date(mut days: i64) -> (i64, u32, u32) {
    days += 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let doe = (days - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let year = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    let year = if month <= 2 { year + 1 } else { year };
    (year, month, day)
}

fn record_error(source: &str, detail: &str) {
    let line = error_line(&error_stamp(unix_secs()), source, detail);
    ERRORS.fetch_add(1, Ordering::Relaxed);
    let mut slot = ERROR_LOG.lock().unwrap_or_else(|err| err.into_inner());
    if slot.is_none() {
        let path = var_dir().join("errors.log");
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            Ok(file) => *slot = Some(file),
            Err(err) => {
                console_error(&format!("error log is not writable: {err}"));
                return;
            }
        }
    }
    if let Some(file) = slot.as_mut() {
        let _ = writeln!(file, "{line}");
        let _ = file.flush();
    }
}

fn record_untyped(chat_id: i64, message_id: i64, raw: &Value) {
    let line = untyped_line(unix_ms(), chat_id, message_id, raw);
    let mut slot = UNTYPED_LOG.lock().unwrap_or_else(|err| err.into_inner());
    if slot.is_none() {
        let path = var_dir().join("untyped-msgs");
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            Ok(file) => *slot = Some(file),
            Err(err) => {
                eprintln!("bridge: untyped log is not writable: {err}");
                return;
            }
        }
    }
    if let Some(file) = slot.as_mut() {
        let _ = writeln!(file, "{line}");
        let _ = file.flush();
    }
}

fn untyped_line(time: i64, chat_id: i64, message_id: i64, raw: &Value) -> String {
    json!({
        "time": time,
        "chatId": chat_id,
        "messageId": message_id,
        "attach": raw,
    })
    .to_string()
}

fn worker_down(age: Option<u64>) -> bool {
    age.is_some_and(|age| age > WORKER_STALL_SECS)
}

fn guard(config: Config) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|err| err.to_string())?;
    beat();
    note_telegram();
    let worker = runtime.spawn(run(config));
    let abort = worker.abort_handle();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let outcome = panic::catch_unwind(AssertUnwindSafe(|| runtime.block_on(worker)));
        drop(runtime);
        let _ = done_tx.send(outcome);
    });
    watch_workers(done_rx, abort)
}

fn watch_workers(
    done: std::sync::mpsc::Receiver<
        std::thread::Result<Result<Result<(), String>, tokio::task::JoinError>>,
    >,
    abort: tokio::task::AbortHandle,
) -> Result<(), String> {
    loop {
        match done.recv_timeout(Duration::from_secs(5)) {
            Ok(outcome) => return finish_run(outcome),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return Err("runtime stopped".into());
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                if stalled(beat_age()) {
                    console_error("event loop stalled; restarting");
                    abort.abort();
                    continue;
                }
                supervise();
            }
        }
    }
}

fn finish_run(
    outcome: std::thread::Result<Result<Result<(), String>, tokio::task::JoinError>>,
) -> Result<(), String> {
    match outcome {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(err))) => Err(err),
        Ok(Err(err)) if err.is_cancelled() => Err("event loop stalled".into()),
        Ok(Err(_)) => Err("task panicked".into()),
        Err(_) => Err("runtime panicked".into()),
    }
}

fn supervise() {
    let telegram_stalled = worker_down(telegram_age());
    let max_stalled = MAX_EXPECTED.load(Ordering::Relaxed) && worker_down(max_beat_age());
    if telegram_stalled {
        record_error("telegram", "worker is not responding");
        console_error("telegram is not responding; restarting");
        note_telegram();
        if let Some(handle) = TELEGRAM_ABORT
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone()
        {
            handle.abort();
        }
    }
    if max_stalled {
        record_error("max", "worker is not responding");
        console_error("MAX is not responding; restarting");
        max_beat();
        if let Some(handle) = MAX_ABORT
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone()
        {
            handle.abort();
        }
    }
    let count = ERRORS.load(Ordering::Relaxed);
    if telegram_stalled && alert_mark(count).is_some() {
        console_alert(&format!("bridge errors {count}, telegram is not connected"));
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
    store: Arc<Store>,
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
        let db_path = data_path("MAX_DB_PATH", "max.db");
        let store = Arc::new(Store::open(&db_path).map_err(|err| err.to_string())?);
        Ok(Config {
            token,
            allowed,
            mode,
            store,
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

fn mini_app_button(link: LinkState) -> Value {
    let label = match link {
        LinkState::Ready => text::ADD_CONTACT,
        LinkState::Off | LinkState::Connecting => text::SETUP,
    };
    json!({
        "text": label,
        "web_app": {"url": auth_http::MINI_APP_URL}
    })
}

fn panel_markup(link: LinkState) -> Value {
    match link {
        LinkState::Off | LinkState::Connecting => json!({
            "inline_keyboard": [[mini_app_button(link)]]
        }),
        LinkState::Ready => json!({
            "inline_keyboard": [
                [mini_app_button(link)],
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
    TelegramStopped,
    Tick,
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
        if let Err(err) = telegram(
            http,
            &config.token,
            "setMyCommands",
            json!({
                "commands": commands.clone(),
                "scope": {"type": "chat", "chat_id": user_id}
            }),
        )
        .await
        {
            eprintln!("bridge: {err}");
        }
        if let Some(message_id) = panels.get(user_id).copied() {
            match telegram(
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
            .await
            {
                Ok(_) => continue,
                Err(err) => eprintln!("bridge: {err}"),
            }
        }
        match telegram(
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
            Ok(payload) => {
                if let Some(message_id) = sent_message_id(&payload) {
                    panels.insert(*user_id, message_id);
                }
            }
            Err(err) => eprintln!("bridge: {err}"),
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

fn forget_session(store: &Store) {
    let _ = store.clear_session();
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

fn set_client(
    slot: &mut Option<Client>,
    published: &watch::Sender<Option<SessionLink>>,
    next: Option<Client>,
) {
    if let Some(client) = next.as_ref() {
        let mut abort = MAX_ABORT.lock().unwrap_or_else(|err| err.into_inner());
        *abort = Some(client.abort_handle());
        expect_max(true);
    }
    let _ = published.send(next.as_ref().map(Client::link));
    *slot = next;
}

async fn begin_logout(
    config: &Config,
    http: &Http,
    client: &mut Option<Client>,
    session_link: &watch::Sender<Option<SessionLink>>,
    link: &mut LinkState,
    me: &mut Option<i64>,
    panels: &mut HashMap<i64, i64>,
    logout_asks: &mut HashMap<i64, i64>,
    user_id: i64,
) {
    set_client(client, session_link, None);
    expect_max(false);
    *link = LinkState::Off;
    *me = None;
    forget_session(&config.store);
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
    let mut map = config.store.load_map();
    let mut links = config.store.load_links();
    let mut receipts = config.store.load_receipts();
    let mut topics = config.store.load_topics();
    let mut offset = config.store.load_offset();
    let mut seen = config.store.load_seen();
    let mut ready_count = 0u32;
    eprintln!(
        "bridge: mode is {}",
        match config.mode {
            Mode::Chat => "Chat",
            Mode::Thread => "Thread",
        }
    );
    let auth = auth_http::AuthHub::new(Arc::clone(&config.store));
    let mut auth_notes = auth.subscribe();
    let auth_page = auth.clone();
    tokio::spawn(async move {
        if let Err(err) = auth_page.listen().await {
            eprintln!("bridge: auth page failed: {err}");
        }
    });
    let (session_link, contacts_link) = watch::channel(None::<SessionLink>);
    let contacts_dir = var_dir();
    let mini_auth = auth.clone();
    tokio::spawn(async move {
        if let Err(err) = auth_http::listen_mini(mini_auth, contacts_link, contacts_dir).await {
            eprintln!("bridge: mini app failed: {err}");
        }
    });
    let mut http = telegram_http().await?;
    clear_reply_keyboard(&http, &config).await;
    // getUpdates runs on its own task. The main thread only watches it.
    let (mut offset_tx, mut poll_rx) = start_telegram(&config.token, offset);
    let mut tick = tokio::time::interval(Duration::from_secs(30));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut client = None;
    let mut link = LinkState::Off;
    if config
        .store
        .read_session()
        .map_err(|err| err.to_string())?
        .is_some()
    {
        match Client::start(Arc::clone(&config.store)).await {
            Ok(started) => {
                set_client(&mut client, &session_link, Some(started));
                link = LinkState::Connecting;
            }
            Err(err) => eprintln!("bridge: {err}"),
        }
    }
    let mut panels = HashMap::new();
    let mut logout_asks = HashMap::new();
    sync_controls(&http, &config, link, &mut panels).await;
    eprintln!("bridge: listening");
    let mut me: Option<i64> = None;
    let mut echoes = HashSet::<i64>::new();
    loop {
        beat();
        if client.is_none() && config.store.read_session().ok().flatten().is_some() {
            eprintln!("bridge: MAX client stopped; starting again");
            tokio::time::sleep(Duration::from_secs(2)).await;
            beat();
            match Client::start(Arc::clone(&config.store)).await {
                Ok(started) => {
                    set_client(&mut client, &session_link, Some(started));
                    link = LinkState::Connecting;
                    sync_controls(&http, &config, link, &mut panels).await;
                }
                Err(err) => {
                    record_error("max", &err.to_string());
                    eprintln!("bridge: {err}");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            }
        } else if client.is_none() {
            expect_max(false);
        }
        let wake = tokio::select! {
            event = max_event(client.as_mut()) => Wake::Max(event),
            polled = poll_rx.recv() => match polled {
                Some(result) => Wake::Telegram(result),
                None => Wake::TelegramStopped,
            },
            _ = tick.tick() => Wake::Tick,
            changed = auth_notes.changed() => Wake::Auth { changed: changed.is_ok() },
        };
        match wake {
            Wake::Auth { changed: false } => {}
            Wake::Auth { changed: true } => {
                if client.is_none() && config.store.read_session().ok().flatten().is_some() {
                    match Client::start(Arc::clone(&config.store)).await {
                        Ok(started) => {
                            set_client(&mut client, &session_link, Some(started));
                            link = LinkState::Connecting;
                            sync_controls(&http, &config, link, &mut panels).await;
                        }
                        Err(err) => eprintln!("bridge: {err}"),
                    }
                }
            }
            Wake::Tick => {
                maybe_alert(&http, &config).await;
            }
            Wake::TelegramStopped => {
                record_error("telegram", "poll task stopped");
                console_error("telegram poll stopped; starting again");
                let restarted = start_telegram(&config.token, offset);
                offset_tx = restarted.0;
                poll_rx = restarted.1;
            }
            Wake::Telegram(Err(_)) => {
                maybe_alert(&http, &config).await;
            }
            Wake::Telegram(Ok(updates)) => {
                let got_updates = !updates.is_empty();
                for update in updates {
                    offset = update.update_id.saturating_add(1);
                    config.store.save_offset(offset);
                    if let Err(err) = handle_update(
                        &config,
                        &mut http,
                        &mut client,
                        &session_link,
                        &mut link,
                        &mut map,
                        &mut links,
                        &mut receipts,
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
                if got_updates {
                    let _ = offset_tx.send(offset);
                }
            }
            Wake::Max(None) => {
                set_client(&mut client, &session_link, None);
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
                        if let Err(err) = flush_pending_marks(active, &config, &mut receipts).await
                        {
                            eprintln!("bridge: pending mark flush failed: {err}");
                        }
                        if let Err(err) =
                            resync_out_receipts(active, &config, &mut http, &mut receipts, id).await
                        {
                            eprintln!("bridge: receipt resync failed: {err}");
                        }
                        match catch_up(
                            &config,
                            &mut http,
                            active,
                            &mut map,
                            &mut links,
                            &mut receipts,
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
                    if note.contains("reconnecting") || session_rejected(&note) {
                        record_error("max", &note);
                        maybe_alert(&http, &config).await;
                    }
                    if session_rejected(&note) {
                        let tell = link != LinkState::Off;
                        expect_max(false);
                        set_client(&mut client, &session_link, None);
                        link = LinkState::Off;
                        me = None;
                        sync_controls(&http, &config, link, &mut panels).await;
                        if tell {
                            notify_main(&http, &config, text::SESSION_REFRESH_FAILED).await;
                        }
                    }
                }
                Event::Contact { dialog, name } => {
                    if config.mode != Mode::Thread {
                        eprintln!("bridge: contact saved, topic needs Thread mode");
                        continue;
                    }
                    let Some(active) = client.as_mut() else {
                        continue;
                    };
                    let users = config.allowed.clone();
                    for tg_user in users {
                        if let Err(err) = open_history_topic(
                            &config,
                            &mut http,
                            active,
                            &mut topics,
                            tg_user,
                            dialog,
                            &name,
                        )
                        .await
                        {
                            eprintln!("bridge: contact topic failed for chat {dialog}: {err}");
                        }
                    }
                }
                Event::Delivered(mark) => {
                    apply_delivery_status(&config, &mut http, &mut receipts, &mark).await;
                }
                Event::Read(mark) => {
                    apply_read_status(&config, &mut http, &mut receipts, &mark, me).await;
                }
                Event::Message(message) => {
                    let Some(active) = client.as_mut() else {
                        continue;
                    };
                    if echoes.remove(&message.id) {
                        for item in &message.media {
                            if let Media::Video { video_type, .. } = item {
                                eprintln!(
                                    "bridge: max echo video id={} video_type={video_type:?}",
                                    message.id
                                );
                            }
                        }
                        mark_seen(&mut seen, &config.store, &message);
                        continue;
                    }
                    if !is_unseen(&message, cursor_of(&seen, message.chat_id)) {
                        continue;
                    }
                    if !forward_message(&message, me) || skipped_status(&message) {
                        mark_seen(&mut seen, &config.store, &message);
                        continue;
                    }
                    if let Err(err) = forward_max(
                        &config,
                        &mut http,
                        active,
                        &mut map,
                        &mut links,
                        &mut receipts,
                        &mut topics,
                        &message,
                        me,
                        false,
                    )
                    .await
                    {
                        eprintln!("bridge: telegram send failed");
                        record_error("telegram", &err);
                        if let Ok(fresh) = telegram_http().await {
                            http = fresh;
                        }
                    } else {
                        mark_seen(&mut seen, &config.store, &message);
                    }
                }
            },
        }
    }
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

fn mark_seen(seen: &mut HashMap<i64, Cursor>, store: &Store, message: &Message) {
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
    store.save_seen(seen);
}

fn unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

async fn apply_out_status(
    config: &Config,
    http: &mut Http,
    receipts: &mut Receipts,
    max_message: i64,
    next: ReceiptStatus,
) {
    let Some(receipt) = receipts.out.get_mut(&max_message) else {
        return;
    };
    if !raise_out_status(receipt, next) {
        return;
    }
    let tg_chat = receipt.tg_chat;
    let tg_message = receipt.tg_message;
    let emoji = status_emoji(receipt.status);
    config.store.save_receipts(receipts);
    if let Err(err) = set_reaction(http, &config.token, tg_chat, tg_message, emoji).await {
        eprintln!("bridge: reaction failed: {err}");
    }
}

async fn apply_delivery_status(
    config: &Config,
    http: &mut Http,
    receipts: &mut Receipts,
    mark: &max_client::DeliveryMark,
) {
    if mark.message_ids.is_empty() {
        return;
    }
    for max_id in &mark.message_ids {
        apply_out_status(config, http, receipts, *max_id, ReceiptStatus::Arrived).await;
    }
}

async fn apply_read_status(
    config: &Config,
    http: &mut Http,
    receipts: &mut Receipts,
    mark: &max_client::ReadMark,
    me: Option<i64>,
) {
    let mut targets: Vec<i64> = mark.message_ids.clone();
    if targets.is_empty() && mark.mark > 0 {
        targets = receipts
            .out
            .iter()
            .filter(|(_, item)| {
                item.max_chat == mark.chat_id
                    && item.time > 0
                    && item.time <= mark.mark
                    && item.status < ReceiptStatus::Read
                    && match mark.user_id {
                        Some(user) => Some(user) != me,
                        None => true,
                    }
            })
            .map(|(id, _)| *id)
            .collect();
    }
    for max_id in targets {
        apply_out_status(config, http, receipts, max_id, ReceiptStatus::Read).await;
    }
}

async fn flush_pending_marks(
    client: &Client,
    config: &Config,
    receipts: &mut Receipts,
) -> Result<(), String> {
    if receipts.pending_marks.is_empty() {
        return Ok(());
    }
    let pending = std::mem::take(&mut receipts.pending_marks);
    let mut left = Vec::new();
    for item in pending {
        match client
            .mark_read(item.max_chat, item.max_message, item.mark)
            .await
        {
            Ok(()) => {}
            Err(err) => {
                eprintln!("bridge: mark read failed: {err}");
                left.push(item);
            }
        }
    }
    receipts.pending_marks = left;
    config.store.save_receipts(receipts);
    Ok(())
}

async fn resync_out_receipts(
    client: &Client,
    config: &Config,
    http: &mut Http,
    receipts: &mut Receipts,
    me: i64,
) -> Result<(), String> {
    for chat_id in pending_out_chats(receipts) {
        let marks = match client.chat_peer_marks(chat_id).await {
            Ok(marks) => marks,
            Err(err) => {
                eprintln!("bridge: chat marks failed for {chat_id}: {err}");
                continue;
            }
        };
        let peer_mark = marks
            .iter()
            .filter(|(user, _)| **user != me)
            .map(|(_, mark)| *mark)
            .max()
            .unwrap_or(0);
        if peer_mark <= 0 {
            continue;
        }
        let ready: Vec<i64> = receipts
            .out
            .iter()
            .filter(|(_, item)| {
                item.max_chat == chat_id
                    && item.time > 0
                    && item.time <= peer_mark
                    && item.status < ReceiptStatus::Read
            })
            .map(|(id, _)| *id)
            .collect();
        for max_id in ready {
            apply_out_status(config, http, receipts, max_id, ReceiptStatus::Read).await;
        }
    }
    Ok(())
}

async fn handle_read_reaction(
    config: &Config,
    http: &mut Http,
    client: &mut Option<Client>,
    receipts: &mut Receipts,
    reaction: &MessageReactionUpdate,
) -> Result<(), String> {
    let Some(user) = reaction.user.as_ref() else {
        return Ok(());
    };
    if !config.allowed.contains(&user.id) && !config.allowed.contains(&reaction.chat.id) {
        return Ok(());
    }
    let eyes = reaction
        .new_reaction
        .iter()
        .any(|item| item.kind == "emoji" && item.emoji.as_deref() == Some(REACT_READ));
    if !eyes {
        return Ok(());
    }
    let Some(inbound) = receipts.inbound.get(&reaction.message_id).cloned() else {
        return Ok(());
    };
    let mark = if inbound.time > 0 {
        inbound.time
    } else {
        unix_ms()
    };
    if let Some(active) = client.as_ref() {
        match active
            .mark_read(inbound.max_chat, inbound.max_message, mark)
            .await
        {
            Ok(()) => return Ok(()),
            Err(err) => eprintln!("bridge: mark read failed: {err}"),
        }
    }
    receipts.pending_marks.push(PendingMark {
        max_chat: inbound.max_chat,
        max_message: inbound.max_message,
        mark,
    });
    config.store.save_receipts(receipts);
    let _ = http;
    Ok(())
}

async fn catch_up(
    config: &Config,
    http: &mut Http,
    client: &mut Client,
    map: &mut HashMap<i64, i64>,
    links: &mut Links,
    receipts: &mut Receipts,
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
        beat();
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
                    config.store.save_seen(seen);
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
                mark_seen(seen, &config.store, &message);
                continue;
            }
            if !forward_message(&message, me) || skipped_status(&message) {
                mark_seen(seen, &config.store, &message);
                continue;
            }
            if let Err(err) = forward_max(
                config, http, client, map, links, receipts, topics, &message, me, false,
            )
            .await
            {
                eprintln!("bridge: telegram send failed");
                record_error("telegram", &err);
                if let Ok(fresh) = telegram_http().await {
                    *http = fresh;
                }
                break;
            }
            mark_seen(seen, &config.store, &message);
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
    receipts: &mut Receipts,
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
    let system = message.control.is_some();
    let own = history && !system && me.is_some_and(|id| message.sender == Some(id));
    let forward_name = if system {
        None
    } else {
        forward_author(client, message).await
    };
    let bold_prefix = header_prefix(forward_name.as_deref(), own, face.group, chat_id, &sender);
    let title = if chat_id == 0 {
        text::FAVORITES.to_owned()
    } else {
        face.label.clone()
    };
    let topic_sender = if own || system { "" } else { sender.as_str() };
    if history {
        if let Some(max_id) = message.reply_to {
            if !links.max.contains_key(&max_id) {
                eprintln!("bridge: reply target {max_id} has no Telegram message");
            }
        }
    }
    let mut caption_used_text = false;
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
                        receipts,
                        topics,
                        *user_id,
                        chat_id,
                        message.id,
                        message.time,
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
                caption_used_text = sent_any && !message.text.trim().is_empty();
                if message.specials.is_empty() {
                    return Ok(());
                }
            }
        } else if message.text.is_empty() && message.specials.is_empty() {
            return Ok(());
        }
    }
    if !message.specials.is_empty() {
        let lead = if caption_used_text {
            ""
        } else {
            message.text.trim()
        };
        let label = topic_label(chat_id, &face, topic_sender);
        return deliver_specials(
            config,
            http,
            client,
            map,
            links,
            receipts,
            topics,
            message,
            chat_id,
            &sender,
            &title,
            &bold_prefix,
            &label,
            lead,
            history,
        )
        .await;
    }
    let body = plain_body(message);
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
                send_feed(
                    http,
                    config,
                    map,
                    links,
                    receipts,
                    *user_id,
                    &text,
                    chat_id,
                    message.id,
                    message.time,
                    &bold_prefix,
                    None,
                )
                .await?;
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
                    receipts,
                    *user_id,
                    chat_id,
                    message.id,
                    message.time,
                    &label,
                    &text,
                    reply_to,
                    &bold_prefix,
                    None,
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
                        links,
                        receipts,
                        *user_id,
                        &fallback,
                        chat_id,
                        message.id,
                        message.time,
                        &bold_prefix,
                        None,
                    )
                    .await?;
                }
            }
        }
    }
    Ok(())
}

async fn deliver_specials(
    config: &Config,
    http: &mut Http,
    client: &mut Client,
    map: &mut HashMap<i64, i64>,
    links: &mut Links,
    receipts: &mut Receipts,
    topics: &mut HashMap<String, Topic>,
    message: &Message,
    chat_id: i64,
    sender: &str,
    title: &str,
    bold_prefix: &str,
    label: &str,
    lead: &str,
    history: bool,
) -> Result<(), String> {
    for item in &message.specials {
        if let SpecialAttach::Unknown { raw } = item {
            record_untyped(chat_id, message.id, raw);
        }
    }
    let head = special_head(config.mode, bold_prefix, sender, title);
    if let Some(rich) = special_rich(&head, lead, &message.specials) {
        let plain = clip(&special_plain(&head, lead, &message.specials), 4000);
        relay_text(
            config,
            http,
            client,
            map,
            links,
            receipts,
            topics,
            message,
            chat_id,
            label,
            &plain,
            &head,
            Some(rich),
            history,
        )
        .await?;
    } else if !lead.is_empty() {
        let text = headed_text(config.mode, &head, lead);
        relay_text(
            config, http, client, map, links, receipts, topics, message, chat_id, label, &text,
            &head, None, history,
        )
        .await?;
    }
    let unknown = unknown_values(&message.specials);
    if unknown.is_empty() {
        return Ok(());
    }
    let json_text = clip(&unknown_text(&unknown), 4000);
    let shown = match config.mode {
        Mode::Chat if !head.is_empty() => clip(&format!("{head}\n\n{json_text}"), 4000),
        _ => json_text,
    };
    relay_text(
        config, http, client, map, links, receipts, topics, message, chat_id, label, &shown, "",
        None, history,
    )
    .await
}

async fn relay_text(
    config: &Config,
    http: &mut Http,
    client: &mut Client,
    map: &mut HashMap<i64, i64>,
    links: &mut Links,
    receipts: &mut Receipts,
    topics: &mut HashMap<String, Topic>,
    message: &Message,
    chat_id: i64,
    label: &str,
    text: &str,
    bold_prefix: &str,
    rich: Option<Value>,
    history: bool,
) -> Result<(), String> {
    match config.mode {
        Mode::Chat => {
            if chat_heading_missing(bold_prefix) {
                eprintln!("bridge: contact name is missing for chat {chat_id}");
            }
            for user_id in &config.allowed {
                send_feed(
                    http,
                    config,
                    map,
                    links,
                    receipts,
                    *user_id,
                    text,
                    chat_id,
                    message.id,
                    message.time,
                    bold_prefix,
                    rich.clone(),
                )
                .await?;
            }
        }
        Mode::Thread => {
            if label == "MAX" {
                eprintln!("bridge: contact name is missing for chat {chat_id}");
            }
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
                    receipts,
                    *user_id,
                    chat_id,
                    message.id,
                    message.time,
                    label,
                    text,
                    reply_to,
                    bold_prefix,
                    rich.clone(),
                )
                .await
                {
                    if history {
                        return Err(err);
                    }
                    eprintln!("bridge: topic send failed: {err}");
                    send_feed(
                        http,
                        config,
                        map,
                        links,
                        receipts,
                        *user_id,
                        text,
                        chat_id,
                        message.id,
                        message.time,
                        bold_prefix,
                        rich.clone(),
                    )
                    .await?;
                }
            }
        }
    }
    Ok(())
}

fn chat_heading_missing(prefix: &str) -> bool {
    prefix == "MAX"
}

fn special_head(mode: Mode, bold_prefix: &str, sender: &str, title: &str) -> String {
    if !bold_prefix.is_empty() {
        return bold_prefix.to_owned();
    }
    if mode == Mode::Chat {
        return chat_heading(sender, title);
    }
    String::new()
}

fn headed_text(mode: Mode, head: &str, body: &str) -> String {
    match mode {
        Mode::Chat if !head.is_empty() => clip(&format!("{head}\n\n{body}"), 4000),
        Mode::Thread if !head.is_empty() => clip(&join_name(head, body), 4000),
        _ => clip(body, 4000),
    }
}

async fn send_feed(
    http: &mut Http,
    config: &Config,
    map: &mut HashMap<i64, i64>,
    links: &mut Links,
    receipts: &mut Receipts,
    user_id: i64,
    text: &str,
    chat_id: i64,
    max_message: i64,
    max_time: i64,
    bold_prefix: &str,
    rich: Option<Value>,
) -> Result<(), String> {
    let message_id = send_text(http, config, user_id, None, None, text, bold_prefix, rich).await?;
    note_telegram_message(
        config,
        map,
        links,
        receipts,
        message_id,
        chat_id,
        max_message,
        max_time,
    );
    Ok(())
}

async fn send_in_topic(
    http: &mut Http,
    config: &Config,
    client: &mut Client,
    topics: &mut HashMap<String, Topic>,
    links: &mut Links,
    receipts: &mut Receipts,
    tg_user: i64,
    max_chat: i64,
    max_message: i64,
    max_time: i64,
    label: &str,
    text: &str,
    reply_to: Option<i64>,
    bold_prefix: &str,
    rich: Option<Value>,
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
        rich.clone(),
    )
    .await
    {
        Ok(id) => id,
        Err(err) if thread_missing(&err) => {
            topics.remove(&key);
            config.store.save_topics(topics);
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
                rich,
            )
            .await?
        }
        Err(err) => return Err(err),
    };
    remember_link(links, tg_message, max_message);
    config.store.save_links(links);
    remember_inbound(
        receipts,
        &config.store,
        tg_message,
        max_chat,
        max_message,
        max_time,
    );
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
                    config.store.save_topics(topics);
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
    config.store.save_topics(topics);
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
        config.store.save_topics(topics);
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
    rich: Option<Value>,
) -> Result<i64, String> {
    send_text(
        http,
        config,
        tg_user,
        Some(thread_id),
        reply_to,
        text,
        bold_prefix,
        rich,
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
    rich: Option<Value>,
) -> Result<i64, String> {
    if let Some(rich) = rich.or_else(|| named_rich(bold_prefix, text)) {
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

async fn set_reaction(
    http: &Http,
    token: &str,
    chat_id: i64,
    message_id: i64,
    emoji: &str,
) -> Result<(), String> {
    telegram(
        http,
        token,
        "setMessageReaction",
        json!({
            "chat_id": chat_id,
            "message_id": message_id,
            "reaction": [{"type": "emoji", "emoji": emoji}],
        }),
    )
    .await
    .map(|_| ())
}

async fn send_media(
    http: &mut Http,
    config: &Config,
    client: &mut Client,
    map: &mut HashMap<i64, i64>,
    links: &mut Links,
    receipts: &mut Receipts,
    topics: &mut HashMap<String, Topic>,
    tg_user: i64,
    max_chat: i64,
    max_message: i64,
    max_time: i64,
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
    note_telegram_message(
        config,
        map,
        links,
        receipts,
        tg_id,
        max_chat,
        max_message,
        max_time,
    );
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
            config.store.save_topics(topics);
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
            if mime == "image/gif" {
                telegram_file(
                    http,
                    &config.token,
                    "sendAnimation",
                    fields,
                    "animation",
                    "animation.gif",
                    mime,
                    bytes,
                )
                .await?
            } else {
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
        Media::Video { .. } => {
            // MAX marks many short square clips (including Telegram GIFs) as
            // videoType=1. Prefer animation so they stay rectangular in TG.
            match telegram_file(
                http,
                &config.token,
                "sendAnimation",
                fields.clone(),
                "animation",
                "animation.mp4",
                "video/mp4",
                bytes,
            )
            .await
            {
                Ok(sent) => sent,
                Err(err) => {
                    eprintln!("bridge: animation send failed: {err}");
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
            }
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
    receipts: &mut Receipts,
    tg_id: i64,
    max_chat: i64,
    max_message: i64,
    time: i64,
) {
    if config.mode == Mode::Chat {
        map.insert(tg_id, max_chat);
        config.store.save_map(map);
    }
    remember_link(links, tg_id, max_message);
    config.store.save_links(links);
    remember_inbound(receipts, &config.store, tg_id, max_chat, max_message, time);
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
    let name = json!({"type": "bold", "text": prefix});
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

fn special_rich(prefix: &str, lead: &str, specials: &[SpecialAttach]) -> Option<Value> {
    let known: Vec<&SpecialAttach> = specials
        .iter()
        .filter(|item| is_known_special(item))
        .collect();
    if known.is_empty() {
        return None;
    }
    let mut blocks: Vec<Vec<Value>> = Vec::new();
    let lead = lead.trim();
    if !prefix.is_empty() || !lead.is_empty() {
        let mut parts = Vec::new();
        if !prefix.is_empty() {
            parts.push(json!({"type": "bold", "text": prefix}));
        }
        if !lead.is_empty() {
            if !parts.is_empty() {
                parts.push(json!("\n\n"));
            }
            parts.push(json!(lead));
        }
        blocks.push(parts);
    }
    for item in &known {
        blocks.push(special_parts(item));
    }
    let anchor = known
        .iter()
        .rev()
        .find_map(|item| special_line(item))
        .map(|line| last_line(&line).to_owned())
        .filter(|line| !line.is_empty())
        .unwrap_or_else(|| {
            if !lead.is_empty() {
                last_line(lead).to_owned()
            } else {
                prefix.to_owned()
            }
        });
    let pad = clock_pad(&anchor);
    if !pad.is_empty() {
        if let Some(last) = blocks.last_mut() {
            last.push(json!(pad));
        }
    }
    let blocks: Vec<Value> = blocks
        .into_iter()
        .map(|text| json!({"type": "paragraph", "text": text}))
        .collect();
    Some(json!({"blocks": blocks}))
}

fn special_plain(prefix: &str, lead: &str, specials: &[SpecialAttach]) -> String {
    let mut parts = Vec::new();
    if !prefix.is_empty() {
        parts.push(prefix.to_owned());
    }
    let lead = lead.trim();
    if !lead.is_empty() {
        parts.push(lead.to_owned());
    }
    for item in specials {
        if let Some(line) = special_line(item) {
            parts.push(line);
        }
    }
    parts.join("\n\n")
}

fn special_parts(item: &SpecialAttach) -> Vec<Value> {
    match item {
        SpecialAttach::Contact { name, phone } => contact_parts(name, phone.as_deref()),
        other => vec![json!(special_line(other).unwrap_or_default())],
    }
}

fn contact_parts(name: &str, phone: Option<&str>) -> Vec<Value> {
    let label = if name.is_empty() {
        text::CONTACT_CARD.to_owned()
    } else {
        name.to_owned()
    };
    let mut parts = vec![json!({"type": "bold", "text": label})];
    if let Some(phone) = phone {
        let shown = format_phone(phone);
        if !shown.is_empty() {
            parts.push(json!("\n"));
            parts.push(json!(shown));
        }
    }
    parts
}

fn special_line(item: &SpecialAttach) -> Option<String> {
    match item {
        SpecialAttach::Contact { name, phone } => {
            let label = if name.is_empty() {
                text::CONTACT_CARD
            } else {
                name.as_str()
            };
            let mut line = label.to_owned();
            if let Some(phone) = phone {
                let shown = format_phone(phone);
                if !shown.is_empty() {
                    line.push('\n');
                    line.push_str(&shown);
                }
            }
            Some(line)
        }
        SpecialAttach::File { name, size } => {
            let name = if name.is_empty() {
                text::FILE_CARD.to_owned()
            } else {
                name.clone()
            };
            if *size > 0 {
                Some(format!("{name}\n{}", file_size(*size)))
            } else {
                Some(name)
            }
        }
        SpecialAttach::Sticker { url } => {
            if url.is_empty() {
                Some(text::STICKER_CARD.to_owned())
            } else {
                Some(format!("{}\n{url}", text::STICKER_CARD))
            }
        }
        SpecialAttach::Share {
            url,
            title,
            description,
        } => {
            let mut lines = Vec::new();
            if !title.is_empty() {
                lines.push(title.clone());
            }
            if !description.is_empty() {
                lines.push(description.clone());
            }
            if !url.is_empty() {
                lines.push(url.clone());
            }
            if lines.is_empty() {
                lines.push(text::LINK_CARD.to_owned());
            }
            Some(lines.join("\n"))
        }
        SpecialAttach::Call {
            call_type,
            hangup,
            duration,
        } => {
            let mut parts = vec![call_kind(call_type.as_deref())];
            if let Some(hangup) = hangup {
                parts.push(call_hangup(hangup));
            }
            if let Some(duration) = duration {
                let clock = call_clock(*duration);
                if !clock.is_empty() {
                    parts.push(clock);
                }
            }
            Some(parts.join(" · "))
        }
        SpecialAttach::Poll { title, answers } => {
            let mut lines = Vec::new();
            if title.is_empty() {
                lines.push(text::POLL_CARD.to_owned());
            } else {
                lines.push(title.clone());
            }
            for answer in answers {
                lines.push(format!("• {answer}"));
            }
            Some(lines.join("\n"))
        }
        SpecialAttach::Keyboard { labels } => {
            if labels.is_empty() {
                Some(text::BUTTONS_CARD.to_owned())
            } else {
                Some(format!("{}\n{}", text::BUTTONS_CARD, labels.join(" · ")))
            }
        }
        SpecialAttach::Unknown { .. } => None,
    }
}

fn is_known_special(item: &SpecialAttach) -> bool {
    !matches!(item, SpecialAttach::Unknown { .. })
}

fn unknown_values(specials: &[SpecialAttach]) -> Vec<&Value> {
    specials
        .iter()
        .filter_map(|item| match item {
            SpecialAttach::Unknown { raw } => Some(raw),
            _ => None,
        })
        .collect()
}

fn unknown_text(items: &[&Value]) -> String {
    let value = if items.len() == 1 {
        items[0].clone()
    } else {
        Value::Array(items.iter().map(|item| (*item).clone()).collect())
    };
    serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string())
}

fn format_phone(raw: &str) -> String {
    let digits: String = raw.chars().filter(|ch| ch.is_ascii_digit()).collect();
    let national = if digits.len() == 11 && (digits.starts_with('7') || digits.starts_with('8')) {
        Some(&digits[1..])
    } else if digits.len() == 10 {
        Some(digits.as_str())
    } else {
        None
    };
    let Some(national) = national else {
        return raw.trim().to_owned();
    };
    format!(
        "8 ({}) {}-{}-{}",
        &national[0..3],
        &national[3..6],
        &national[6..8],
        &national[8..10]
    )
}

fn file_size(size: i64) -> String {
    const KB: i64 = 1024;
    const MB: i64 = 1024 * 1024;
    const GB: i64 = 1024 * 1024 * 1024;
    if size >= GB {
        format!("{} GB", size / GB)
    } else if size >= MB {
        format!("{} MB", size / MB)
    } else if size >= KB {
        format!("{} KB", size / KB)
    } else {
        format!("{size} B")
    }
}

fn call_kind(kind: Option<&str>) -> String {
    match kind {
        Some("AUDIO") => text::CALL_AUDIO.to_owned(),
        Some("VIDEO") => text::CALL_VIDEO.to_owned(),
        Some(other) if !other.is_empty() => other.to_owned(),
        _ => text::CALL_CARD.to_owned(),
    }
}

fn call_hangup(kind: &str) -> String {
    match kind {
        "MISSED" => text::CALL_MISSED.to_owned(),
        "REJECTED" => text::CALL_REJECTED.to_owned(),
        "CANCELED" => text::CALL_CANCELED.to_owned(),
        "HUNGUP" => text::CALL_HUNGUP.to_owned(),
        other => other.to_owned(),
    }
}

fn call_clock(duration: i64) -> String {
    if duration <= 0 {
        return String::new();
    }
    let secs = if duration >= 1000 {
        duration / 1000
    } else {
        duration
    };
    format!("{}:{:02}", secs / 60, secs % 60)
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
    if let Some(body) = system_body(message) {
        return body;
    }
    if !message.media.is_empty() {
        return String::new();
    }
    if !message.specials.is_empty() {
        return String::new();
    }
    if message.status.as_deref() == Some("REMOVED") {
        text::DELETED.to_owned()
    } else {
        text::NOT_FORWARDED.to_owned()
    }
}

fn plain_body(message: &Message) -> String {
    if !message.text.trim().is_empty() {
        return message.text.clone();
    }
    if let Some(body) = system_body(message) {
        return body;
    }
    text::NO_TEXT.to_owned()
}

fn system_body(message: &Message) -> Option<String> {
    let control = message.control.as_ref()?;
    let title = control
        .title
        .as_deref()
        .map(str::trim)
        .filter(|title| !title.is_empty());
    match (control.event.as_deref(), title) {
        (Some("new"), Some(title)) => Some(text::chat_created(title)),
        _ => Some(control_line(control)),
    }
}

fn control_line(control: &max_client::ControlAttach) -> String {
    let mut parts = Vec::new();
    for value in [
        control.event.as_deref(),
        control.action.as_deref(),
        control.title.as_deref(),
    ] {
        if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
            parts.push(value.to_owned());
        }
    }
    if parts.is_empty() {
        text::SYSTEM_EVENT.to_owned()
    } else {
        parts.join(" · ")
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
        beat();
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
    receipts: &mut Receipts,
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
    config.store.save_topics(topics);
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
    config.store.save_links(links);
    receipts.out.retain(|_, item| item.tg_chat != tg_user);
    receipts.inbound.clear();
    receipts.pending_marks.clear();
    config.store.save_receipts(receipts);
    map.clear();
    config.store.save_map(map);
    eprintln!("bridge: cleared topics and bot history");
}

async fn wipe_topic(
    config: &Config,
    http: &Http,
    client: &mut Client,
    links: &mut Links,
    topics: &mut HashMap<String, Topic>,
    seen: &mut HashMap<i64, Cursor>,
    tg_user: i64,
    chat_id: i64,
) -> Result<(), String> {
    let key = topic_key(tg_user, chat_id);
    if let Some(topic) = topics.remove(&key) {
        if let Err(err) = delete_topic(http, config, tg_user, topic.thread_id).await {
            topics.insert(key, topic);
            config.store.save_topics(topics);
            return Err(err);
        }
        config.store.save_topics(topics);
    }
    match read_chat_history(client, chat_id).await {
        Ok(history) => {
            drop_chat_links(links, &history);
            config.store.save_links(links);
        }
        Err(err) => eprintln!("bridge: history links kept for chat {chat_id}: {err}"),
    }
    seen.remove(&chat_id);
    config.store.save_seen(seen);
    eprintln!("bridge: cleared topic for chat {chat_id}");
    Ok(())
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
    contact_name: &str,
) -> Result<(), String> {
    let mut face = client.chat_face(chat_id).await;
    if face.label.is_empty() && !contact_name.is_empty() {
        face.label = contact_name.to_owned();
    }
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
    receipts: &mut Receipts,
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
                config.store.save_topics(topics);
                return Err(err);
            }
            config.store.save_topics(topics);
            drop_chat_links(links, &history);
            config.store.save_links(links);
            open_history_topic(config, http, client, topics, tg_user, chat_id, "").await?;
            eprintln!("bridge: topic recreated for chat {chat_id}");
            rewrite = true;
        }
    }
    let mut wrote = 0u32;
    for message in &history {
        beat();
        if !rewrite && links.max.contains_key(&message.id) {
            continue;
        }
        let mut outgoing = message.clone();
        outgoing.text = history_body(message);
        let mut sent = false;
        for attempt in 0..3 {
            match forward_max(
                config, http, client, map, links, receipts, topics, &outgoing, me, true,
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
            mark_seen(seen, &config.store, message);
        }
    }
    if wrote > 0 {
        eprintln!("bridge: history wrote {wrote} messages for chat {chat_id}");
    }
    Ok(())
}

fn history_chat_for_thread(
    topics: &HashMap<String, Topic>,
    tg_user: i64,
    thread_id: Option<i64>,
) -> Option<i64> {
    topic_chat(topics, tg_user, thread_id?)
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

fn history_topic_ids(started: &[i64], quiet: &[i64]) -> Vec<i64> {
    let mut ids = started.to_vec();
    for chat_id in quiet {
        if !ids.contains(chat_id) {
            ids.push(*chat_id);
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
    receipts: &mut Receipts,
    topics: &mut HashMap<String, Topic>,
    seen: &mut HashMap<i64, Cursor>,
    me: Option<i64>,
    tg_user: i64,
) -> Result<(), String> {
    let known = client.chat_ids().await.map_err(|err| err.to_string())?;
    let started = history_chat_ids(&known, seen, topics, tg_user);
    let quiet = match client.quiet_dialog_ids().await {
        Ok(ids) => ids,
        Err(err) => {
            eprintln!("bridge: contact list failed: {err}");
            Vec::new()
        }
    };
    let chats = history_topic_ids(&started, &quiet);
    eprintln!("bridge: history found {} chats", started.len());
    eprintln!(
        "bridge: history found {} contacts with no chat",
        chats.len().saturating_sub(started.len())
    );
    let mut ready = Vec::new();
    for chat_id in chats {
        let fresh = !topics.contains_key(&topic_key(tg_user, chat_id));
        if let Err(err) =
            open_history_topic(config, http, client, topics, tg_user, chat_id, "").await
        {
            eprintln!("bridge: history topic failed for chat {chat_id}: {err}");
            continue;
        }
        ready.push((chat_id, fresh));
    }
    eprintln!("bridge: history opened {} topics", ready.len());
    for (chat_id, fresh) in ready {
        if !started.contains(&chat_id) {
            continue;
        }
        if let Err(err) = load_topic_history(
            config, http, client, map, links, receipts, topics, seen, me, tg_user, chat_id, fresh,
        )
        .await
        {
            eprintln!("bridge: history load failed for chat {chat_id}: {err}");
        }
    }
    Ok(())
}

async fn notify_main(http: &Http, config: &Config, text: &str) -> bool {
    let mut sent = false;
    for user_id in &config.allowed {
        let body = json!({"chat_id": user_id, "text": text});
        if telegram(http, &config.token, "sendMessage", body)
            .await
            .is_ok()
        {
            sent = true;
        }
    }
    sent
}

async fn maybe_alert(http: &Http, config: &Config) {
    let count = ERRORS.load(Ordering::Relaxed);
    let Some(mark) = alert_mark(count) else {
        return;
    };
    if ERRORS_TOLD.load(Ordering::Relaxed) >= mark {
        return;
    }
    let text = format!("{} ({count})", text::ERROR_BUDGET);
    if notify_main(http, config, &text).await {
        ERRORS_TOLD.store(mark, Ordering::Relaxed);
    } else {
        console_alert(&format!("bridge errors {count}, telegram is not connected"));
    }
}

async fn post_status(
    http: &Http,
    config: &Config,
    tg_user: i64,
    thread_id: Option<i64>,
    text: &str,
) {
    let mut body = json!({
        "chat_id": tg_user,
        "text": text,
    });
    if let Some(thread_id) = thread_id {
        body["message_thread_id"] = json!(thread_id);
    }
    let _ = telegram(http, &config.token, "sendMessage", body).await;
}

async fn run_history(
    config: &Config,
    http: &mut Http,
    client: &mut Client,
    map: &mut HashMap<i64, i64>,
    links: &mut Links,
    receipts: &mut Receipts,
    topics: &mut HashMap<String, Topic>,
    seen: &mut HashMap<i64, Cursor>,
    me: Option<i64>,
    tg_user: i64,
    status_thread: Option<i64>,
    reread: bool,
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
    let chat_id = history_chat_for_thread(topics, tg_user, status_thread);
    if reread {
        if let Some(chat_id) = chat_id {
            if let Err(err) =
                wipe_topic(config, http, client, links, topics, seen, tg_user, chat_id).await
            {
                eprintln!("bridge: topic reread failed for chat {chat_id}: {err}");
                return Ok(());
            }
        } else {
            wipe_bot_chat(config, http, map, links, receipts, topics, tg_user).await;
        }
    }
    if let Some(chat_id) = chat_id {
        let fresh = reread || !topics.contains_key(&topic_key(tg_user, chat_id));
        if let Err(err) =
            open_history_topic(config, http, client, topics, tg_user, chat_id, "").await
        {
            eprintln!("bridge: history topic failed for chat {chat_id}: {err}");
            return Ok(());
        }
        let thread_id = if reread {
            topics
                .get(&topic_key(tg_user, chat_id))
                .map(|topic| topic.thread_id)
        } else {
            status_thread
        };
        let loading = if reread {
            text::HISTORY_REREAD_TOPIC
        } else {
            text::HISTORY_LOADING_TOPIC
        };
        let done = if reread {
            text::HISTORY_REREAD_DONE_TOPIC
        } else {
            text::HISTORY_DONE_TOPIC
        };
        post_status(http, config, tg_user, thread_id, loading).await;
        if let Err(err) = load_topic_history(
            config, http, client, map, links, receipts, topics, seen, me, tg_user, chat_id, fresh,
        )
        .await
        {
            eprintln!("bridge: history load failed for chat {chat_id}: {err}");
        }
        post_status(http, config, tg_user, thread_id, done).await;
        return Ok(());
    }
    post_status(http, config, tg_user, None, text::HISTORY_LOADING).await;
    load_all_history(
        config, http, client, map, links, receipts, topics, seen, me, tg_user,
    )
    .await?;
    post_status(http, config, tg_user, None, text::HISTORY_DONE).await;
    Ok(())
}

async fn handle_update(
    config: &Config,
    http: &mut Http,
    client: &mut Option<Client>,
    session_link: &watch::Sender<Option<SessionLink>>,
    link: &mut LinkState,
    map: &mut HashMap<i64, i64>,
    links: &mut Links,
    receipts: &mut Receipts,
    topics: &mut HashMap<String, Topic>,
    seen: &mut HashMap<i64, Cursor>,
    echoes: &mut HashSet<i64>,
    me: &mut Option<i64>,
    panels: &mut HashMap<i64, i64>,
    logout_asks: &mut HashMap<i64, i64>,
    update: &Update,
) -> Result<(), String> {
    if let Some(reaction) = &update.reaction {
        return handle_read_reaction(config, http, client, receipts, reaction).await;
    }
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
                begin_logout(
                    config,
                    http,
                    client,
                    session_link,
                    link,
                    me,
                    panels,
                    logout_asks,
                    user_id,
                )
                .await;
                return Ok(());
            }
            Some("logout_yes") => {
                wipe_bot_chat(config, http, map, links, receipts, topics, user_id).await;
                seen.clear();
                config.store.save_seen(seen);
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
        let reread = action == Some("reread");
        let status_thread = callback.message.as_ref().and_then(|message| {
            message
                .thread_id
                .filter(|thread| topic_chat(topics, user_id, *thread).is_some())
        });
        return run_history(
            config,
            http,
            active,
            map,
            links,
            receipts,
            topics,
            seen,
            *me,
            user_id,
            status_thread,
            reread,
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
        forget_session(&config.store);
        sync_controls(http, config, *link, panels).await;
        return Ok(());
    }
    if is_logout_command(&text) {
        begin_logout(
            config,
            http,
            client,
            session_link,
            link,
            me,
            panels,
            logout_asks,
            from,
        )
        .await;
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
        return run_history(
            config,
            http,
            active,
            map,
            links,
            receipts,
            topics,
            seen,
            *me,
            from,
            status_thread,
            reread,
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
    if let Some((file_id, video_type)) = video_clip {
        let kind = if message.video_note.is_some() {
            "video_note"
        } else if message.animation.is_some() {
            "animation"
        } else {
            "video"
        };
        eprintln!("bridge: tg media {kind} file={file_id} video_type={video_type:?}");
    }
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
    let (sent, echo) = if let Some((bytes, duration_ms)) = voice_file {
        (
            send_voice_on_tcp(
                client,
                chat_id,
                &text,
                reply_to,
                &bytes,
                duration_ms,
                attaches,
            )
            .await,
            true,
        )
    } else if let Some((bytes, video_type)) = video_file {
        if video_type != Some(1) && bytes.starts_with(b"GIF8") {
            match upload_photo(client, &bytes).await {
                Ok(token) => {
                    let mut photos = attaches;
                    photos.push(Attach::Photo { token });
                    (
                        send_with_voice_retry(client, chat_id, &text, reply_to, photos).await,
                        true,
                    )
                }
                Err(err) => {
                    eprintln!("bridge: gif photo upload failed: {err}");
                    let body = text.as_str();
                    (
                        send_video_on_tcp(
                            client,
                            chat_id,
                            body,
                            reply_to,
                            &bytes,
                            Some(0),
                            attaches,
                        )
                        .await,
                        true,
                    )
                }
            }
        } else {
            let body = if video_type == Some(1) {
                ""
            } else {
                text.as_str()
            };
            (
                send_video_on_tcp(
                    client, chat_id, body, reply_to, &bytes, video_type, attaches,
                )
                .await,
                true,
            )
        }
    } else {
        (
            send_with_voice_retry(client, chat_id, &text, reply_to, attaches).await,
            false,
        )
    };
    let sent = match sent {
        Ok(sent) => {
            if echo {
                if let Some(max_id) = sent {
                    echoes.insert(max_id);
                }
            }
            sent
        }
        Err(err) if contact_error_kind(&err) == ContactErrorKind::Privacy => {
            eprintln!("bridge: {err}");
            post_status(http, config, from, message.thread_id, text::CONTACT_PRIVACY).await;
            return Ok(());
        }
        Err(err) => return Err(err),
    };
    if message.message_id != 0 {
        if let Some(max_id) = sent {
            remember_link(links, message.message_id, max_id);
            config.store.save_links(links);
            remember_outbound(
                receipts,
                &config.store,
                max_id,
                from,
                message.message_id,
                chat_id,
                unix_ms(),
            );
            if let Err(err) =
                set_reaction(http, &config.token, from, message.message_id, REACT_SENT).await
            {
                eprintln!("bridge: reaction failed: {err}");
            }
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
        // Explicit 0: short square MP4 GIFs become circular notes without it.
        return Some((animation.file_id.as_str(), Some(0)));
    }
    message
        .video
        .as_ref()
        .filter(|video| !video.file_id.is_empty())
        .map(|video| (video.file_id.as_str(), Some(0)))
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

fn start_telegram(
    token: &str,
    offset: i64,
) -> (
    watch::Sender<i64>,
    mpsc::Receiver<Result<Vec<Update>, String>>,
) {
    let (offset_tx, offset_rx) = watch::channel(offset);
    let (poll_tx, poll_rx) = mpsc::channel(4);
    let token = token.to_owned();
    tokio::spawn(async move {
        telegram_supervisor(token, offset_rx, poll_tx).await;
    });
    (offset_tx, poll_rx)
}

async fn telegram_supervisor(
    token: String,
    offset_rx: watch::Receiver<i64>,
    poll_tx: mpsc::Sender<Result<Vec<Update>, String>>,
) {
    loop {
        note_telegram();
        let task = tokio::spawn(telegram_poll_loop(
            token.clone(),
            offset_rx.clone(),
            poll_tx.clone(),
        ));
        {
            let mut slot = TELEGRAM_ABORT.lock().unwrap_or_else(|err| err.into_inner());
            *slot = Some(task.abort_handle());
        }
        match task.await {
            Ok(false) => return,
            Ok(true) => {
                record_error("telegram", "poll task stopped");
                note_telegram();
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(err) if err.is_cancelled() => {
                note_telegram();
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(_) => {
                record_error("telegram", "poll task panicked");
                note_telegram();
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn telegram_poll_loop(
    token: String,
    mut offset_rx: watch::Receiver<i64>,
    poll_tx: mpsc::Sender<Result<Vec<Update>, String>>,
) -> bool {
    let mut http = loop {
        note_telegram();
        match telegram_http().await {
            Ok(http) => break http,
            Err(err) => {
                record_error("telegram", &err);
                if poll_tx.send(Err(err)).await.is_err() {
                    return false;
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    };
    loop {
        note_telegram();
        let offset = *offset_rx.borrow_and_update();
        let polled = poll_telegram(&http, &token, offset).await;
        let failed = polled.is_err();
        let pause = polled
            .as_ref()
            .err()
            .map(|err| error_pause(err))
            .unwrap_or(0);
        let advanced = polled
            .as_ref()
            .ok()
            .is_some_and(|updates| !updates.is_empty());
        if failed {
            if let Err(err) = &polled {
                record_error("telegram", err);
            }
        }
        if poll_tx.send(polled).await.is_err() {
            return false;
        }
        if failed {
            tokio::time::sleep(Duration::from_secs(pause)).await;
            note_telegram();
            if let Ok(fresh) = telegram_http().await {
                http = fresh;
            }
            continue;
        }
        if advanced {
            loop {
                note_telegram();
                tokio::select! {
                    changed = offset_rx.changed() => {
                        if changed.is_err() {
                            return false;
                        }
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                }
            }
        }
    }
}

fn error_pause(err: &str) -> u64 {
    if err.contains("502") {
        5
    } else {
        3
    }
}

async fn telegram_http() -> Result<Http, String> {
    let addrs = lookup_v4_all(TELEGRAM_HOST, 443)
        .await
        .map_err(|err| err.to_string())?;
    let index = TELEGRAM_ADDR.fetch_add(1, Ordering::Relaxed);
    let addr = addrs[index % addrs.len()];
    Http::builder()
        .resolve(TELEGRAM_HOST, addr)
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(60))
        .tcp_keepalive(Duration::from_secs(15))
        .http1_only()
        .build()
        .map_err(|err| err.to_string())
}

fn http_failure(err: reqwest::Error) -> String {
    let mut text = redact_telegram(&err.to_string());
    let mut source = std::error::Error::source(&err);
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = std::error::Error::source(cause);
    }
    text
}

fn redact_telegram(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find("/bot") {
        let token_at = start + 4;
        out.push_str(&rest[..token_at]);
        let after = &rest[token_at..];
        let end = after.find('/').unwrap_or(after.len());
        if end == 0 {
            break;
        }
        out.push('…');
        rest = &after[end..];
    }
    out.push_str(rest);
    out
}

async fn poll_telegram(http: &Http, token: &str, offset: i64) -> Result<Vec<Update>, String> {
    note_telegram();
    beat();
    let url = format!("https://api.telegram.org/bot{token}/getUpdates");
    let allowed = serde_json::to_string(&["message", "callback_query", "message_reaction"])
        .unwrap_or_else(|_| "[]".into());
    let response = http
        .get(url)
        .query(&[
            ("timeout", "25"),
            ("offset", &offset.to_string()),
            ("allowed_updates", &allowed),
        ])
        .send()
        .await
        .map_err(http_failure)?;
    let status = response.status();
    let body: Value = response.json().await.map_err(http_failure)?;
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
    if video_type == Some(1) {
        return send_video_note_on_tcp(client, chat_id, reply_to, bytes, photos).await;
    }
    let started = std::time::Instant::now();
    eprintln!("bridge: max video send note=false bytes={}", bytes.len());
    let mut link = client.open_voice().await.map_err(|err| err.to_string())?;
    eprintln!(
        "bridge: video session open ms={}",
        started.elapsed().as_millis()
    );
    let ticket = link
        .ticket_video(false)
        .await
        .map_err(|err| err.to_string())?;
    let (video_id, token) = upload_video(&ticket.url, ticket.video_id, ticket.token, bytes).await?;
    eprintln!(
        "bridge: video uploaded video_id={video_id} ms={}",
        started.elapsed().as_millis()
    );
    let ready = link
        .wait_video_ready(video_id, Duration::from_secs(8))
        .await
        .map_err(|err| err.to_string())?;
    eprintln!(
        "bridge: video ready={} video_id={video_id} ms={}",
        ready,
        started.elapsed().as_millis()
    );
    let sent = link
        .finish_video(
            chat_id,
            text,
            reply_to,
            video_id,
            &token,
            Some(0),
            None,
            &photos,
        )
        .await
        .map(|stamp| stamp.map(|(id, _)| id))
        .map_err(|err| err.to_string())?;
    eprintln!(
        "bridge: max video sent id={sent:?} note=false ms={}",
        started.elapsed().as_millis()
    );
    Ok(sent)
}

/// Video note path (mobile / max-account-api):
/// op 82 type=1/uploaderType=0 → OK-CDN POST → optional VIDEO_READY →
/// attach token + videoType:1 + thumbhash.
async fn send_video_note_on_tcp(
    client: &Client,
    chat_id: i64,
    reply_to: Option<i64>,
    bytes: &[u8],
    photos: Vec<Attach>,
) -> Result<Option<i64>, String> {
    let started = std::time::Instant::now();
    eprintln!("bridge: max video_note send bytes={}", bytes.len());
    let mut link = client.open_voice().await.map_err(|err| err.to_string())?;
    eprintln!(
        "bridge: video_note session open ms={}",
        started.elapsed().as_millis()
    );
    let ticket = link
        .ticket_video(true)
        .await
        .map_err(|err| err.to_string())?;
    let (video_id, token, thumbhash) =
        upload_video_note(&ticket.url, ticket.video_id, ticket.token, bytes).await?;
    eprintln!(
        "bridge: video_note uploaded video_id={video_id} thumb={} ms={}",
        thumbhash.is_some(),
        started.elapsed().as_millis()
    );
    let ready = link
        .wait_video_ready(video_id, Duration::from_secs(8))
        .await
        .map_err(|err| err.to_string())?;
    eprintln!(
        "bridge: video_note ready={} video_id={video_id} ms={}",
        ready,
        started.elapsed().as_millis()
    );
    let sent = link
        .finish_video(
            chat_id,
            "",
            reply_to,
            video_id,
            &token,
            Some(1),
            thumbhash.as_deref(),
            &photos,
        )
        .await
        .map(|stamp| stamp.map(|(id, _)| id))
        .map_err(|err| err.to_string())?;
    eprintln!(
        "bridge: max video_note sent id={sent:?} ms={}",
        started.elapsed().as_millis()
    );
    Ok(sent)
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
    // PyMax regular video: filename= + Content-Range + Content-Length.
    let file_name = format!("video-{len}.mp4");
    let response = post_https(url, "video upload failed", |builder| {
        builder
            .header(
                "Content-Disposition",
                format!("attachment; filename={file_name}"),
            )
            .header("Content-Range", range)
            .header("Content-Length", len.to_string())
            .header("Connection", "keep-alive")
            .body(body)
    })
    .await?;
    let body = response
        .text()
        .await
        .map_err(|_| "video upload failed".to_owned())?;
    let note = upload_body_note(&body);
    eprintln!("bridge: video upload body {note}");
    if note.starts_with("len=") {
        let preview: String = body.chars().take(80).collect();
        eprintln!("bridge: video upload raw {preview:?}");
    }
    if let Some(err) = upload_error(&body) {
        return Err(err);
    }
    let token = uploaded_token(&body).unwrap_or(fallback_token);
    Ok((video_id, token))
}

async fn upload_video_note(
    url: &str,
    video_id: i64,
    fallback_token: String,
    bytes: &[u8],
) -> Result<(i64, String, Option<Vec<u8>>), String> {
    if bytes.is_empty() || bytes.len() > MAX_MEDIA_BYTES {
        return Err(media_size_error(bytes.len()));
    }
    let len = bytes.len();
    eprintln!(
        "bridge: video_note upload bytes={len} place={}",
        upload_place(url)
    );
    let body = bytes.to_vec();
    let range = content_range_header(len);
    // Mobile OK-CDN shape (ru.oneme.app): x-binary + fileName= + X-Uploading-Mode.
    // Filename is an opaque negative digit string, not a real .mp4 name.
    let sniff_name = format!("-{}", video_id.unsigned_abs() % 2_147_483_647);
    let response = post_https(url, "video_note upload failed", |builder| {
        builder
            .header(
                "Content-Type",
                "application/x-binary; charset=x-user-defined",
            )
            .header(
                "Content-Disposition",
                format!("attachment; fileName=\"{sniff_name}\""),
            )
            .header("Content-Range", range)
            .header("Content-Length", len.to_string())
            .header("X-Uploading-Mode", "parallel")
            .header("Connection", "keep-alive")
            .body(body)
    })
    .await?;
    let body = response
        .text()
        .await
        .map_err(|_| "video_note upload failed".to_owned())?;
    let note = upload_body_note(&body);
    eprintln!("bridge: video_note upload body {note}");
    if note.starts_with("len=") {
        let preview: String = body.chars().take(80).collect();
        eprintln!("bridge: video_note upload raw {preview:?}");
    }
    if let Some(err) = upload_error(&body) {
        let preview: String = body.chars().take(200).collect();
        eprintln!("bridge: video_note upload error raw {preview}");
        return Err(err);
    }
    let token = uploaded_token(&body).unwrap_or(fallback_token);
    let thumbhash = upload_thumbhash(&body);
    Ok((video_id, token, thumbhash))
}

fn upload_thumbhash(body: &str) -> Option<Vec<u8>> {
    use base64::Engine;
    let value: Value = serde_json::from_str(body.trim()).ok()?;
    let raw = value.get("thumbhash").and_then(Value::as_str)?;
    if raw.is_empty() {
        return None;
    }
    let mut padded = raw.to_owned();
    while padded.len() % 4 != 0 {
        padded.push('=');
    }
    base64::engine::general_purpose::STANDARD
        .decode(padded)
        .ok()
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
        return Some("upload rejected".into());
    }
    let value: Value = serde_json::from_str(trimmed).ok()?;
    let code = value
        .get("error_code")
        .or_else(|| value.get("errorCode"))
        .and_then(|item| {
            item.as_str()
                .map(str::to_owned)
                .or_else(|| item.as_i64().map(|n| n.to_string()))
        });
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
    Some(match (code.as_deref(), message) {
        (Some(code), Some(text)) => format!("upload rejected: {code}: {text}"),
        (Some(code), None) => format!("upload rejected: {code}"),
        (None, Some(text)) => format!("upload rejected: {text}"),
        (None, None) => "upload rejected".into(),
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
        .map_err(http_failure)?;
    let status = response.status();
    let payload: Value = response.json().await.map_err(http_failure)?;
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
    beat();
    let url = format!("https://api.telegram.org/bot{token}/{method}");
    let response = http
        .post(url)
        .json(&body)
        .send()
        .await
        .map_err(http_failure)?;
    let status = response.status();
    let payload: Value = response.json().await.map_err(http_failure)?;
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
    #[serde(default, rename = "message_reaction")]
    reaction: Option<MessageReactionUpdate>,
}

#[derive(Debug, Deserialize)]
struct MessageReactionUpdate {
    chat: TgChat,
    message_id: i64,
    user: Option<TgUser>,
    #[serde(default)]
    new_reaction: Vec<TgReaction>,
}

#[derive(Debug, Deserialize)]
struct TgChat {
    id: i64,
}

#[derive(Debug, Deserialize)]
struct TgReaction {
    #[serde(default, rename = "type")]
    kind: String,
    #[serde(default)]
    emoji: Option<String>,
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

fn remember_link(links: &mut Links, tg_message: i64, max_message: i64) {
    links.tg.insert(tg_message, max_message);
    links.max.insert(max_message, tg_message);
}

fn remember_outbound(
    receipts: &mut Receipts,
    store: &Store,
    max_message: i64,
    tg_chat: i64,
    tg_message: i64,
    max_chat: i64,
    time: i64,
) {
    receipts.out.insert(
        max_message,
        OutReceipt {
            tg_chat,
            tg_message,
            max_chat,
            time,
            status: ReceiptStatus::Sent,
        },
    );
    store.save_receipts(receipts);
}

fn remember_inbound(
    receipts: &mut Receipts,
    store: &Store,
    tg_message: i64,
    max_chat: i64,
    max_message: i64,
    time: i64,
) {
    receipts.inbound.insert(
        tg_message,
        InReceipt {
            max_chat,
            max_message,
            time,
        },
    );
    store.save_receipts(receipts);
}

fn raise_out_status(receipt: &mut OutReceipt, next: ReceiptStatus) -> bool {
    if next <= receipt.status {
        return false;
    }
    receipt.status = next;
    true
}

fn status_emoji(status: ReceiptStatus) -> &'static str {
    match status {
        ReceiptStatus::Sent => REACT_SENT,
        ReceiptStatus::Arrived => REACT_ARRIVED,
        ReceiptStatus::Read => REACT_READ,
    }
}

fn pending_out_chats(receipts: &Receipts) -> Vec<i64> {
    let mut chats: Vec<i64> = receipts
        .out
        .values()
        .filter(|item| item.status < ReceiptStatus::Read)
        .map(|item| item.max_chat)
        .collect();
    chats.sort_unstable();
    chats.dedup();
    chats
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
    fn telegram_errors_hide_the_bot_token() {
        let raw = "error sending request for url (https://api.telegram.org/bot123:ABC/getUpdates?timeout=25)";
        let hidden = redact_telegram(raw);
        assert!(!hidden.contains("123:ABC"));
        assert!(hidden.contains("/bot…/getUpdates"));
        let line = error_line("2026-10-26T07:00:00Z", "telegram", raw);
        assert!(!line.contains("123:ABC"));
        assert!(line.starts_with("2026-10-26T07:00:00Z telegram "));
    }

    #[test]
    fn error_budget_starts_after_one_hundred() {
        assert_eq!(alert_mark(100), None);
        assert_eq!(alert_mark(101), Some(100));
        assert_eq!(alert_mark(250), Some(200));
    }

    #[test]
    fn error_stamp_uses_utc() {
        assert_eq!(error_stamp(0), "1970-01-01T00:00:00Z");
        assert_eq!(civil_date(0), (1970, 1, 1));
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
            control: None,
            time: 0,
            media: Vec::new(),
            specials: Vec::new(),
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
            control: None,
            time: 0,
            media: Vec::new(),
            specials: Vec::new(),
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
        assert!(!stalled(None));
        assert!(!stalled(Some(STALL_AFTER_SECS)));
        assert!(stalled(Some(STALL_AFTER_SECS + 1)));
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
    fn inbound_video_marks_gif_as_regular_not_note() {
        let gif = TgMessage {
            message_id: 1,
            from: None,
            text: None,
            caption: None,
            photo: Vec::new(),
            voice: None,
            video: None,
            video_note: None,
            animation: Some(TgFile {
                file_id: "gif1".into(),
            }),
            thread_id: None,
            reply_to: None,
        };
        assert_eq!(inbound_video(&gif), Some(("gif1", Some(0))));
        let note = TgMessage {
            message_id: 2,
            from: None,
            text: None,
            caption: None,
            photo: Vec::new(),
            voice: None,
            video: None,
            video_note: Some(TgFile {
                file_id: "note1".into(),
            }),
            animation: None,
            thread_id: None,
            reply_to: None,
        };
        assert_eq!(inbound_video(&note), Some(("note1", Some(1))));
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
    fn outbound_status_only_moves_forward() {
        let mut receipt = OutReceipt {
            tg_chat: 1,
            tg_message: 2,
            max_chat: 3,
            time: 10,
            status: ReceiptStatus::Sent,
        };
        assert!(raise_out_status(&mut receipt, ReceiptStatus::Arrived));
        assert_eq!(receipt.status, ReceiptStatus::Arrived);
        assert!(!raise_out_status(&mut receipt, ReceiptStatus::Sent));
        assert!(raise_out_status(&mut receipt, ReceiptStatus::Read));
        assert_eq!(status_emoji(receipt.status), REACT_READ);
    }

    #[test]
    fn pending_out_chats_skip_read_receipts() {
        let mut receipts = Receipts::default();
        receipts.out.insert(
            1,
            OutReceipt {
                tg_chat: 9,
                tg_message: 1,
                max_chat: 55,
                time: 10,
                status: ReceiptStatus::Sent,
            },
        );
        receipts.out.insert(
            2,
            OutReceipt {
                tg_chat: 9,
                tg_message: 2,
                max_chat: 55,
                time: 20,
                status: ReceiptStatus::Read,
            },
        );
        receipts.out.insert(
            3,
            OutReceipt {
                tg_chat: 9,
                tg_message: 3,
                max_chat: 77,
                time: 30,
                status: ReceiptStatus::Arrived,
            },
        );
        assert_eq!(pending_out_chats(&receipts), vec![55, 77]);
    }

    #[test]
    fn inbound_receipt_maps_tg_to_max() {
        let dir = std::env::temp_dir().join(format!(
            "bridge-receipts-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let store = Store::open(dir.join("max.db")).unwrap();
        let mut receipts = Receipts::default();
        remember_inbound(&mut receipts, &store, 8, 55, 42, 100);
        let loaded = store.load_receipts();
        assert_eq!(
            loaded.inbound.get(&8).map(|item| item.max_message),
            Some(42)
        );
        assert_eq!(loaded.inbound.get(&8).map(|item| item.max_chat), Some(55));
        let _ = fs::remove_dir_all(&dir);
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
            control: None,
            time,
            media: Vec::new(),
            specials: Vec::new(),
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
        assert_eq!(
            history_topic_ids(&[0, 2, 4, 9], &[9, 15]),
            vec![0, 2, 4, 9, 15]
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
        assert_eq!(history_chat_for_thread(&topics, 7, Some(3)), Some(9));
        assert_eq!(history_chat_for_thread(&topics, 7, Some(40)), None);
        assert_eq!(history_chat_for_thread(&topics, 7, None), None);
        assert!(is_reread_command(text::RELOAD_HISTORY));
        assert_eq!(
            history_chat_for_thread(&topics, 7, inside.thread_id),
            Some(9)
        );
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
            {"type": "bold", "text": "Ната К"},
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
            control: None,
            time: 1,
            media: Vec::new(),
            specials: Vec::new(),
        };
        assert_eq!(history_body(&removed), text::DELETED);
        let file = Message {
            status: None,
            text: String::new(),
            ..removed
        };
        assert_eq!(history_body(&file), text::NOT_FORWARDED);
        assert_eq!(plain_body(&file), text::NO_TEXT);
        let created = Message {
            text: String::new(),
            control: Some(max_client::ControlAttach {
                event: Some("new".into()),
                action: None,
                title: Some("Систэрс".into()),
                chat_type: Some("CHAT".into()),
            }),
            ..file.clone()
        };
        assert_eq!(history_body(&created), text::chat_created("Систэрс"));
        assert!(system_body(&created).is_some());
        let contact = Message {
            text: String::new(),
            specials: vec![SpecialAttach::Contact {
                name: "Светлана".into(),
                phone: Some("79179158935".into()),
            }],
            ..file
        };
        assert_eq!(history_body(&contact), "");
    }

    #[test]
    fn contact_card_is_a_rich_message() {
        let rich = special_rich(
            "",
            "",
            &[SpecialAttach::Contact {
                name: "Светлана".into(),
                phone: Some("79179158935".into()),
            }],
        )
        .unwrap();
        let parts = rich["blocks"][0]["text"].as_array().unwrap();
        assert_eq!(parts[0]["type"], "bold");
        assert_eq!(parts[0]["text"], "Светлана");
        assert_eq!(parts[1], "\n");
        assert_eq!(parts[2], "8 (917) 915-89-35");
        assert_eq!(format_phone("8 (917) 915-89-35"), "8 (917) 915-89-35");
        assert_eq!(format_phone("+7 917 915-89-35"), "8 (917) 915-89-35");
    }

    #[test]
    fn unknown_special_is_plain_json() {
        let raw = json!({"_type": "WIDGET", "foo": 1});
        let specials = [SpecialAttach::Unknown { raw: raw.clone() }];
        assert!(special_rich("", "", &specials).is_none());
        assert_eq!(
            unknown_text(&unknown_values(&specials)),
            serde_json::to_string_pretty(&raw).unwrap()
        );
        let line = untyped_line(15, 9, 4, &raw);
        let logged: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(logged["time"], 15);
        assert_eq!(logged["chatId"], 9);
        assert_eq!(logged["messageId"], 4);
        assert_eq!(logged["attach"], raw);
        assert!(!line.contains(text::NO_TEXT));
    }

    #[test]
    fn panel_keeps_the_mini_app_button() {
        assert!(!history_allowed(LinkState::Off));
        assert!(!history_allowed(LinkState::Connecting));
        assert!(history_allowed(LinkState::Ready));
        let off = panel_markup(LinkState::Off);
        assert_eq!(off["inline_keyboard"][0][0]["text"], text::SETUP);
        assert_eq!(
            off["inline_keyboard"][0][0]["web_app"]["url"],
            auth_http::MINI_APP_URL
        );
        assert!(off["inline_keyboard"][0][0].get("url").is_none());
        let connecting = panel_markup(LinkState::Connecting);
        assert_eq!(connecting["inline_keyboard"][0][0]["text"], text::SETUP);
        assert_eq!(
            connecting["inline_keyboard"][0][0]["web_app"]["url"],
            auth_http::MINI_APP_URL
        );
        let ready = panel_markup(LinkState::Ready);
        assert_eq!(ready["inline_keyboard"][0][0]["text"], text::ADD_CONTACT);
        assert_eq!(
            ready["inline_keyboard"][0][0]["web_app"]["url"],
            auth_http::MINI_APP_URL
        );
        assert!(ready["inline_keyboard"][0][0]
            .get("callback_data")
            .is_none());
        assert_eq!(ready["inline_keyboard"][1][0]["callback_data"], "history");
        assert_eq!(ready["inline_keyboard"][2][0]["callback_data"], "reread");
        assert_eq!(ready["inline_keyboard"][3][0]["callback_data"], "logout");
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
