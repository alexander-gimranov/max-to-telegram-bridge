use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rustls::pki_types::{CertificateDer, ServerName};
use rustls::ClientConfig;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_rustls::TlsConnector;

use crate::error::{Error, Result};
use crate::frame::{
    decode_bin, encode_bin, json_i64, payload_error, request, AUTH, AUTH_LOGIN_CHECK_PASSWORD,
    AUTH_REQUEST, CHATS_LIST, CHAT_HISTORY, CHAT_INFO, CHAT_MARK, CMD_ERROR, CMD_EVENT,
    CONTACT_ADD_BY_PHONE, CONTACT_INFO, LOGIN, MSG_DELIVERY, MSG_SEND, NOTIF_ATTACH, NOTIF_MARK,
    NOTIF_MESSAGE, PHOTO_UPLOAD, PING, SESSION_INIT, VIDEO_PLAY, VIDEO_UPLOAD,
};
use crate::models::{
    ack_payload, add_contact_payload, auth_code_payload, auth_mode, auth_password_payload,
    auth_request_payload, history_payload, login_payload, mark_read_payload, me_id,
    mobile_handshake_payload, parse_added_contact, parse_book, parse_chats, parse_code_challenge,
    parse_code_status, parse_delivery_mark, parse_login_token, parse_message, parse_messages,
    parse_read_mark, parse_users, send_payload, split_contact_name, sync_time, AddedContact,
    Attach, Chat, ChatFace, CodeChallenge, CodeStatus, DeliveryMark, Message, Name, ReadMark, User,
    VoiceTicket,
};
use crate::profile::Session;
use crate::store::Store;
use crate::voice_tcp::VoiceLink;

const QUIET_FOR: Duration = Duration::from_secs(60);
const CMD_WAIT: Duration = Duration::from_secs(90);
const AUTH_HOST: &str = "api2.oneme.ru";
static BEAT: AtomicU64 = AtomicU64::new(0);
static MAX_BEAT: AtomicU64 = AtomicU64::new(0);

pub fn beat() {
    BEAT.store(now_secs(), Ordering::Relaxed);
}

pub fn max_beat() {
    let now = now_secs();
    BEAT.store(now, Ordering::Relaxed);
    MAX_BEAT.store(now, Ordering::Relaxed);
}

pub fn max_beat_age() -> Option<u64> {
    let beat = MAX_BEAT.load(Ordering::Relaxed);
    if beat == 0 {
        None
    } else {
        Some(now_secs().saturating_sub(beat))
    }
}

pub fn beat_age() -> Option<u64> {
    let beat = BEAT.load(Ordering::Relaxed);
    if beat == 0 {
        None
    } else {
        Some(now_secs().saturating_sub(beat))
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
}

#[derive(Debug)]
pub enum Event {
    Ready { me: i64 },
    Message(Message),
    Delivered(DeliveryMark),
    Read(ReadMark),
    Logged { note: String },
    Contact { dialog: i64, name: String },
}

enum Cmd {
    SendText {
        chat_id: i64,
        text: String,
        reply_to: Option<i64>,
        attaches: Vec<Attach>,
        reply: oneshot::Sender<Result<Option<i64>>>,
    },
    PhotoUpload {
        reply: oneshot::Sender<Result<String>>,
    },
    VoiceUpload {
        reply: oneshot::Sender<Result<VoiceTicket>>,
    },
    WaitVoice {
        audio_id: i64,
        reply: oneshot::Sender<()>,
    },
    ChatTitle {
        chat_id: i64,
        reply: oneshot::Sender<String>,
    },
    UserName {
        user_id: i64,
        reply: oneshot::Sender<String>,
    },
    ChatFace {
        chat_id: i64,
        reply: oneshot::Sender<ChatFace>,
    },
    ChatPhoto {
        chat_id: i64,
        reply: oneshot::Sender<std::result::Result<Option<String>, ()>>,
    },
    History {
        chat_id: i64,
        backward: i64,
        from_ms: i64,
        reply: oneshot::Sender<Result<Vec<Message>>>,
    },
    ChatIds {
        reply: oneshot::Sender<Vec<i64>>,
    },
    QuietDialogs {
        reply: oneshot::Sender<Result<Vec<i64>>>,
    },
    VideoPlay {
        chat_id: i64,
        message_id: i64,
        video_id: i64,
        reply: oneshot::Sender<Result<String>>,
    },
    AddContact {
        phone: String,
        first_name: String,
        last_name: String,
        reply: oneshot::Sender<Result<AddedContact>>,
    },
    MarkRead {
        chat_id: i64,
        message_id: i64,
        mark: i64,
        reply: oneshot::Sender<Result<()>>,
    },
    ChatPeerMarks {
        chat_id: i64,
        reply: oneshot::Sender<Result<HashMap<i64, i64>>>,
    },
}

pub struct Client {
    cmd_tx: mpsc::Sender<Cmd>,
    events: mpsc::Receiver<Event>,
    session: Session,
    voice_hold: Arc<AtomicBool>,
    voice_cids: Arc<Mutex<Vec<i64>>>,
    abort: tokio::task::AbortHandle,
}

impl Drop for Client {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

impl Client {
    pub async fn start(store: Arc<Store>) -> Result<Self> {
        let session = store.read_session()?.ok_or(Error::SessionNotFound)?;
        let saved = session.clone();
        let voice_hold = Arc::new(AtomicBool::new(false));
        let voice_cids = Arc::new(Mutex::new(Vec::new()));
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let (event_tx, events) = mpsc::channel(256);
        let hold = Arc::clone(&voice_hold);
        let cids = Arc::clone(&voice_cids);
        let task = tokio::spawn(async move {
            eprintln!("bridge: MAX client task started");
            if let Err(err) =
                run_forever(session, store, cmd_rx, event_tx.clone(), hold, cids).await
            {
                eprintln!("bridge: MAX client task stopped: {err}");
                let note = Event::Logged {
                    note: err.to_string(),
                };
                let _ = tokio::time::timeout(Duration::from_secs(2), event_tx.send(note)).await;
            }
        });
        let abort = task.abort_handle();
        Ok(Client {
            cmd_tx,
            events,
            session: saved,
            voice_hold,
            voice_cids,
            abort,
        })
    }

    pub async fn open_voice(&self) -> Result<VoiceLink> {
        VoiceLink::open(
            &self.session,
            Arc::clone(&self.voice_hold),
            Arc::clone(&self.voice_cids),
        )
        .await
    }

    pub async fn next_event(&mut self) -> Option<Event> {
        self.events.recv().await
    }

    pub fn abort_handle(&self) -> tokio::task::AbortHandle {
        self.abort.clone()
    }

    pub async fn send_text(
        &self,
        chat_id: i64,
        text: &str,
        reply_to: Option<i64>,
        attaches: Vec<Attach>,
    ) -> Result<Option<i64>> {
        let (reply, rx) = oneshot::channel();
        send_cmd(
            &self.cmd_tx,
            Cmd::SendText {
                chat_id,
                text: text.to_owned(),
                reply_to,
                attaches,
                reply,
            },
        )
        .await?;
        wait_cmd(rx).await?
    }

    pub async fn photo_upload_url(&self) -> Result<String> {
        let (reply, rx) = oneshot::channel();
        send_cmd(&self.cmd_tx, Cmd::PhotoUpload { reply }).await?;
        wait_cmd(rx).await?
    }

    pub async fn voice_upload(&self) -> Result<VoiceTicket> {
        let (reply, rx) = oneshot::channel();
        send_cmd(&self.cmd_tx, Cmd::VoiceUpload { reply }).await?;
        wait_cmd(rx).await?
    }

    pub async fn wait_voice(&self, audio_id: i64) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        send_cmd(&self.cmd_tx, Cmd::WaitVoice { audio_id, reply }).await?;
        match tokio::time::timeout(Duration::from_secs(12), rx).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(Error::Protocol("MAX connection stopped".into())),
            Err(_) => Err(Error::Protocol("voice is not ready".into())),
        }
    }

    pub async fn chat_title(&self, chat_id: i64) -> String {
        let (reply, rx) = oneshot::channel();
        if send_cmd(&self.cmd_tx, Cmd::ChatTitle { chat_id, reply })
            .await
            .is_err()
        {
            return String::new();
        }
        match tokio::time::timeout(Duration::from_secs(8), rx).await {
            Ok(Ok(title)) => title,
            _ => String::new(),
        }
    }

    pub async fn user_name(&self, user_id: i64) -> String {
        let (reply, rx) = oneshot::channel();
        if send_cmd(&self.cmd_tx, Cmd::UserName { user_id, reply })
            .await
            .is_err()
        {
            return String::new();
        }
        match tokio::time::timeout(Duration::from_secs(8), rx).await {
            Ok(Ok(name)) => name,
            _ => String::new(),
        }
    }

    pub async fn chat_face(&self, chat_id: i64) -> ChatFace {
        let (reply, rx) = oneshot::channel();
        if send_cmd(&self.cmd_tx, Cmd::ChatFace { chat_id, reply })
            .await
            .is_err()
        {
            return ChatFace::default();
        }
        match tokio::time::timeout(Duration::from_secs(8), rx).await {
            Ok(Ok(face)) => face,
            _ => ChatFace::default(),
        }
    }

    pub async fn chat_photo(&self, chat_id: i64) -> std::result::Result<Option<String>, ()> {
        let (reply, rx) = oneshot::channel();
        if send_cmd(&self.cmd_tx, Cmd::ChatPhoto { chat_id, reply })
            .await
            .is_err()
        {
            return Err(());
        }
        match tokio::time::timeout(CMD_WAIT, rx).await {
            Ok(Ok(photo)) => photo,
            _ => Err(()),
        }
    }

    pub async fn fetch_history(
        &self,
        chat_id: i64,
        backward: i64,
        from_ms: i64,
    ) -> Result<Vec<Message>> {
        let (reply, rx) = oneshot::channel();
        send_cmd(
            &self.cmd_tx,
            Cmd::History {
                chat_id,
                backward,
                from_ms,
                reply,
            },
        )
        .await?;
        wait_cmd(rx).await?
    }

    pub async fn chat_ids(&self) -> Result<Vec<i64>> {
        let (reply, rx) = oneshot::channel();
        send_cmd(&self.cmd_tx, Cmd::ChatIds { reply }).await?;
        wait_cmd(rx).await
    }

    pub async fn quiet_dialog_ids(&self) -> Result<Vec<i64>> {
        let (reply, rx) = oneshot::channel();
        send_cmd(&self.cmd_tx, Cmd::QuietDialogs { reply }).await?;
        wait_cmd(rx).await?
    }

    pub async fn video_play_url(
        &self,
        chat_id: i64,
        message_id: i64,
        video_id: i64,
    ) -> Result<String> {
        let (reply, rx) = oneshot::channel();
        send_cmd(
            &self.cmd_tx,
            Cmd::VideoPlay {
                chat_id,
                message_id,
                video_id,
                reply,
            },
        )
        .await?;
        wait_cmd(rx).await?
    }

    pub async fn mark_read(&self, chat_id: i64, message_id: i64, mark: i64) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        send_cmd(
            &self.cmd_tx,
            Cmd::MarkRead {
                chat_id,
                message_id,
                mark,
                reply,
            },
        )
        .await?;
        wait_cmd(rx).await?
    }

    pub async fn chat_peer_marks(&self, chat_id: i64) -> Result<HashMap<i64, i64>> {
        let (reply, rx) = oneshot::channel();
        send_cmd(&self.cmd_tx, Cmd::ChatPeerMarks { chat_id, reply }).await?;
        wait_cmd(rx).await?
    }
}

#[derive(Clone)]
pub struct SessionLink {
    cmd_tx: mpsc::Sender<Cmd>,
}

impl Client {
    pub fn link(&self) -> SessionLink {
        SessionLink {
            cmd_tx: self.cmd_tx.clone(),
        }
    }
}

impl SessionLink {
    pub async fn add_contact(&self, phone: &str, name: &str) -> Result<AddedContact> {
        let (first_name, last_name) = split_contact_name(name);
        let (reply, rx) = oneshot::channel();
        send_cmd(
            &self.cmd_tx,
            Cmd::AddContact {
                phone: phone.to_owned(),
                first_name,
                last_name,
                reply,
            },
        )
        .await?;
        wait_cmd(rx).await?
    }
}

async fn send_cmd(tx: &mpsc::Sender<Cmd>, cmd: Cmd) -> Result<()> {
    max_beat();
    let result = match tokio::time::timeout(CMD_WAIT, tx.send(cmd)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(Error::Protocol("MAX connection stopped".into())),
        Err(_) => Err(Error::Protocol("MAX request timed out".into())),
    };
    max_beat();
    result
}

async fn wait_cmd<T>(rx: oneshot::Receiver<T>) -> Result<T> {
    max_beat();
    let result = match tokio::time::timeout(CMD_WAIT, rx).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(_)) => Err(Error::Protocol("MAX connection stopped".into())),
        Err(_) => Err(Error::Protocol("MAX request timed out".into())),
    };
    max_beat();
    result
}

pub struct PendingAuth {
    write: WriteHalf<tokio_rustls::client::TlsStream<TcpStream>>,
    read: ReadHalf<tokio_rustls::client::TlsStream<TcpStream>>,
    buf: Vec<u8>,
    seq: i64,
    device_id: String,
    instance_id: String,
    calls_seed: i64,
}

impl PendingAuth {
    pub async fn connect(device_id: &str) -> Result<Self> {
        let addr = lookup_v4(AUTH_HOST, 443).await?;
        let tcp = TcpStream::connect(addr).await?;
        tcp.set_nodelay(true)?;
        let connector = TlsConnector::from(Arc::new(tls_config()));
        let name = ServerName::try_from(AUTH_HOST)
            .map_err(|err| Error::Protocol(err.to_string()))?
            .to_owned();
        let tls = connector
            .connect(name, tcp)
            .await
            .map_err(|err| Error::Protocol(err.to_string()))?;
        let (read, write) = tokio::io::split(tls);
        let instance_id = instance_id();
        let mut auth = Self {
            write,
            read,
            buf: Vec::new(),
            seq: 0,
            device_id: device_id.to_owned(),
            instance_id: instance_id.clone(),
            calls_seed: 0,
        };
        let hello = auth
            .call(
                SESSION_INIT,
                mobile_handshake_payload(device_id, &instance_id, session_id(device_id)),
            )
            .await?;
        auth.calls_seed = json_i64(hello.get("callsSeed"))
            .ok_or_else(|| Error::Protocol("handshake did not return a calls seed".into()))?;
        Ok(auth)
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub async fn finish_login(&mut self, token: &str) -> Result<String> {
        let payload = self
            .call(LOGIN, login_payload(token, -1, -1, -1, -1, false))
            .await?;
        Ok(payload
            .get("token")
            .and_then(Value::as_str)
            .filter(|token| !token.is_empty())
            .unwrap_or(token)
            .to_owned())
    }

    pub async fn ping(&mut self) -> Result<()> {
        self.write_opcode(PING, json!({})).await
    }

    pub async fn request_code(&mut self, phone: &str) -> Result<CodeChallenge> {
        let mode = auth_mode(&self.device_id, self.calls_seed);
        let payload = self
            .call(AUTH_REQUEST, auth_request_payload(phone, &mode))
            .await?;
        parse_code_challenge(&payload)
    }

    pub async fn submit_code(&mut self, token: &str, code: &str) -> Result<CodeStatus> {
        let payload = self.call(AUTH, auth_code_payload(token, code)).await?;
        parse_code_status(&payload)
    }

    pub async fn submit_password(&mut self, track_id: &str, password: &str) -> Result<String> {
        let payload = self
            .call(
                AUTH_LOGIN_CHECK_PASSWORD,
                auth_password_payload(track_id, password),
            )
            .await?;
        parse_login_token(&payload)
    }

    async fn call(&mut self, opcode: i64, payload: Value) -> Result<Value> {
        self.write_opcode(opcode, payload).await?;
        let seq = self.seq;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(Error::Protocol("MAX request timed out".into()));
            }
            let frame = tokio::time::timeout(remaining, self.read_frame())
                .await
                .map_err(|_| Error::Protocol("MAX request timed out".into()))??;
            if frame.seq == seq && frame.cmd != CMD_EVENT {
                if frame.cmd == CMD_ERROR {
                    let err = payload_error(&frame.payload)
                        .unwrap_or_else(|| "MAX rejected the request".into());
                    return Err(Error::Protocol(err));
                }
                if let Some(err) = payload_error(&frame.payload) {
                    return Err(Error::Protocol(err));
                }
                return Ok(frame.payload);
            }
        }
    }

    async fn write_opcode(&mut self, opcode: i64, payload: Value) -> Result<()> {
        self.seq += 1;
        let raw = encode_bin(&request(self.seq, opcode, payload))?;
        self.write
            .write_all(&raw)
            .await
            .map_err(|err| Error::Protocol(err.to_string()))?;
        Ok(())
    }

    async fn read_frame(&mut self) -> Result<crate::frame::Frame> {
        read_tcp_frame(&mut self.read, &mut self.buf).await
    }
}

async fn read_tcp_frame(
    read: &mut (impl AsyncReadExt + Unpin),
    buf: &mut Vec<u8>,
) -> Result<crate::frame::Frame> {
    loop {
        if let Some(len) = tcp_frame_len(buf) {
            let raw: Vec<u8> = buf.drain(..len).collect();
            return decode_bin(&raw);
        }
        let mut tmp = [0u8; 4096];
        let read = read
            .read(&mut tmp)
            .await
            .map_err(|err| Error::Protocol(err.to_string()))?;
        if read == 0 {
            return Err(Error::Protocol("MAX socket closed".into()));
        }
        buf.extend_from_slice(&tmp[..read]);
    }
}

fn tcp_frame_len(buf: &[u8]) -> Option<usize> {
    if buf.len() < 10 {
        return None;
    }
    let len = ((buf[7] as usize) << 16) | ((buf[8] as usize) << 8) | (buf[9] as usize);
    let total = 10 + len;
    if buf.len() < total {
        None
    } else {
        Some(total)
    }
}

fn instance_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0);
    format!("{:016x}", nanos ^ 0xA5A5_A5A5_A5A5_A5A5)
}

fn session_id(device_id: &str) -> i64 {
    let mut mixed = 0i64;
    for byte in device_id.bytes() {
        mixed = mixed.wrapping_mul(31).wrapping_add(i64::from(byte));
    }
    (mixed.unsigned_abs() % 70) as i64 + 1
}

async fn run_forever(
    mut session: Session,
    store: Arc<Store>,
    mut cmd_rx: mpsc::Receiver<Cmd>,
    event_tx: mpsc::Sender<Event>,
    voice_hold: Arc<AtomicBool>,
    voice_cids: Arc<Mutex<Vec<i64>>>,
) -> Result<()> {
    let mut attempt = 0u32;
    let mut queued = VecDeque::new();
    loop {
        max_beat();
        match run_once(
            &mut session,
            &store,
            &mut cmd_rx,
            &event_tx,
            &voice_cids,
            &mut queued,
        )
        .await
        {
            Ok(()) => attempt = 0,
            Err(err) => {
                if credential_rejected(&err) {
                    let _ = store.clear_session();
                    return Err(Error::Protocol(
                        "saved MAX session was rejected. Run the program again and sign in".into(),
                    ));
                }
                while voice_hold.load(Ordering::SeqCst) {
                    max_beat();
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                attempt = attempt.saturating_add(1);
                let delay = Duration::from_secs(u64::from(attempt.min(5)));
                push_event(
                    &event_tx,
                    &mut queued,
                    Event::Logged {
                        note: format!("{err}; reconnecting"),
                    },
                );
                max_beat();
                tokio::time::sleep(delay).await;
            }
        }
        if cmd_rx.is_closed() {
            return Ok(());
        }
    }
}

fn credential_rejected(err: &Error) -> bool {
    let text = err.to_string();
    text.contains("FAIL_WRONG_PASSWORD")
        || text.contains("FAIL_LOGIN_TOKEN")
        || text.contains("login.cred")
        || text.contains("login.token")
}

struct Link {
    write: WriteHalf<tokio_rustls::client::TlsStream<TcpStream>>,
    read: ReadHalf<tokio_rustls::client::TlsStream<TcpStream>>,
    buf: Vec<u8>,
    seq: i64,
}

enum Waiter {
    Ignore,
    Done(oneshot::Sender<Result<Option<i64>>>),
    History(oneshot::Sender<Result<Vec<Message>>>),
    Title {
        reply: oneshot::Sender<String>,
    },
    Name {
        user_id: i64,
        reply: oneshot::Sender<String>,
    },
    Face {
        chat_id: i64,
        me: i64,
        reply: oneshot::Sender<ChatFace>,
    },
}

async fn run_once(
    session: &mut Session,
    store: &Store,
    cmd_rx: &mut mpsc::Receiver<Cmd>,
    event_tx: &mpsc::Sender<Event>,
    voice_cids: &Mutex<Vec<i64>>,
    queued: &mut VecDeque<Event>,
) -> Result<()> {
    let mut link = connect().await?;
    let mut pending = HashMap::<i64, Waiter>::new();
    let mut voices = HashMap::<i64, VoiceSlot>::new();
    if session.instance_id.is_empty() {
        session.instance_id = instance_id();
    }
    let hello = roundtrip(
        &mut link,
        &mut pending,
        &mut voices,
        SESSION_INIT,
        mobile_handshake_payload(
            &session.device_id,
            &session.instance_id,
            session_id(&session.device_id),
        ),
        event_tx,
    )
    .await?;
    if let Some(err) = payload_error(&hello) {
        return Err(Error::Protocol(err));
    }
    let logged = roundtrip(
        &mut link,
        &mut pending,
        &mut voices,
        LOGIN,
        login_payload(&session.token, -1, -1, -1, -1, session.desktop),
        event_tx,
    )
    .await?;
    if let Some(err) = payload_error(&logged) {
        return Err(Error::Protocol(err));
    }
    let me =
        me_id(&logged).ok_or_else(|| Error::Protocol("login did not return a profile".into()))?;
    if let Some(token) = logged.get("token").and_then(Value::as_str) {
        if !token.is_empty() {
            session.token = token.to_owned();
        }
    }
    if let Some(time) = sync_time(&logged) {
        session.chats_sync = time;
        session.contacts_sync = time;
        session.drafts_sync = time;
        session.presence_sync = time;
    }
    let _ = store.write_session(session);

    let mut chats: HashMap<i64, Chat> = parse_chats(&logged)
        .into_iter()
        .map(|chat| (chat.id, chat))
        .collect();
    let mut users = HashMap::new();
    apply_name_cache(&mut users, &store.load_names().unwrap_or_default());
    let mut contact_ids = store.load_contacts().unwrap_or_default();
    for user in parse_book(&logged) {
        contact_ids.insert(user.id);
        remember_user(&mut users, user);
    }
    let _ = store.save_contacts(&contact_ids);
    let _ = store.merge_names_from_users(&users);
    let raw_contacts = match logged.get("contacts") {
        None => "missing".to_owned(),
        Some(value) => value_brief(value),
    };
    let chat_marker = json_i64(logged.get("chatMarker")).or_else(|| json_i64(logged.get("marker")));
    let login_shape = logged
        .as_object()
        .map(|obj| {
            let mut keys: Vec<_> = obj
                .iter()
                .map(|(key, value)| format!("{key}:{}", value_brief(value)))
                .collect();
            keys.sort();
            keys.join(" ")
        })
        .unwrap_or_else(|| "none".into());
    push_event(
        event_tx,
        queued,
        Event::Logged {
            note: format!(
                "login chats {} contacts {} raw {} marker {} {login_shape}",
                chats.len(),
                contact_ids.len(),
                raw_contacts,
                chat_marker
                    .map(|marker| marker.to_string())
                    .unwrap_or_else(|| "none".into())
            ),
        },
    );
    if let Ok(fresh) = roundtrip(
        &mut link,
        &mut pending,
        &mut voices,
        CHATS_LIST,
        json!({"marker": now_ms()}),
        event_tx,
    )
    .await
    {
        for chat in parse_chats(&fresh) {
            remember_chat(&mut chats, chat);
        }
    }

    push_event(event_tx, queued, Event::Ready { me });
    push_event(
        event_tx,
        queued,
        Event::Logged {
            note: "logged in".into(),
        },
    );

    let cid = Arc::new(AtomicI64::new(now_ms()));
    let mut sent_cids = VecDeque::new();
    let mut ping = tokio::time::interval(Duration::from_secs(25));
    ping.tick().await;
    let mut last_rx = tokio::time::Instant::now();

    loop {
        max_beat();
        flush_events(event_tx, queued);
        if let Ok(mut extra) = voice_cids.lock() {
            for cid in extra.drain(..) {
                remember_cid(&mut sent_cids, cid);
            }
        }
        tokio::select! {
            incoming = read_tcp_frame(&mut link.read, &mut link.buf) => {
                last_rx = tokio::time::Instant::now();
                let frame = match incoming {
                    Ok(frame) => frame,
                    Err(err) => {
                        fail_pending(&mut pending, "MAX socket closed");
                        return Err(err);
                    }
                };
                if frame.opcode == NOTIF_ATTACH {
                    note_voice(&mut voices, &frame.payload);
                }
                // Push events reuse seq numbers. Only a response may finish a request.
                if frame.cmd != CMD_EVENT {
                    if let Some(waiter) = pending.remove(&frame.seq) {
                        finish_waiter(
                            waiter,
                            &frame.cmd,
                            &frame.payload,
                            &mut chats,
                            &mut users,
                            store,
                        );
                        continue;
                    }
                }
                if frame.opcode == NOTIF_MESSAGE {
                    if let Some(message) = parse_message(&frame.payload) {
                        if let Some(chat_id) = message.chat_id {
                            let _ = send_frame(&mut link, &mut pending, NOTIF_MESSAGE, ack_payload(chat_id, message.id), Waiter::Ignore).await;
                        }
                        if message.status.as_deref() == Some("REMOVED")
                            || message.status.as_deref() == Some("EDITED")
                        {
                            continue;
                        }
                        absorb_voice_cids(voice_cids, &mut sent_cids);
                        if consume_echo(&mut sent_cids, message.cid) {
                            continue;
                        }
                        let _ = event_tx.try_send(Event::Message(message));
                    }
                } else if frame.opcode == NOTIF_MARK {
                    if let Some(mark) = parse_read_mark(&frame.payload) {
                        let _ = event_tx.try_send(Event::Read(mark));
                    }
                } else if frame.opcode == MSG_DELIVERY {
                    if let Some(mark) = parse_delivery_mark(&frame.payload) {
                        let _ = event_tx.try_send(Event::Delivered(mark));
                    }
                }
            }
            cmd = cmd_rx.recv() => {
                let Some(cmd) = cmd else {
                    return Ok(());
                };
                match cmd {
                    Cmd::SendText {
                        chat_id,
                        text,
                        reply_to,
                        attaches,
                        reply,
                    } => {
                        let next = cid.fetch_add(1, Ordering::Relaxed);
                        remember_cid(&mut sent_cids, next);
                        if let Err(err) = send_frame(
                            &mut link,
                            &mut pending,
                            MSG_SEND,
                            send_payload(chat_id, &text, next, reply_to, &attaches),
                            Waiter::Done(reply),
                        )
                        .await
                        {
                            let _ = err;
                        }
                    }
                    Cmd::ChatTitle { chat_id, reply } => {
                        if let Some(title) = cached_title(&chats, chat_id) {
                            let _ = reply.send(title);
                        } else {
                            let _ = send_frame(
                                &mut link,
                                &mut pending,
                                CHAT_INFO,
                                json!({"chatIds": [chat_id]}),
                                Waiter::Title { reply },
                            )
                            .await;
                        }
                    }
                    Cmd::UserName { user_id, reply } => {
                        if let Some(name) = cached_name(&users, user_id) {
                            let _ = reply.send(name);
                        } else {
                            let _ = send_frame(
                                &mut link,
                                &mut pending,
                                CONTACT_INFO,
                                json!({"contactIds": [user_id]}),
                                Waiter::Name { user_id, reply },
                            )
                            .await;
                        }
                    }
                    Cmd::ChatPhoto { chat_id, reply } => {
                        let photo = lookup_chat_photo(
                            &mut link,
                            &mut pending,
                            &mut voices,
                            event_tx,
                            &mut chats,
                            &mut users,
                            store,
                            me,
                            chat_id,
                        )
                        .await;
                        let _ = reply.send(photo);
                    }
                    Cmd::ChatFace { chat_id, reply } => {
                        if chats.contains_key(&chat_id) || cached_contact(&users, me, chat_id).is_some()
                        {
                            let _ = reply.send(chat_face(&chats, &users, me, chat_id));
                        } else {
                            let _ = send_frame(
                                &mut link,
                                &mut pending,
                                CHAT_INFO,
                                json!({"chatIds": [chat_id]}),
                                Waiter::Face {
                                    chat_id,
                                    me,
                                    reply,
                                },
                            )
                            .await;
                        }
                    }
                    Cmd::PhotoUpload { reply } => {
                        let result = match roundtrip(
                            &mut link,
                            &mut pending,
                            &mut voices,
                            PHOTO_UPLOAD,
                            json!({"count": 1, "profile": false}),
                            event_tx,
                        )
                        .await
                        {
                            Ok(payload) => payload
                                .get("url")
                                .and_then(Value::as_str)
                                .filter(|url| !url.is_empty())
                                .map(str::to_owned)
                                .ok_or_else(|| {
                                    Error::Protocol("MAX photo upload did not return a url".into())
                                }),
                            Err(err) => Err(err),
                        };
                        let _ = reply.send(result);
                    }
                    Cmd::VoiceUpload { reply } => {
                        let result = match roundtrip(
                            &mut link,
                            &mut pending,
                            &mut voices,
                            VIDEO_UPLOAD,
                            json!({"count": 1, "type": 2, "uploaderType": 1}),
                            event_tx,
                        )
                        .await
                        {
                            Ok(payload) => {
                                let ticket = voice_ticket(&payload);
                                if let Ok(ticket) = &ticket {
                                    arm_voice(&mut voices, ticket.audio_id);
                                }
                                ticket
                            }
                            Err(err) => Err(err),
                        };
                        let _ = reply.send(result);
                    }
                    Cmd::WaitVoice { audio_id, reply } => {
                        if matches!(voices.remove(&audio_id), Some(VoiceSlot::Ready)) {
                            let _ = reply.send(());
                        } else {
                            voices.insert(audio_id, VoiceSlot::Waiting(reply));
                        }
                    }
                    Cmd::History {
                        chat_id,
                        backward,
                        from_ms,
                        reply,
                    } => {
                        let _ = send_frame(
                            &mut link,
                            &mut pending,
                            CHAT_HISTORY,
                            history_payload(chat_id, backward, from_ms),
                            Waiter::History(reply),
                        )
                        .await;
                    }
                    Cmd::ChatIds { reply } => {
                        fill_chats(&mut link, &mut pending, &mut voices, &mut chats, event_tx).await;
                        let _ = reply.send(known_chats(&chats));
                    }
                    Cmd::QuietDialogs { reply } => {
                        let ids: Vec<i64> = contact_ids.iter().copied().collect();
                        let _ = reply.send(Ok(unstarted_dialogs(&chats, &ids, me)));
                    }
                    Cmd::VideoPlay {
                        chat_id,
                        message_id,
                        video_id,
                        reply,
                    } => {
                        let result = match roundtrip(
                            &mut link,
                            &mut pending,
                            &mut voices,
                            VIDEO_PLAY,
                            json!({
                                "chatId": chat_id,
                                "messageId": message_id,
                                "videoId": video_id,
                            }),
                            event_tx,
                        )
                        .await
                        {
                            Ok(payload) => video_play_url(&payload).ok_or_else(|| {
                                Error::Protocol("MAX video play did not return a url".into())
                            }),
                            Err(err) => Err(err),
                        };
                        let _ = reply.send(result);
                    }
                    Cmd::AddContact {
                        phone,
                        first_name,
                        last_name,
                        reply,
                    } => {
                        let result = match roundtrip(
                            &mut link,
                            &mut pending,
                            &mut voices,
                            CONTACT_ADD_BY_PHONE,
                            add_contact_payload(&phone, &first_name, &last_name),
                            event_tx,
                        )
                        .await
                        {
                            Ok(payload) => match parse_added_contact(&payload) {
                                Ok(added) => {
                                    contact_ids.insert(added.id);
                                    let _ = store.save_contacts(&contact_ids);
                                    let named = noted_contact(&added, &first_name, &last_name);
                                    let name = named
                                        .as_ref()
                                        .map(|user| user.display_name())
                                        .unwrap_or_default();
                                    if let Some(user) = named {
                                        remember_user(&mut users, user);
                                        let _ = store.merge_names_from_users(&users);
                                    }
                                    let dialog = dialog_id(me, added.id);
                                    if dialog != 0 && !name.is_empty() {
                                        push_event(
                                            event_tx,
                                            queued,
                                            Event::Contact { dialog, name },
                                        );
                                    }
                                    Ok(added)
                                }
                                Err(err) => Err(err),
                            },
                            Err(err) => Err(err),
                        };
                        let _ = reply.send(result);
                    }
                    Cmd::MarkRead {
                        chat_id,
                        message_id,
                        mark,
                        reply,
                    } => {
                        let result = roundtrip(
                            &mut link,
                            &mut pending,
                            &mut voices,
                            CHAT_MARK,
                            mark_read_payload(chat_id, message_id, mark),
                            event_tx,
                        )
                        .await
                        .map(|_| ());
                        let _ = reply.send(result);
                    }
                    Cmd::ChatPeerMarks { chat_id, reply } => {
                        let result = match roundtrip(
                            &mut link,
                            &mut pending,
                            &mut voices,
                            CHAT_INFO,
                            json!({"chatIds": [chat_id]}),
                            event_tx,
                        )
                        .await
                        {
                            Ok(payload) => {
                                for chat in parse_chats(&payload) {
                                    remember_chat(&mut chats, chat);
                                }
                                Ok(cached_peer_marks(&chats, chat_id))
                            }
                            Err(err) => Err(err),
                        };
                        let _ = reply.send(result);
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(200)), if !queued.is_empty() => {
                flush_events(event_tx, queued);
            }
            _ = tokio::time::sleep_until(last_rx + QUIET_FOR) => {
                fail_pending(&mut pending, "MAX connection is quiet");
                return Err(Error::Protocol("MAX connection is quiet".into()));
            }
            _ = ping.tick() => {
                if send_frame(&mut link, &mut pending, PING, json!({}), Waiter::Ignore).await.is_err() {
                    fail_pending(&mut pending, "MAX ping failed");
                    return Err(Error::Protocol("MAX ping failed".into()));
                }
            }
        }
    }
}

async fn fill_chats(
    link: &mut Link,
    pending: &mut HashMap<i64, Waiter>,
    voices: &mut HashMap<i64, VoiceSlot>,
    chats: &mut HashMap<i64, Chat>,
    event_tx: &mpsc::Sender<Event>,
) {
    let mut marker = now_ms();
    let mut seen_pages = HashSet::new();
    for _ in 0..50 {
        max_beat();
        let payload = match roundtrip(
            link,
            pending,
            voices,
            CHATS_LIST,
            json!({"marker": marker}),
            event_tx,
        )
        .await
        {
            Ok(payload) => payload,
            Err(_) => break,
        };
        let page = parse_chats(&payload);
        let ids: Vec<i64> = page.iter().map(|chat| chat.id).collect();
        for chat in page {
            remember_chat(chats, chat);
        }
        let Some(next) = next_chat_marker(&payload, marker, &ids, &mut seen_pages) else {
            break;
        };
        marker = next;
    }
}

fn next_chat_marker(
    payload: &Value,
    marker: i64,
    page_ids: &[i64],
    seen_pages: &mut HashSet<String>,
) -> Option<i64> {
    if page_ids.is_empty() {
        return None;
    }
    let mut ids = page_ids.to_vec();
    ids.sort_unstable();
    let fingerprint = ids
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    if !seen_pages.insert(fingerprint) {
        return None;
    }
    let next = json_i64(payload.get("chatMarker")).or_else(|| json_i64(payload.get("marker")))?;
    if next == marker {
        None
    } else {
        Some(next)
    }
}

fn value_brief(value: &Value) -> String {
    match value {
        Value::Array(items) => format!("array {}", items.len()),
        Value::Object(map) => format!("object {}", map.len()),
        Value::Null => "null".into(),
        Value::Bool(flag) => format!("bool {flag}"),
        Value::Number(_) => "number".into(),
        Value::String(text) => format!("string {}", text.len()),
    }
}

fn dialog_id(me: i64, contact: i64) -> i64 {
    // A personal dialog keeps this id before the first message.
    me ^ contact
}

fn unstarted_dialogs(chats: &HashMap<i64, Chat>, contacts: &[i64], me: i64) -> Vec<i64> {
    let mut peers = HashSet::new();
    for chat in chats.values() {
        if chat.id == 0 || is_group(chat) {
            continue;
        }
        for peer in &chat.peers {
            if *peer != me {
                peers.insert(*peer);
            }
        }
    }
    let mut ids = Vec::new();
    for contact in contacts {
        if *contact == me || *contact == 0 || peers.contains(contact) {
            continue;
        }
        let dialog = dialog_id(me, *contact);
        if dialog == 0 || chats.contains_key(&dialog) || ids.contains(&dialog) {
            continue;
        }
        ids.push(dialog);
    }
    ids.sort_unstable();
    ids
}

fn cached_contact<'a>(users: &'a HashMap<i64, User>, me: i64, chat_id: i64) -> Option<&'a User> {
    users
        .get(&dialog_id(me, chat_id))
        .or_else(|| users.get(&chat_id))
}

fn known_chats(chats: &HashMap<i64, Chat>) -> Vec<i64> {
    let mut ids: Vec<i64> = chats.keys().copied().collect();
    if !ids.contains(&0) {
        ids.push(0);
    }
    ids.sort_unstable();
    ids
}

fn remember_cid(sent: &mut VecDeque<i64>, cid: i64) {
    if sent.len() >= 64 {
        sent.pop_front();
    }
    sent.push_back(cid);
}

fn absorb_voice_cids(voice_cids: &Mutex<Vec<i64>>, sent_cids: &mut VecDeque<i64>) {
    if let Ok(mut extra) = voice_cids.lock() {
        for cid in extra.drain(..) {
            remember_cid(sent_cids, cid);
        }
    }
}

fn consume_echo(sent: &mut VecDeque<i64>, cid: Option<i64>) -> bool {
    let Some(cid) = cid else {
        return false;
    };
    let Some(index) = sent.iter().position(|item| *item == cid) else {
        return false;
    };
    sent.remove(index);
    true
}

fn video_play_url(payload: &Value) -> Option<String> {
    let obj = payload.as_object()?;
    let mut ranked: Vec<(&str, &str)> = Vec::new();
    for (key, value) in obj {
        if key == "EXTERNAL" || key == "cache" {
            continue;
        }
        let Some(url) = value.as_str().map(str::trim).filter(|url| !url.is_empty()) else {
            continue;
        };
        ranked.push((key.as_str(), url));
    }
    ranked.sort_by_key(|(key, _)| {
        if key.starts_with("MP4_") {
            0
        } else if key.eq_ignore_ascii_case("url") {
            1
        } else {
            2
        }
    });
    ranked.first().map(|(_, url)| (*url).to_owned())
}

fn voice_ticket(payload: &Value) -> Result<VoiceTicket> {
    let info = payload
        .get("info")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .and_then(Value::as_object)
        .ok_or_else(|| Error::Protocol("MAX voice upload did not return a url".into()))?;
    let url = info
        .get("url")
        .and_then(Value::as_str)
        .filter(|url| !url.is_empty())
        .ok_or_else(|| Error::Protocol("MAX voice upload did not return a url".into()))?;
    let audio_id = crate::frame::json_i64(info.get("videoId"))
        .ok_or_else(|| Error::Protocol("MAX voice upload did not return an id".into()))?;
    let token = info
        .get("token")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .ok_or_else(|| Error::Protocol("MAX voice upload did not return a token".into()))?;
    Ok(VoiceTicket {
        url: url.to_owned(),
        audio_id,
        token: token.to_owned(),
    })
}

fn cached_peer_marks(chats: &HashMap<i64, Chat>, chat_id: i64) -> HashMap<i64, i64> {
    chats
        .get(&chat_id)
        .map(|chat| chat.peer_marks.clone())
        .unwrap_or_default()
}

fn cached_title(chats: &HashMap<i64, Chat>, chat_id: i64) -> Option<String> {
    let title = chats.get(&chat_id)?.title.clone()?;
    if title.is_empty() {
        None
    } else {
        Some(title)
    }
}

fn cached_name(users: &HashMap<i64, User>, user_id: i64) -> Option<String> {
    let name = users.get(&user_id)?.display_name();
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

fn apply_name_cache(users: &mut HashMap<i64, User>, cached: &HashMap<i64, String>) {
    for (id, name) in cached {
        if name.is_empty() || cached_name(users, *id).is_some() {
            continue;
        }
        users.insert(*id, remembered_user(*id, name));
    }
}

fn remembered_user(id: i64, name: &str) -> User {
    User {
        id,
        names: vec![Name {
            name: Some(name.to_owned()),
            first_name: None,
            last_name: None,
            kind: "CUSTOM".into(),
        }],
        photo: None,
    }
}

fn name_text(item: &Name) -> String {
    if let Some(name) = item.name.as_deref() {
        if !name.is_empty() {
            return name.to_owned();
        }
    }
    let first = item.first_name.as_deref().unwrap_or("");
    let last = item.last_name.as_deref().unwrap_or("");
    format!("{first} {last}").trim().to_owned()
}

fn noted_contact(added: &AddedContact, first_name: &str, last_name: &str) -> Option<User> {
    let mut user = added.user.clone().unwrap_or(User {
        id: added.id,
        names: Vec::new(),
        photo: None,
    });
    if user.display_name().is_empty() {
        let name = format!("{first_name} {last_name}").trim().to_owned();
        if name.is_empty() {
            return None;
        }
        user.names.push(Name {
            name: Some(name),
            first_name: (!first_name.is_empty()).then(|| first_name.to_owned()),
            last_name: (!last_name.is_empty()).then(|| last_name.to_owned()),
            kind: "CUSTOM".into(),
        });
    }
    Some(user)
}

fn remember_user(users: &mut HashMap<i64, User>, mut user: User) {
    user.photo = clean_photo(user.photo);
    if user.display_name().is_empty() {
        if let Some(existing) = users.get_mut(&user.id) {
            if user.photo.is_some() {
                existing.photo = user.photo;
            }
        }
        return;
    }
    let has_custom = user
        .names
        .iter()
        .any(|item| item.kind == "CUSTOM" && !name_text(item).is_empty());
    let keep_cached = users.get(&user.id).is_some_and(|existing| {
        !has_custom && existing.names.iter().any(|item| item.kind == "CUSTOM")
    });
    if keep_cached {
        if let Some(existing) = users.get_mut(&user.id) {
            if user.photo.is_some() {
                existing.photo = user.photo;
            }
        }
        return;
    }
    if user.photo.is_none() {
        if let Some(existing) = users.get(&user.id) {
            user.photo = existing.photo.clone();
        }
    }
    users.insert(user.id, user);
}

fn remember_chat(chats: &mut HashMap<i64, Chat>, mut chat: Chat) {
    chat.photo = clean_photo(chat.photo);
    if chat.photo.is_none() {
        if let Some(existing) = chats.get(&chat.id) {
            chat.photo = existing.photo.clone();
        }
    }
    chats.insert(chat.id, chat);
}

fn clean_photo(photo: Option<String>) -> Option<String> {
    photo
        .map(|url| url.trim().to_owned())
        .filter(|url| !url.is_empty())
}

async fn lookup_chat_photo(
    link: &mut Link,
    pending: &mut HashMap<i64, Waiter>,
    voices: &mut HashMap<i64, VoiceSlot>,
    event_tx: &mpsc::Sender<Event>,
    chats: &mut HashMap<i64, Chat>,
    users: &mut HashMap<i64, User>,
    store: &Store,
    me: i64,
    chat_id: i64,
) -> std::result::Result<Option<String>, ()> {
    if chat_id == 0 {
        return Ok(None);
    }
    if !chats.contains_key(&chat_id) {
        if let Some(user) = cached_contact(users, me, chat_id) {
            return Ok(clean_photo(user.photo.clone()));
        }
    }
    if !chats.contains_key(&chat_id) {
        let payload = roundtrip(
            link,
            pending,
            voices,
            CHAT_INFO,
            json!({"chatIds": [chat_id]}),
            event_tx,
        )
        .await
        .map_err(|_| ())?;
        for chat in parse_chats(&payload) {
            remember_chat(chats, chat);
        }
    }
    let Some(chat) = chats.get(&chat_id).cloned() else {
        return Ok(None);
    };
    if is_group(&chat) {
        return Ok(stored_chat_photo(&chat, users, me));
    }
    let Some(peer) = chat.peers.iter().copied().find(|id| *id != me) else {
        return Ok(None);
    };
    if users
        .get(&peer)
        .and_then(|user| clean_photo(user.photo.clone()))
        .is_some()
    {
        return Ok(stored_chat_photo(&chat, users, me));
    }
    let payload = roundtrip(
        link,
        pending,
        voices,
        CONTACT_INFO,
        json!({"contactIds": [peer]}),
        event_tx,
    )
    .await
    .map_err(|_| ())?;
    for user in parse_users(&payload) {
        remember_user(users, user);
    }
    let _ = store.merge_names_from_users(users);
    Ok(stored_chat_photo(&chat, users, me))
}

fn stored_chat_photo(chat: &Chat, users: &HashMap<i64, User>, me: i64) -> Option<String> {
    if chat.id == 0 {
        return None;
    }
    if is_group(chat) {
        return clean_photo(chat.photo.clone());
    }
    let peer = chat.peers.iter().copied().find(|id| *id != me)?;
    users
        .get(&peer)
        .and_then(|user| clean_photo(user.photo.clone()))
}

fn chat_face(
    chats: &HashMap<i64, Chat>,
    users: &HashMap<i64, User>,
    me: i64,
    chat_id: i64,
) -> ChatFace {
    let Some(chat) = chats.get(&chat_id) else {
        let label = cached_contact(users, me, chat_id)
            .map(|user| user.display_name())
            .filter(|name| !name.is_empty())
            .unwrap_or_default();
        return ChatFace {
            label,
            group: false,
        };
    };
    if is_group(chat) {
        return ChatFace {
            label: chat.title.clone().unwrap_or_default(),
            group: true,
        };
    }
    let label = chat
        .peers
        .iter()
        .copied()
        .find(|id| *id != me)
        .and_then(|id| cached_name(users, id))
        .or_else(|| chat.title.clone().filter(|title| !title.is_empty()))
        .unwrap_or_default();
    ChatFace {
        label,
        group: false,
    }
}

fn is_group(chat: &Chat) -> bool {
    match chat.kind.as_str() {
        "CHAT" | "CHANNEL" => true,
        "DIALOG" => false,
        _ => chat.title.as_ref().is_some_and(|title| !title.is_empty()),
    }
}

fn finish_waiter(
    waiter: Waiter,
    cmd: &i64,
    payload: &Value,
    chats: &mut HashMap<i64, Chat>,
    users: &mut HashMap<i64, User>,
    store: &Store,
) {
    let failed = if *cmd == CMD_ERROR {
        Some(payload_error(payload).unwrap_or_else(|| "MAX rejected the request".into()))
    } else {
        payload_error(payload)
    };
    match waiter {
        Waiter::Ignore => {}
        Waiter::Done(reply) => {
            let _ = reply.send(match failed {
                Some(err) => Err(Error::Protocol(err)),
                None => Ok(parse_message(payload).map(|message| message.id)),
            });
        }
        Waiter::History(reply) => {
            let _ = reply.send(match failed {
                Some(err) => Err(Error::Protocol(err)),
                None => Ok(parse_messages(payload)),
            });
        }
        Waiter::Title { reply } => {
            let mut title = String::new();
            if failed.is_none() {
                for chat in parse_chats(payload) {
                    if title.is_empty() {
                        title = chat.title.clone().unwrap_or_default();
                    }
                    remember_chat(chats, chat);
                }
            }
            let _ = reply.send(title);
        }
        Waiter::Name { user_id, reply } => {
            if failed.is_none() {
                for user in parse_users(payload) {
                    remember_user(users, user);
                }
                let _ = store.merge_names_from_users(users);
            }
            let name = cached_name(users, user_id).unwrap_or_default();
            let _ = reply.send(name);
        }
        Waiter::Face { chat_id, me, reply } => {
            if failed.is_none() {
                for chat in parse_chats(payload) {
                    remember_chat(chats, chat);
                }
            }
            let _ = reply.send(chat_face(chats, users, me, chat_id));
        }
    }
}

// A full bridge inbox must not stall the MAX socket. Keep the latest status and send it when there is room.
fn push_event(tx: &mpsc::Sender<Event>, queued: &mut VecDeque<Event>, event: Event) {
    queued.retain(|old| match (&event, old) {
        (Event::Ready { .. }, Event::Ready { .. }) => false,
        (Event::Logged { .. }, Event::Logged { .. }) => false,
        _ => true,
    });
    queued.push_back(event);
    flush_events(tx, queued);
}

fn flush_events(tx: &mpsc::Sender<Event>, queued: &mut VecDeque<Event>) {
    while let Some(event) = queued.pop_front() {
        match tx.try_send(event) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(event)) => {
                queued.push_front(event);
                return;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                queued.clear();
                return;
            }
        }
    }
}

fn fail_pending(pending: &mut HashMap<i64, Waiter>, note: &str) {
    for (_, waiter) in pending.drain() {
        fail_waiter(waiter, note);
    }
}

fn fail_waiter(waiter: Waiter, note: &str) {
    let err = Error::Protocol(note.into());
    match waiter {
        Waiter::Ignore => {}
        Waiter::Title { reply } => {
            let _ = reply.send(String::new());
        }
        Waiter::Name { reply, .. } => {
            let _ = reply.send(String::new());
        }
        Waiter::Face { reply, .. } => {
            let _ = reply.send(ChatFace::default());
        }
        Waiter::Done(reply) => {
            let _ = reply.send(Err(err));
        }
        Waiter::History(reply) => {
            let _ = reply.send(Err(err));
        }
    }
}

async fn send_frame(
    link: &mut Link,
    pending: &mut HashMap<i64, Waiter>,
    opcode: i64,
    payload: Value,
    waiter: Waiter,
) -> Result<i64> {
    link.seq += 1;
    let seq = link.seq;
    let frame = request(seq, opcode, payload);
    let raw = encode_bin(&frame)?;
    pending.insert(seq, waiter);
    if let Err(err) = link.write.write_all(&raw).await {
        if let Some(waiter) = pending.remove(&seq) {
            fail_waiter(waiter, &err.to_string());
        }
        return Err(Error::Protocol(err.to_string()));
    }
    Ok(seq)
}

enum VoiceSlot {
    Armed,
    Ready,
    Waiting(oneshot::Sender<()>),
}

fn arm_voice(slots: &mut HashMap<i64, VoiceSlot>, id: i64) {
    slots.entry(id).or_insert(VoiceSlot::Armed);
}

fn note_voice(slots: &mut HashMap<i64, VoiceSlot>, payload: &Value) {
    let Some(id) = voice_ready_id(payload) else {
        return;
    };
    match slots.remove(&id) {
        Some(VoiceSlot::Waiting(tx)) => {
            let _ = tx.send(());
        }
        Some(VoiceSlot::Armed) | Some(VoiceSlot::Ready) => {
            slots.insert(id, VoiceSlot::Ready);
        }
        None => {}
    }
}

fn voice_ready_id(payload: &Value) -> Option<i64> {
    let obj = payload.as_object()?;
    for key in ["audioId", "videoId", "fileId"] {
        if let Some(id) = json_i64(obj.get(key)) {
            return Some(id);
        }
    }
    None
}

async fn roundtrip(
    link: &mut Link,
    pending: &mut HashMap<i64, Waiter>,
    voices: &mut HashMap<i64, VoiceSlot>,
    opcode: i64,
    payload: Value,
    event_tx: &mpsc::Sender<Event>,
) -> Result<Value> {
    let seq = send_frame(link, pending, opcode, payload, Waiter::Ignore).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        max_beat();
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            pending.remove(&seq);
            return Err(Error::Protocol("MAX request timed out".into()));
        }
        let frame = tokio::time::timeout(remaining, read_tcp_frame(&mut link.read, &mut link.buf))
            .await
            .map_err(|_| Error::Protocol("MAX request timed out".into()))??;
        if frame.opcode == NOTIF_ATTACH {
            note_voice(voices, &frame.payload);
        }
        if frame.seq == seq && frame.cmd != CMD_EVENT {
            pending.remove(&seq);
            if frame.cmd == CMD_ERROR {
                let err = payload_error(&frame.payload)
                    .unwrap_or_else(|| "MAX rejected the request".into());
                return Err(Error::Protocol(err));
            }
            if let Some(err) = payload_error(&frame.payload) {
                return Err(Error::Protocol(err));
            }
            return Ok(frame.payload);
        }
        if pending.contains_key(&frame.seq) && frame.cmd != CMD_EVENT {
            pending.remove(&frame.seq);
            continue;
        }
        if frame.opcode == NOTIF_MESSAGE {
            if let Some(message) = parse_message(&frame.payload) {
                if let Some(chat_id) = message.chat_id {
                    let _ = send_frame(
                        link,
                        pending,
                        NOTIF_MESSAGE,
                        ack_payload(chat_id, message.id),
                        Waiter::Ignore,
                    )
                    .await;
                }
                if message.status.as_deref() != Some("REMOVED")
                    && message.status.as_deref() != Some("EDITED")
                {
                    let _ = event_tx.try_send(Event::Message(message));
                }
            }
        } else if frame.opcode == NOTIF_MARK {
            if let Some(mark) = parse_read_mark(&frame.payload) {
                let _ = event_tx.try_send(Event::Read(mark));
            }
        } else if frame.opcode == MSG_DELIVERY {
            if let Some(mark) = parse_delivery_mark(&frame.payload) {
                let _ = event_tx.try_send(Event::Delivered(mark));
            }
        }
    }
}

async fn connect() -> Result<Link> {
    let addr = lookup_v4(AUTH_HOST, 443).await?;
    let tcp = tokio::time::timeout(Duration::from_secs(10), TcpStream::connect(addr))
        .await
        .map_err(|_| Error::Protocol(format!("MAX connect to {addr} timed out")))??;
    tcp.set_nodelay(true)?;
    let connector = TlsConnector::from(Arc::new(tls_config()));
    let name = ServerName::try_from(AUTH_HOST)
        .map_err(|err| Error::Protocol(err.to_string()))?
        .to_owned();
    let tls = connector
        .connect(name, tcp)
        .await
        .map_err(|err| Error::Protocol(err.to_string()))?;
    let (read, write) = tokio::io::split(tls);
    Ok(Link {
        write,
        read,
        buf: Vec::new(),
        seq: 0,
    })
}

pub fn install_tls_provider() {
    // reqwest links the aws-lc provider and this crate links ring. rustls
    // refuses to guess when both are present.
    let _ = rustls::crypto::ring::default_provider().install_default();
}

pub(crate) fn tls_config() -> ClientConfig {
    install_tls_provider();
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    // api2.oneme.ru sends only the leaf. Its issuer is absent from the Mozilla set,
    // and the handshake omits the intermediate, so both certificates are anchors.
    for der in [RUSSIAN_TRUSTED_SUB_CA, RUSSIAN_TRUSTED_ROOT_CA] {
        roots
            .add(CertificateDer::from(der.to_vec()))
            .expect("embedded MAX CA");
    }
    ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth()
}

const RUSSIAN_TRUSTED_SUB_CA: &[u8] = include_bytes!("../certs/russian_trusted_sub_ca.der");
const RUSSIAN_TRUSTED_ROOT_CA: &[u8] = include_bytes!("../certs/russian_trusted_root_ca.der");

pub async fn lookup_v4(host: &str, port: u16) -> Result<std::net::SocketAddr> {
    let mut found = lookup_v4_all(host, port).await?;
    Ok(found.remove(0))
}

pub async fn lookup_v4_all(host: &str, port: u16) -> Result<Vec<std::net::SocketAddr>> {
    let addrs = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::net::lookup_host((host, port)),
    )
    .await
    .map_err(|_| Error::Protocol(format!("{host} lookup timed out")))??;
    let found: Vec<_> = addrs.filter(|addr| addr.is_ipv4()).collect();
    if found.is_empty() {
        return Err(Error::Protocol(format!("{host} has no IPv4 address")));
    }
    Ok(found)
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
    use sha2::{Digest, Sha256};

    #[test]
    fn tls_config_trusts_the_oneme_issuer() {
        let _ = tls_config();
        assert_eq!(
            format!("{:x}", Sha256::digest(RUSSIAN_TRUSTED_SUB_CA)),
            "2155785036c900dbb5f1bb2a1569c80c55595bd6bf94867a29bbddbc7d88a3f2"
        );
        assert_eq!(
            format!("{:x}", Sha256::digest(RUSSIAN_TRUSTED_ROOT_CA)),
            "d26d2d0231b7c39f92cc738512ba54103519e4405d68b5bd703e9788ca8ecf31"
        );
    }

    #[test]
    fn voice_signal_is_ready_before_the_wait() {
        let mut slots = HashMap::new();
        arm_voice(&mut slots, 7);
        note_voice(&mut slots, &json!({"audioId": 7}));
        assert!(matches!(slots.get(&7), Some(VoiceSlot::Ready)));
        assert_eq!(voice_ready_id(&json!({"videoId": "9"})), Some(9));
        assert_eq!(voice_ready_id(&json!({})), None);
    }

    #[test]
    fn video_play_prefers_mp4_url() {
        let url = video_play_url(&json!({
            "EXTERNAL": "https://m.ok.ru/video/1",
            "cache": true,
            "HLS": "https://cdn.example/hls",
            "MP4_720": "https://cdn.example/720.mp4",
        }));
        assert_eq!(url.as_deref(), Some("https://cdn.example/720.mp4"));
    }

    #[test]
    fn voice_signal_wakes_a_waiting_upload() {
        let mut slots = HashMap::new();
        arm_voice(&mut slots, 7);
        let (tx, mut rx) = oneshot::channel();
        slots.insert(7, VoiceSlot::Waiting(tx));
        note_voice(&mut slots, &json!({"audioId": 7}));
        assert!(rx.try_recv().is_ok());
    }

    #[test]
    fn name_cache_survives_a_reload() {
        let dir = std::env::temp_dir().join(format!("max-names-{}", now_ms()));
        std::fs::create_dir_all(&dir).unwrap();
        let store = Store::open(dir.join("max.db")).unwrap();
        let mut users = HashMap::new();
        users.insert(7, remembered_user(7, "Маня Н"));
        store.merge_names_from_users(&users).unwrap();
        let mut again = HashMap::new();
        apply_name_cache(&mut again, &store.load_names().unwrap());
        assert_eq!(cached_name(&again, 7).as_deref(), Some("Маня Н"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn dialog_face_uses_the_other_person() {
        let mut chats = HashMap::new();
        chats.insert(
            9,
            Chat {
                id: 9,
                title: None,
                kind: "DIALOG".into(),
                peers: vec![5, 8],
                peer_marks: HashMap::new(),
                photo: None,
            },
        );
        let mut users = HashMap::new();
        users.insert(8, remembered_user(8, "Маня Н"));
        let face = chat_face(&chats, &users, 5, 9);
        assert_eq!(face.label, "Маня Н");
        assert!(!face.group);
        let quiet = dialog_id(5, 8);
        assert_eq!(quiet, 5 ^ 8);
        let synced = login_payload("t", 4, 0, 1, 2, false);
        assert_eq!(synced["contactsSync"], 0);
        assert_eq!(synced["chatsSync"], 4);
        assert_eq!(chat_face(&HashMap::new(), &users, 5, quiet).label, "Маня Н");
        assert_eq!(
            unstarted_dialogs(&chats, &[8, 5, 20], 5),
            vec![dialog_id(5, 20)]
        );
        let mut grouped = chats.clone();
        grouped.insert(
            30,
            Chat {
                id: 30,
                title: Some("Kitchen".into()),
                kind: "CHAT".into(),
                peers: vec![5, 20],
                peer_marks: HashMap::new(),
                photo: None,
            },
        );
        assert_eq!(
            unstarted_dialogs(&grouped, &[8, 20], 5),
            vec![dialog_id(5, 20)]
        );
    }

    #[test]
    fn added_contact_stays_in_the_book() {
        let added = AddedContact {
            id: 20,
            fresh: false,
            user: None,
        };
        let user = noted_contact(&added, "Иван", "Петров").unwrap();
        assert_eq!(user.display_name(), "Иван Петров");
        let mut users = HashMap::new();
        remember_user(&mut users, user);
        let dialogs = unstarted_dialogs(&HashMap::new(), &[added.id], 5);
        assert_eq!(dialogs, vec![dialog_id(5, 20)]);
        assert_eq!(
            chat_face(&HashMap::new(), &users, 5, dialogs[0]).label,
            "Иван Петров"
        );
        let dir = std::env::temp_dir().join(format!("max-contacts-{}", now_ms()));
        std::fs::create_dir_all(&dir).unwrap();
        let store = Store::open(dir.join("max.db")).unwrap();
        let mut ids = HashSet::new();
        ids.insert(added.id);
        ids.insert(0);
        store.save_contacts(&ids).unwrap();
        let loaded = store.load_contacts().unwrap();
        assert!(loaded.contains(&added.id));
        assert!(!loaded.contains(&0));
        let saved: Vec<i64> = loaded.into_iter().collect();
        assert_eq!(unstarted_dialogs(&HashMap::new(), &saved, 5), dialogs);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn group_face_uses_the_title() {
        let mut chats = HashMap::new();
        chats.insert(
            3,
            Chat {
                id: 3,
                title: Some("Kitchen".into()),
                kind: "CHAT".into(),
                peers: vec![5, 8],
                peer_marks: HashMap::new(),
                photo: Some("https://i.oneme.ru/i?r=icon".into()),
            },
        );
        let face = chat_face(&chats, &HashMap::new(), 5, 3);
        assert_eq!(face.label, "Kitchen");
        assert!(face.group);
        assert_eq!(
            stored_chat_photo(chats.get(&3).unwrap(), &HashMap::new(), 5).as_deref(),
            Some("https://i.oneme.ru/i?r=icon")
        );
    }

    #[test]
    fn dialog_photo_uses_the_other_person() {
        let mut user = remembered_user(8, "Маня Н");
        user.photo = Some("https://i.oneme.ru/i?r=a".into());
        let chat = Chat {
            id: 9,
            title: None,
            kind: "DIALOG".into(),
            peers: vec![5, 8],
            peer_marks: HashMap::new(),
            photo: Some("https://i.oneme.ru/i?r=chat".into()),
        };
        let mut users = HashMap::new();
        users.insert(8, user);
        assert_eq!(
            stored_chat_photo(&chat, &users, 5).as_deref(),
            Some("https://i.oneme.ru/i?r=a")
        );
        let favorites = Chat {
            id: 0,
            title: None,
            kind: "DIALOG".into(),
            peers: vec![5],
            peer_marks: HashMap::new(),
            photo: Some("https://i.oneme.ru/i?r=me".into()),
        };
        assert!(stored_chat_photo(&favorites, &users, 5).is_none());
        let bare = remembered_user(8, "Маня Н");
        let mut quiet = HashMap::new();
        quiet.insert(8, bare);
        assert!(stored_chat_photo(&chat, &quiet, 5).is_none());
    }

    #[test]
    fn chat_list_marker_stops_on_a_repeat() {
        let payload = json!({"marker": 10});
        let mut seen = std::collections::HashSet::new();
        assert_eq!(next_chat_marker(&payload, 20, &[1, 2], &mut seen), Some(10));
        assert_eq!(next_chat_marker(&payload, 10, &[1, 2], &mut seen), None);
        assert_eq!(
            next_chat_marker(&json!({}), 20, &[], &mut std::collections::HashSet::new()),
            None
        );
    }

    #[test]
    fn a_full_event_queue_keeps_the_latest_status() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (tx, mut rx) = mpsc::channel(1);
            let mut queued = VecDeque::new();
            push_event(&tx, &mut queued, Event::Ready { me: 1 });
            push_event(
                &tx,
                &mut queued,
                Event::Logged {
                    note: "reconnecting".into(),
                },
            );
            push_event(
                &tx,
                &mut queued,
                Event::Logged {
                    note: "logged in".into(),
                },
            );
            assert_eq!(queued.len(), 1);
            match rx.recv().await.unwrap() {
                Event::Ready { me } => assert_eq!(me, 1),
                other => panic!("expected ready, got {other:?}"),
            }
            flush_events(&tx, &mut queued);
            match rx.recv().await.unwrap() {
                Event::Logged { note } => assert_eq!(note, "logged in"),
                other => panic!("expected logged, got {other:?}"),
            }
            assert!(queued.is_empty());
        });
    }

    #[test]
    fn history_payload_names_from_field() {
        let payload = history_payload(4, 20, 99);
        assert_eq!(payload["from"], 99);
        assert_eq!(payload["itemType"], "REGULAR");
        assert_eq!(payload["chatId"], 4);
        assert_eq!(payload["interactive"], true);
    }
}
