use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::ClientConfig;
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_rustls::TlsConnector;

use crate::error::{Error, Result};
use crate::frame::{
    decode_bin, encode_bin, json_i64, payload_error, request, AUTH, AUTH_LOGIN_CHECK_PASSWORD,
    AUTH_REQUEST, CHAT_HISTORY, CHAT_INFO, CHATS_LIST, CMD_ERROR, CMD_EVENT, CONTACT_INFO, LOGIN,
    MSG_SEND, NOTIF_ATTACH, NOTIF_MESSAGE, PHOTO_UPLOAD, PING, SESSION_INIT, VIDEO_PLAY,
    VIDEO_UPLOAD,
};
use crate::models::{
    ack_payload, auth_code_payload, auth_mode, auth_password_payload, auth_request_payload,
    history_payload, login_payload, me_id, mobile_handshake_payload, parse_chats,
    parse_code_challenge, parse_code_status, parse_login_token, parse_message, parse_messages,
    parse_users, send_payload, sync_time, Attach, Chat, ChatFace, CodeChallenge, CodeStatus,
    Message, Name, User, VoiceTicket,
};
use crate::profile::{self, Session};
use crate::voice_tcp::VoiceLink;

const QUIET_FOR: Duration = Duration::from_secs(60);
const AUTH_HOST: &str = "api2.oneme.ru";

#[derive(Debug)]
pub enum Event {
    Ready { me: i64 },
    Message(Message),
    Logged { note: String },
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
    VideoPlay {
        chat_id: i64,
        message_id: i64,
        video_id: i64,
        reply: oneshot::Sender<Result<String>>,
    },
}

pub struct Client {
    cmd_tx: mpsc::Sender<Cmd>,
    events: mpsc::Receiver<Event>,
    session: Session,
    voice_hold: Arc<AtomicBool>,
    voice_cids: Arc<Mutex<Vec<i64>>>,
}

impl Client {
    pub async fn start(session_path: PathBuf) -> Result<Self> {
        let session = profile::read_session(&session_path)?.ok_or(Error::SessionNotFound)?;
        let saved = session.clone();
        let voice_hold = Arc::new(AtomicBool::new(false));
        let voice_cids = Arc::new(Mutex::new(Vec::new()));
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let (event_tx, events) = mpsc::channel(256);
        let hold = Arc::clone(&voice_hold);
        let cids = Arc::clone(&voice_cids);
        tokio::spawn(async move {
            if let Err(err) =
                run_forever(session, session_path, cmd_rx, event_tx.clone(), hold, cids).await
            {
                let _ = event_tx
                    .send(Event::Logged {
                        note: err.to_string(),
                    })
                    .await;
            }
        });
        Ok(Client {
            cmd_tx,
            events,
            session: saved,
            voice_hold,
            voice_cids,
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

    pub async fn send_text(
        &self,
        chat_id: i64,
        text: &str,
        reply_to: Option<i64>,
        attaches: Vec<Attach>,
    ) -> Result<Option<i64>> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Cmd::SendText {
                chat_id,
                text: text.to_owned(),
                reply_to,
                attaches,
                reply,
            })
            .await
            .map_err(|_| Error::Protocol("MAX connection stopped".into()))?;
        rx.await
            .map_err(|_| Error::Protocol("MAX connection stopped".into()))?
    }

    pub async fn photo_upload_url(&self) -> Result<String> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Cmd::PhotoUpload { reply })
            .await
            .map_err(|_| Error::Protocol("MAX connection stopped".into()))?;
        rx.await
            .map_err(|_| Error::Protocol("MAX connection stopped".into()))?
    }

    pub async fn voice_upload(&self) -> Result<VoiceTicket> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Cmd::VoiceUpload { reply })
            .await
            .map_err(|_| Error::Protocol("MAX connection stopped".into()))?;
        rx.await
            .map_err(|_| Error::Protocol("MAX connection stopped".into()))?
    }

    pub async fn wait_voice(&self, audio_id: i64) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Cmd::WaitVoice { audio_id, reply })
            .await
            .map_err(|_| Error::Protocol("MAX connection stopped".into()))?;
        match tokio::time::timeout(Duration::from_secs(12), rx).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(Error::Protocol("MAX connection stopped".into())),
            Err(_) => Err(Error::Protocol("voice is not ready".into())),
        }
    }

    pub async fn chat_title(&self, chat_id: i64) -> String {
        let (reply, rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(Cmd::ChatTitle { chat_id, reply })
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
        if self
            .cmd_tx
            .send(Cmd::UserName { user_id, reply })
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
        if self
            .cmd_tx
            .send(Cmd::ChatFace { chat_id, reply })
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
        self.cmd_tx
            .send(Cmd::ChatPhoto { chat_id, reply })
            .await
            .map_err(|_| ())?;
        rx.await.map_err(|_| ())?
    }

    pub async fn fetch_history(
        &self,
        chat_id: i64,
        backward: i64,
        from_ms: i64,
    ) -> Result<Vec<Message>> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Cmd::History {
                chat_id,
                backward,
                from_ms,
                reply,
            })
            .await
            .map_err(|_| Error::Protocol("MAX connection stopped".into()))?;
        rx.await
            .map_err(|_| Error::Protocol("MAX connection stopped".into()))?
    }

    pub async fn chat_ids(&self) -> Result<Vec<i64>> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Cmd::ChatIds { reply })
            .await
            .map_err(|_| Error::Protocol("MAX connection stopped".into()))?;
        rx.await
            .map_err(|_| Error::Protocol("MAX connection stopped".into()))
    }

    pub async fn video_play_url(
        &self,
        chat_id: i64,
        message_id: i64,
        video_id: i64,
    ) -> Result<String> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Cmd::VideoPlay {
                chat_id,
                message_id,
                video_id,
                reply,
            })
            .await
            .map_err(|_| Error::Protocol("MAX connection stopped".into()))?;
        rx.await
            .map_err(|_| Error::Protocol("MAX connection stopped".into()))?
    }
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
    session_path: PathBuf,
    mut cmd_rx: mpsc::Receiver<Cmd>,
    event_tx: mpsc::Sender<Event>,
    voice_hold: Arc<AtomicBool>,
    voice_cids: Arc<Mutex<Vec<i64>>>,
) -> Result<()> {
    let mut attempt = 0u32;
    loop {
        match run_once(
            &mut session,
            &session_path,
            &mut cmd_rx,
            &event_tx,
            &voice_cids,
        )
        .await
        {
            Ok(()) => attempt = 0,
            Err(err) => {
                if credential_rejected(&err) {
                    let _ = std::fs::remove_file(&session_path);
                    return Err(Error::Protocol(
                        "saved MAX session was rejected. Run the program again and sign in".into(),
                    ));
                }
                while voice_hold.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                attempt = attempt.saturating_add(1);
                let delay = Duration::from_secs(u64::from(attempt.min(5)));
                let _ = event_tx
                    .send(Event::Logged {
                        note: format!("{err}; reconnecting"),
                    })
                    .await;
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
    session_path: &std::path::Path,
    cmd_rx: &mut mpsc::Receiver<Cmd>,
    event_tx: &mpsc::Sender<Event>,
    voice_cids: &Mutex<Vec<i64>>,
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
        login_payload(
            &session.token,
            session.chats_sync,
            session.contacts_sync,
            session.drafts_sync,
            session.presence_sync,
            session.desktop,
        ),
        event_tx,
    )
    .await?;
    if let Some(err) = payload_error(&logged) {
        return Err(Error::Protocol(err));
    }
    let me = me_id(&logged).ok_or_else(|| Error::Protocol("login did not return a profile".into()))?;
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
    let _ = profile::write_session(session_path, session);

    let names_file = names_path(session_path);
    let mut chats: HashMap<i64, Chat> = parse_chats(&logged)
        .into_iter()
        .map(|chat| (chat.id, chat))
        .collect();
    let mut users = HashMap::new();
    apply_name_cache(&mut users, &load_name_cache(&names_file));
    for user in parse_users(&logged) {
        remember_user(&mut users, user);
    }
    save_name_cache(&names_file, &users);
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

    let _ = event_tx
        .send(Event::Ready { me })
        .await;
    let _ = event_tx
        .send(Event::Logged {
            note: "logged in".into(),
        })
        .await;

    let cid = Arc::new(AtomicI64::new(now_ms()));
    let mut sent_cids = VecDeque::new();
    let mut ping = tokio::time::interval(Duration::from_secs(25));
    ping.tick().await;
    let mut last_rx = tokio::time::Instant::now();

    loop {
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
                            &names_file,
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
                            &names_file,
                            me,
                            chat_id,
                        )
                        .await;
                        let _ = reply.send(photo);
                    }
                    Cmd::ChatFace { chat_id, reply } => {
                        if chats.contains_key(&chat_id) {
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
                }
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
    let next = json_i64(payload.get("marker"))?;
    if next == marker {
        None
    } else {
        Some(next)
    }
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
    let audio_id = crate::frame::json_i64(info.get("videoId")).ok_or_else(|| {
        Error::Protocol("MAX voice upload did not return an id".into())
    })?;
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

fn names_path(session_path: &Path) -> PathBuf {
    session_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("names.json")
}

fn load_name_cache(path: &Path) -> HashMap<i64, String> {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return HashMap::new();
    };
    serde_json::from_str(&raw).unwrap_or_default()
}

fn save_name_cache(path: &Path, users: &HashMap<i64, User>) {
    let mut names = load_name_cache(path);
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
    if !changed && path.exists() {
        return;
    }
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let Ok(raw) = serde_json::to_string(&names) else {
        return;
    };
    let tmp = path.with_extension("json.tmp");
    if std::fs::write(&tmp, raw).is_err() {
        let _ = std::fs::remove_file(&tmp);
        return;
    }
    restrict_file(&tmp);
    if std::fs::rename(&tmp, path).is_err() {
        let _ = std::fs::remove_file(&tmp);
        return;
    }
    restrict_file(path);
}

fn restrict_file(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    #[cfg(not(unix))]
    {
        let _ = path;
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
    names_file: &Path,
    me: i64,
    chat_id: i64,
) -> std::result::Result<Option<String>, ()> {
    if chat_id == 0 {
        return Ok(None);
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
    save_name_cache(names_file, users);
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
    users.get(&peer).and_then(|user| clean_photo(user.photo.clone()))
}

fn chat_face(
    chats: &HashMap<i64, Chat>,
    users: &HashMap<i64, User>,
    me: i64,
    chat_id: i64,
) -> ChatFace {
    let Some(chat) = chats.get(&chat_id) else {
        return ChatFace::default();
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
        _ => chat
            .title
            .as_ref()
            .is_some_and(|title| !title.is_empty()),
    }
}

fn finish_waiter(
    waiter: Waiter,
    cmd: &i64,
    payload: &Value,
    chats: &mut HashMap<i64, Chat>,
    users: &mut HashMap<i64, User>,
    names_path: &Path,
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
                save_name_cache(names_path, users);
            }
            let name = cached_name(users, user_id).unwrap_or_default();
            let _ = reply.send(name);
        }
        Waiter::Face {
            chat_id,
            me,
            reply,
        } => {
            if failed.is_none() {
                for chat in parse_chats(payload) {
                    remember_chat(chats, chat);
                }
            }
            let _ = reply.send(chat_face(chats, users, me, chat_id));
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
        }
    }
}

async fn connect() -> Result<Link> {
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
    Ok(Link {
        write,
        read,
        buf: Vec::new(),
        seq: 0,
    })
}

pub(crate) fn tls_config() -> ClientConfig {
    // reqwest links the aws-lc provider and this crate links ring. rustls
    // refuses to guess when both are present.
    let _ = rustls::crypto::ring::default_provider().install_default();
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
    let mut addrs = tokio::net::lookup_host((host, port)).await?;
    addrs
        .find(|addr| addr.is_ipv4())
        .ok_or_else(|| Error::Protocol(format!("{host} has no IPv4 address")))
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
        let path = dir.join("names.json");
        let mut users = HashMap::new();
        users.insert(7, remembered_user(7, "Маня Н"));
        save_name_cache(&path, &users);
        let mut again = HashMap::new();
        apply_name_cache(&mut again, &load_name_cache(&path));
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
                photo: None,
            },
        );
        let mut users = HashMap::new();
        users.insert(8, remembered_user(8, "Маня Н"));
        let face = chat_face(&chats, &users, 5, 9);
        assert_eq!(face.label, "Маня Н");
        assert!(!face.group);
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
    fn history_payload_names_from_field() {
        let payload = history_payload(4, 20, 99);
        assert_eq!(payload["from"], 99);
        assert_eq!(payload["itemType"], "REGULAR");
        assert_eq!(payload["chatId"], 4);
        assert_eq!(payload["interactive"], true);
    }
}
