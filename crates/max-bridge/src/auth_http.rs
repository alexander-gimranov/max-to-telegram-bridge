use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use max_client::{new_device_id, write_session, CodeStatus, PendingAuth, Session};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{watch, Mutex};

use crate::text;

pub const AUTH_URL: &str = "http://127.0.0.1:8787";
const AUTH_HOLD: Duration = Duration::from_secs(180);

async fn bind_auth() -> Result<tokio::net::TcpListener, String> {
    let mut last = String::from("auth page failed to bind");
    for attempt in 0..40 {
        match tokio::net::TcpListener::bind("127.0.0.1:8787").await {
            Ok(listener) => return Ok(listener),
            Err(err) => {
                last = err.to_string();
                if attempt == 0 {
                    eprintln!("bridge: auth page is busy; waiting");
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    Err(last)
}

fn page_html() -> &'static str {
    static PAGE: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    PAGE.get_or_init(|| {
        include_str!("../webapp/auth.html")
            .replace("{{lang}}", text::HTML_LANG)
            .replace("{{title}}", text::AUTH_TITLE)
            .replace("{{phone_label}}", text::PHONE_LABEL)
            .replace("{{next}}", text::NEXT)
            .replace("{{code_label}}", text::CODE_LABEL)
            .replace("{{password_label}}", text::PASSWORD_LABEL)
            .replace("{{sign_in}}", text::SIGN_IN)
            .replace("{{page_error}}", text::PAGE_ERROR)
    })
    .as_str()
}

#[derive(Clone)]
pub struct AuthHub {
    inner: Arc<Mutex<AuthInner>>,
    notify: watch::Sender<u64>,
}

struct AuthInner {
    session_path: PathBuf,
    auth: Option<PendingAuth>,
    device_id: String,
    phase: Phase,
    code_token: String,
    code_digits: i64,
    code_wait: i64,
    track_id: String,
    password_hint: Option<String>,
    touched: Instant,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Idle,
    Code,
    Password,
}

#[derive(Deserialize)]
struct PhoneBody {
    phone: String,
}

#[derive(Deserialize)]
struct CodeBody {
    code: String,
}

#[derive(Deserialize)]
struct PasswordBody {
    password: String,
}

impl AuthHub {
    pub fn new(session_path: PathBuf) -> Self {
        let (notify, _) = watch::channel(0u64);
        Self {
            inner: Arc::new(Mutex::new(AuthInner {
                session_path,
                auth: None,
                device_id: String::new(),
                phase: Phase::Idle,
                code_token: String::new(),
                code_digits: 6,
                code_wait: 60,
                track_id: String::new(),
                password_hint: None,
                touched: Instant::now(),
            })),
            notify,
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.notify.subscribe()
    }

    pub async fn listen(self) -> Result<(), String> {
        let hub = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(20)).await;
                hub.maintain().await;
            }
        });
        let listener = bind_auth().await?;
        eprintln!("bridge: auth page at {AUTH_URL}");
        let app = Router::new()
            .route("/", get(page))
            .route("/auth/phone", post(phone))
            .route("/auth/code", post(code))
            .route("/auth/password", post(password))
            .with_state(self);
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .map_err(|err| err.to_string())
    }

    async fn maintain(&self) {
        let mut inner = self.inner.lock().await;
        if inner.auth.is_none() {
            return;
        }
        if inner.touched.elapsed() > AUTH_HOLD {
            inner.reset();
            eprintln!("bridge: auth page timed out");
            return;
        }
        let Some(auth) = inner.auth.as_mut() else {
            return;
        };
        if let Err(err) = auth.ping().await {
            inner.reset();
            eprintln!("bridge: auth socket closed: {err}");
        }
    }

    async fn submit_phone(&self, phone: &str) -> Value {
        let phone = phone.trim();
        if phone.is_empty() {
            return failure_step(text::ENTER_PHONE);
        }
        let mut inner = self.inner.lock().await;
        if inner.auth.is_some() {
            return inner.current_step();
        }
        let device_id = new_device_id();
        let mut auth = match PendingAuth::connect(&device_id).await {
            Ok(auth) => auth,
            Err(err) => return failure_step(&err.to_string()),
        };
        let challenge = match auth.request_code(phone).await {
            Ok(challenge) => challenge,
            Err(err) => return failure_step(&err.to_string()),
        };
        inner.device_id = device_id;
        inner.code_token = challenge.token;
        inner.code_digits = challenge.code_length;
        inner.code_wait = challenge.wait_secs;
        inner.phase = Phase::Code;
        inner.touched = Instant::now();
        inner.auth = Some(auth);
        code_step(challenge.code_length, challenge.wait_secs)
    }

    async fn submit_code(&self, code: &str) -> Value {
        let code = code.trim();
        if code.is_empty() {
            return json!({
                "step": "code",
                "message": text::ENTER_CODE,
            });
        }
        let mut inner = self.inner.lock().await;
        if inner.phase != Phase::Code || inner.auth.is_none() {
            return failure_step(text::PHONE_FIRST);
        }
        let token = inner.code_token.clone();
        let status = {
            let auth = inner.auth.as_mut().unwrap();
            match auth.submit_code(&token, code).await {
                Ok(status) => status,
                Err(err) => {
                    inner.reset();
                    return failure_step(&err.to_string());
                }
            }
        };
        inner.touched = Instant::now();
        match after_code(status) {
            AfterCode::Unregistered => {
                inner.reset();
                failure_step(text::UNREGISTERED)
            }
            AfterCode::Password { track_id, hint } => {
                inner.track_id = track_id;
                inner.password_hint = hint.clone();
                inner.phase = Phase::Password;
                password_step(hint)
            }
            AfterCode::Token(token) => self.finish(&mut inner, token).await,
        }
    }

    async fn submit_password(&self, password: &str) -> Value {
        if password.is_empty() {
            return json!({
                "step": "password",
                "message": text::ENTER_PASSWORD,
            });
        }
        let mut inner = self.inner.lock().await;
        if inner.phase != Phase::Password || inner.auth.is_none() {
            return failure_step(text::CODE_FIRST);
        }
        let track_id = inner.track_id.clone();
        let token = {
            let auth = inner.auth.as_mut().unwrap();
            match auth.submit_password(&track_id, password).await {
                Ok(token) => token,
                Err(err) => {
                    inner.reset();
                    return failure_step(&err.to_string());
                }
            }
        };
        inner.touched = Instant::now();
        self.finish(&mut inner, token).await
    }

    async fn finish(&self, inner: &mut AuthInner, login_token: String) -> Value {
        let Some(mut auth) = inner.auth.take() else {
            return failure_step(text::PHONE_FIRST);
        };
        let instance_id = auth.instance_id().to_owned();
        let device_id = inner.device_id.clone();
        let saved = match auth.finish_login(&login_token).await {
            Ok(token) => token,
            Err(err) => {
                inner.reset();
                return failure_step(&err.to_string());
            }
        };
        drop(auth);
        if let Err(err) = write_session(
            &inner.session_path,
            &Session::signed_in(saved, device_id, instance_id),
        ) {
            inner.reset();
            return failure_step(&err.to_string());
        }
        inner.reset();
        self.notify.send_modify(|generation| {
            *generation = generation.wrapping_add(1);
        });
        done_step()
    }
}

impl AuthInner {
    fn reset(&mut self) {
        self.auth = None;
        self.device_id.clear();
        self.phase = Phase::Idle;
        self.code_token.clear();
        self.track_id.clear();
        self.password_hint = None;
    }

    fn current_step(&self) -> Value {
        match self.phase {
            Phase::Code => code_step(self.code_digits, self.code_wait),
            Phase::Password => password_step(self.password_hint.clone()),
            Phase::Idle => failure_step(text::ENTER_PHONE),
        }
    }
}

enum AfterCode {
    Token(String),
    Password {
        track_id: String,
        hint: Option<String>,
    },
    Unregistered,
}

fn after_code(status: CodeStatus) -> AfterCode {
    match status {
        CodeStatus::Token(token) => AfterCode::Token(token),
        CodeStatus::Password { track_id, hint } => AfterCode::Password { track_id, hint },
        CodeStatus::Unregistered => AfterCode::Unregistered,
    }
}

pub fn from_loopback(addr: SocketAddr) -> bool {
    addr.ip().is_loopback()
}

pub fn code_step(digits: i64, wait_secs: i64) -> Value {
    json!({
        "step": "code",
        "digits": digits,
        "waitSecs": wait_secs,
        "message": text::code_call(digits, wait_secs),
    })
}

pub fn password_step(hint: Option<String>) -> Value {
    let message = match hint.as_deref() {
        Some(hint) if !hint.is_empty() => text::password_hint(hint),
        _ => String::new(),
    };
    json!({
        "step": "password",
        "message": message,
    })
}

pub fn done_step() -> Value {
    json!({
        "step": "done",
        "message": text::SIGNED_IN,
    })
}

fn failure_step(message: &str) -> Value {
    json!({
        "step": "phone",
        "message": message,
    })
}

async fn page(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Result<axum::response::Html<&'static str>, axum::http::StatusCode> {
    if !from_loopback(addr) {
        return Err(axum::http::StatusCode::FORBIDDEN);
    }
    Ok(axum::response::Html(page_html()))
}

async fn phone(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(hub): State<AuthHub>,
    Json(body): Json<PhoneBody>,
) -> Result<Json<Value>, axum::http::StatusCode> {
    if !from_loopback(addr) {
        return Err(axum::http::StatusCode::FORBIDDEN);
    }
    Ok(Json(hub.submit_phone(&body.phone).await))
}

async fn code(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(hub): State<AuthHub>,
    Json(body): Json<CodeBody>,
) -> Result<Json<Value>, axum::http::StatusCode> {
    if !from_loopback(addr) {
        return Err(axum::http::StatusCode::FORBIDDEN);
    }
    Ok(Json(hub.submit_code(&body.code).await))
}

async fn password(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(hub): State<AuthHub>,
    Json(body): Json<PasswordBody>,
) -> Result<Json<Value>, axum::http::StatusCode> {
    if !from_loopback(addr) {
        return Err(axum::http::StatusCode::FORBIDDEN);
    }
    Ok(Json(hub.submit_password(&body.password).await))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn code_step_names_the_call() {
        let step = code_step(6, 60);
        assert_eq!(step["step"], "code");
        assert_eq!(step["digits"], 6);
        assert_eq!(step["waitSecs"], 60);
        assert_eq!(step["message"], text::code_call(6, 60));
    }

    #[test]
    fn password_step_keeps_the_hint() {
        let hinted = password_step(Some("pet".into()));
        assert_eq!(hinted["step"], "password");
        assert_eq!(hinted["message"], text::password_hint("pet"));
        let plain = password_step(None);
        assert_eq!(plain["message"], "");
        assert_eq!(done_step()["message"], text::SIGNED_IN);
    }

    #[test]
    fn unregistered_number_returns_to_the_phone_step() {
        let step = match after_code(CodeStatus::Unregistered) {
            AfterCode::Unregistered => failure_step(text::UNREGISTERED),
            AfterCode::Token(_) | AfterCode::Password { .. } => json!({}),
        };
        assert_eq!(step["step"], "phone");
        assert_eq!(step["message"], text::UNREGISTERED);
    }

    #[test]
    fn page_accepts_only_loopback() {
        let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8787);
        let remote = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 8)), 8787);
        assert!(from_loopback(local));
        assert!(!from_loopback(remote));
        let page = page_html();
        assert!(page.contains("id=\"phone\""));
        assert!(page.contains("id=\"code\""));
        assert!(page.contains("id=\"password\""));
        assert!(page.contains("type=\"password\""));
        assert!(page.contains(text::AUTH_TITLE));
        assert!(page.contains(text::PHONE_LABEL));
        assert!(page.contains(text::PAGE_ERROR));
        assert!(!page.contains("{{"));
    }
}
