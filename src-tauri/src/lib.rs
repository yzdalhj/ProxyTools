use std::sync::Mutex;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming, header::HOST, service::service_fn, Request, Response, StatusCode};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::{TokioExecutor, TokioIo},
};
use if_addrs::{get_if_addrs, IfAddr};
use serde::Serialize;
use tauri::{
    menu::{Menu, MenuItem},
    tray::TrayIconBuilder,
    AppHandle, Emitter, Manager, State, WindowEvent,
};
use tokio::{io::copy_bidirectional, net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use url::Url;

struct RunningProxy {
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

type WebSocketClient = Client<HttpsConnector<HttpConnector>, Full<Bytes>>;

#[derive(Default)]
struct AppState(Mutex<Option<RunningProxy>>);

#[derive(Clone, Serialize)]
struct LogEntry {
    level: String,
    message: String,
}

fn emit_log(app: &AppHandle, level: &str, message: impl Into<String>) {
    let _ = app.emit(
        "proxy-log",
        LogEntry {
            level: level.into(),
            message: message.into(),
        },
    );
}

fn local_lan_ip() -> Option<String> {
    get_if_addrs()
        .ok()?
        .into_iter()
        .find_map(|interface| match interface.addr {
            IfAddr::V4(address) if address.ip.is_private() && !address.ip.is_loopback() => {
                Some(address.ip.to_string())
            }
            _ => None,
        })
}

fn response(status: StatusCode, text: impl Into<Bytes>) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::new(text.into()))
        .unwrap()
}

fn is_hop_header(name: &str) -> bool {
    matches!(
        name,
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn target_url(mut base: Url, uri: &hyper::Uri) -> Url {
    let suffix = uri.path().trim_start_matches('/');
    let base_path = base.path().trim_end_matches('/').to_owned();
    base.set_path(&format!("{base_path}/{suffix}"));
    base.set_query(uri.query());
    base
}

fn is_websocket(request: &Request<Incoming>) -> bool {
    request
        .headers()
        .get("upgrade")
        .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"websocket"))
}

async fn forward_websocket(
    mut request: Request<Incoming>,
    base: Url,
    client: WebSocketClient,
    app: AppHandle,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let request_uri = request.uri().clone();
    let mut url = target_url(base, &request_uri);
    let scheme = match url.scheme() {
        "ws" => "http",
        "wss" => "https",
        "http" => "http",
        "https" => "https",
        _ => return Ok(response(StatusCode::BAD_REQUEST, "不支持的 WebSocket 地址")),
    };
    let _ = url.set_scheme(scheme);
    let upstream_uri = match url.as_str().parse() {
        Ok(uri) => uri,
        Err(_) => return Ok(response(StatusCode::BAD_REQUEST, "无效的 WebSocket 地址")),
    };
    emit_log(
        &app,
        "info",
        format!("WebSocket {} -> {}", request_uri, url),
    );

    let downstream_upgrade = hyper::upgrade::on(&mut request);
    let (mut parts, _) = request.into_parts();
    parts.uri = upstream_uri;
    parts.headers.remove(HOST);
    let upstream = match client
        .request(Request::from_parts(parts, Full::new(Bytes::new())))
        .await
    {
        Ok(response) => response,
        Err(error) => {
            emit_log(&app, "error", format!("WebSocket 上游连接失败：{error}"));
            return Ok(response(StatusCode::BAD_GATEWAY, error.to_string()));
        }
    };
    let status = upstream.status();
    let headers = upstream.headers().clone();
    if status != StatusCode::SWITCHING_PROTOCOLS {
        let body = match upstream.into_body().collect().await {
            Ok(body) => body.to_bytes(),
            Err(error) => return Ok(response(StatusCode::BAD_GATEWAY, error.to_string())),
        };
        let mut result = response(status, body);
        for (name, value) in &headers {
            if !is_hop_header(name.as_str()) {
                result.headers_mut().append(name, value.clone());
            }
        }
        return Ok(result);
    }

    let upstream_upgrade = hyper::upgrade::on(upstream);
    tokio::spawn(async move {
        match (downstream_upgrade.await, upstream_upgrade.await) {
            (Ok(mut downstream), Ok(mut upstream)) => {
                if let Err(error) = copy_bidirectional(
                    &mut TokioIo::new(&mut downstream),
                    &mut TokioIo::new(&mut upstream),
                )
                .await
                {
                    emit_log(&app, "error", format!("WebSocket 转发中断：{error}"));
                }
            }
            (Err(error), _) | (_, Err(error)) => {
                emit_log(&app, "error", format!("WebSocket 升级失败：{error}"));
            }
        }
    });

    let mut result = response(status, Bytes::new());
    for (name, value) in &headers {
        result.headers_mut().append(name, value.clone());
    }
    Ok(result)
}

async fn forward(
    request: Request<Incoming>,
    base: Url,
    client: reqwest::Client,
    websocket_client: WebSocketClient,
    app: AppHandle,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    if is_websocket(&request) {
        return forward_websocket(request, base, websocket_client, app).await;
    }
    if matches!(base.scheme(), "ws" | "wss") {
        return Ok(response(
            StatusCode::BAD_REQUEST,
            "ws/wss 地址只能转发 WebSocket 请求",
        ));
    }
    let (parts, body) = request.into_parts();
    let url = target_url(base, &parts.uri);
    emit_log(
        &app,
        "info",
        format!("{} {} -> {}", parts.method, parts.uri, url),
    );

    let mut upstream = client.request(parts.method, url);
    for (name, value) in &parts.headers {
        if !is_hop_header(name.as_str()) && name != "host" {
            upstream = upstream.header(name, value);
        }
    }
    let body = match body.collect().await {
        Ok(body) => body.to_bytes(),
        Err(_) => {
            emit_log(&app, "error", "请求正文读取失败");
            return Ok(response(StatusCode::BAD_REQUEST, "Invalid request body"));
        }
    };
    let upstream = match upstream.body(body).send().await {
        Ok(response) => response,
        Err(error) => {
            emit_log(&app, "error", format!("上游请求失败：{error}"));
            return Ok(response(StatusCode::BAD_GATEWAY, error.to_string()));
        }
    };
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let body = match upstream.bytes().await {
        Ok(body) => body,
        Err(error) => {
            emit_log(&app, "error", format!("上游响应读取失败：{error}"));
            return Ok(response(StatusCode::BAD_GATEWAY, error.to_string()));
        }
    };
    let mut result = response(status, body);
    for (name, value) in &headers {
        if !is_hop_header(name.as_str()) {
            result.headers_mut().append(name, value.clone());
        }
    }
    Ok(result)
}

#[tauri::command]
fn get_local_ip() -> Result<String, String> {
    local_lan_ip().ok_or_else(|| "未找到局域网 IPv4 地址".into())
}

#[tauri::command]
async fn start_proxy(
    app: AppHandle,
    state: State<'_, AppState>,
    target: String,
    host: String,
    port: u16,
) -> Result<String, String> {
    let base = Url::parse(&target)
        .map_err(|_| "转发网址必须以 http://、https://、ws:// 或 wss:// 开头".to_string())?;
    if !matches!(base.scheme(), "http" | "https" | "ws" | "wss") || base.host().is_none() {
        return Err("转发网址必须是有效的 HTTP(S) 或 WebSocket 地址".into());
    }
    if state.0.lock().map_err(|_| "状态锁定失败")?.is_some() {
        return Err("代理已启动".into());
    }
    #[cfg(unix)]
    if port < 1024 {
        return Err("1024 以下端口需要管理员权限，请使用 8080 等端口".into());
    }
    let address = format!("{host}:{port}");
    let listener = TcpListener::bind(&address)
        .await
        .map_err(|error| error.to_string())?;
    emit_log(&app, "info", format!("转发已启动：{address} -> {base}"));
    let cancel = CancellationToken::new();
    let stop = cancel.clone();
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| error.to_string())?;
    let websocket_client = Client::builder(TokioExecutor::new()).build(
        HttpsConnectorBuilder::new()
            .with_native_roots()
            .map_err(|error| error.to_string())?
            .https_or_http()
            .enable_http1()
            .build(),
    );
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = stop.cancelled() => break,
                accepted = listener.accept() => match accepted {
                    Ok((stream, _)) => {
                        let base = base.clone();
                        let client = client.clone();
                        let websocket_client = websocket_client.clone();
                        let app = app.clone();
                        tokio::spawn(async move {
                            let service_app = app.clone();
                            let service = service_fn(move |request| {
                                forward(
                                    request,
                                    base.clone(),
                                    client.clone(),
                                    websocket_client.clone(),
                                    service_app.clone(),
                                )
                            });
                            if let Err(error) = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), service).with_upgrades().await {
                                emit_log(&app, "error", format!("客户端连接错误：{error}"));
                            }
                        });
                    }
                    Err(error) => {
                        emit_log(&app, "error", format!("监听错误：{error}"));
                        break;
                    }
                }
            }
        }
    });
    *state.0.lock().map_err(|_| "状态锁定失败")? = Some(RunningProxy { cancel, task });
    Ok(address)
}

#[tauri::command]
fn stop_proxy(app: AppHandle, state: State<'_, AppState>) -> Result<(), String> {
    if let Some(running) = state.0.lock().map_err(|_| "状态锁定失败")?.take() {
        running.cancel.cancel();
        running.task.abort();
        emit_log(&app, "info", "转发已停止");
    }
    Ok(())
}

pub fn run() {
    tauri::Builder::default()
        .manage(AppState::default())
        .invoke_handler(tauri::generate_handler![
            get_local_ip,
            start_proxy,
            stop_proxy
        ])
        .setup(|app| {
            let show = MenuItem::with_id(app, "show", "显示窗口", true, None::<&str>)?;
            let quit = MenuItem::with_id(app, "quit", "退出", true, None::<&str>)?;
            let menu = Menu::with_items(app, &[&show, &quit])?;
            TrayIconBuilder::with_id("tray")
                .icon(app.default_window_icon().unwrap().clone())
                .menu(&menu)
                .on_menu_event(|app, event| match event.id().as_ref() {
                    "show" => {
                        if let Some(window) = app.get_webview_window("main") {
                            let _ = window.show();
                            let _ = window.set_focus();
                        }
                    }
                    "quit" => app.exit(0),
                    _ => {}
                })
                .build(app)?;
            Ok(())
        })
        .on_window_event(|window, event| {
            if let WindowEvent::CloseRequested { api, .. } = event {
                api.prevent_close();
                let _ = window.hide();
            }
        })
        .run(tauri::generate_context!())
        .expect("error while running VPN LAN Proxy");
}
