use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    app_state::{ImAccountRuntimeState, SharedState, im_account_key},
    types::{ChatType, ImPlatformKind, InboundMessage, now_ms},
};

use super::api::{WecomApi, WecomSendCommand};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const AUTH_TIMEOUT: Duration = Duration::from_secs(15);

pub async fn listen_ws(
    state: SharedState,
    api: WecomApi,
    account_id: String,
    inbound_tx: mpsc::Sender<InboundMessage>,
) -> Result<()> {
    set_account_ws_state(&state, &account_id, true, false, None).await;
    let url = if api.settings.websocket_url.trim().is_empty() {
        "wss://openws.work.weixin.qq.com"
    } else {
        api.settings.websocket_url.trim()
    };
    let (stream, _) = connect_async(url)
        .await
        .with_context(|| format!("failed to connect WeCom WebSocket host={}", host_label(url)))?;
    let (mut sink, mut source) = stream.split();

    let auth_req_id = request_id("aibot_subscribe");
    let auth = json!({
        "cmd": "aibot_subscribe",
        "headers": { "req_id": auth_req_id },
        "body": {
            "bot_id": api.settings.bot_id,
            "secret": api.settings.secret,
            "scene": 1,
            "plug_version": env!("CARGO_PKG_VERSION")
        }
    });
    sink.send(Message::Text(auth.to_string().into())).await?;
    wait_for_auth(&mut source, &auth_req_id).await?;

    let (send_tx, mut send_rx) = mpsc::unbounded_channel();
    api.install_sender(Some(send_tx)).await;
    set_account_ws_state(&state, &account_id, false, true, None).await;
    state
        .push_event(
            "info",
            "wecom_ws_connected",
            format!("account={account_id}"),
        )
        .await;

    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await;
    let mut pending: HashMap<String, oneshot::Sender<Result<String>>> = HashMap::new();

    let result = loop {
        tokio::select! {
            frame = source.next() => {
                let Some(frame) = frame else {
                    break Err(anyhow::anyhow!("WeCom WebSocket closed"));
                };
                match frame? {
                    Message::Text(text) => {
                        let value: Value = serde_json::from_str(text.as_ref())
                            .context("invalid WeCom WebSocket JSON")?;
                        if let Some(req_id) = response_req_id(&value)
                            && let Some(reply) = pending.remove(req_id)
                        {
                            let result = if response_ok(&value) {
                                Ok(req_id.to_string())
                            } else {
                                Err(anyhow::anyhow!("WeCom send failed: {}", response_error(&value)))
                            };
                            let _ = reply.send(result);
                            continue;
                        }
                        if value.get("cmd").and_then(Value::as_str) == Some("aibot_msg_callback") {
                            if let Some(message) = normalize_message(&api, &account_id, &value) {
                                update_last_inbound(&state, &account_id).await;
                                inbound_tx.send(message).await.context("WeCom inbound channel closed")?;
                            }
                        }
                    }
                    Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
                    Message::Close(frame) => break Err(anyhow::anyhow!("WeCom WebSocket closed: {frame:?}")),
                    _ => {}
                }
            }
            command = send_rx.recv() => {
                let Some(command) = command else {
                    break Err(anyhow::anyhow!("WeCom outbound channel closed"));
                };
                send_command(&mut sink, &mut pending, command).await?;
            }
            _ = heartbeat.tick() => {
                let ping = json!({
                    "cmd": "ping",
                    "headers": { "req_id": request_id("ping") }
                });
                sink.send(Message::Text(ping.to_string().into())).await?;
            }
        }
    };

    api.install_sender(None).await;
    for (_, reply) in pending {
        let _ = reply.send(Err(anyhow::anyhow!("WeCom WebSocket disconnected")));
    }
    set_account_ws_state(
        &state,
        &account_id,
        false,
        false,
        result.as_ref().err().map(ToString::to_string),
    )
    .await;
    result
}

async fn wait_for_auth<S>(source: &mut S, expected_req_id: &str) -> Result<()>
where
    S: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    tokio::time::timeout(AUTH_TIMEOUT, async {
        while let Some(frame) = source.next().await {
            if let Message::Text(text) = frame? {
                let value: Value = serde_json::from_str(text.as_ref())?;
                if response_req_id(&value) == Some(expected_req_id) {
                    if response_ok(&value) {
                        return Ok(());
                    }
                    bail!("WeCom authentication failed: {}", response_error(&value));
                }
            }
        }
        bail!("WeCom WebSocket closed during authentication")
    })
    .await
    .context("WeCom authentication timed out")?
}

async fn send_command<S>(
    sink: &mut S,
    pending: &mut HashMap<String, oneshot::Sender<Result<String>>>,
    command: WecomSendCommand,
) -> Result<()>
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let req_id = request_id("aibot_send_msg");
    let frame = json!({
        "cmd": "aibot_send_msg",
        "headers": { "req_id": req_id },
        "body": {
            "chatid": command.chat_id,
            "msgtype": "markdown",
            "markdown": { "content": command.content }
        }
    });
    sink.send(Message::Text(frame.to_string().into())).await?;
    pending.insert(req_id, command.result);
    Ok(())
}

fn normalize_message(api: &WecomApi, account_id: &str, value: &Value) -> Option<InboundMessage> {
    let body = value.get("body")?;
    if body.get("msgtype")?.as_str()? != "text" {
        return None;
    }
    let sender_id = body.pointer("/from/userid")?.as_str()?.trim();
    let chat_type = match body.get("chattype").and_then(Value::as_str)? {
        "single" => ChatType::Direct,
        "group" => ChatType::Group,
        _ => return None,
    };
    let chat_id = match chat_type {
        ChatType::Direct => sender_id,
        ChatType::Group => body.get("chatid")?.as_str()?.trim(),
    };
    if !allowed(&api.settings.allowed_user_ids, sender_id)
        || !allowed(&api.settings.allowed_chat_ids, chat_id)
    {
        return None;
    }
    Some(InboundMessage {
        platform: ImPlatformKind::Wecom,
        account_id: account_id.to_string(),
        sender_id: sender_id.to_string(),
        chat_id: chat_id.to_string(),
        chat_type,
        message_id: body
            .get("msgid")
            .and_then(Value::as_str)
            .or_else(|| response_req_id(value))
            .unwrap_or_default()
            .to_string(),
        received_at_ms: now_ms(),
        text: body.pointer("/text/content")?.as_str()?.to_string(),
        mentioned: true,
        approval_request_key: None,
        action: None,
        card_message_id: None,
        attachments: Vec::new(),
    })
}

fn allowed(allowlist: &[String], value: &str) -> bool {
    allowlist.is_empty() || allowlist.iter().any(|allowed| allowed.trim() == value)
}

fn request_id(prefix: &str) -> String {
    format!("{prefix}_{}_{}", now_ms(), uuid::Uuid::new_v4().simple())
}

fn response_req_id(value: &Value) -> Option<&str> {
    value.pointer("/headers/req_id").and_then(Value::as_str)
}

fn response_ok(value: &Value) -> bool {
    value
        .get("errcode")
        .or_else(|| value.pointer("/body/errcode"))
        .and_then(Value::as_i64)
        .unwrap_or(0)
        == 0
}

fn response_error(value: &Value) -> String {
    value
        .get("errmsg")
        .or_else(|| value.pointer("/body/errmsg"))
        .and_then(Value::as_str)
        .unwrap_or("unknown error")
        .to_string()
}

fn host_label(raw: &str) -> String {
    url::Url::parse(raw)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
        .unwrap_or_else(|| "invalid-url".to_string())
}

pub(crate) async fn set_account_ws_state(
    state: &SharedState,
    account_id: &str,
    connecting: bool,
    connected: bool,
    last_error: Option<String>,
) {
    let key = im_account_key(ImPlatformKind::Wecom, account_id);
    let mut accounts = state.im_accounts.lock().await;
    let entry = accounts
        .entry(key)
        .or_insert_with(|| ImAccountRuntimeState::new(ImPlatformKind::Wecom, account_id));
    entry.connecting = connecting;
    entry.polling = false;
    entry.connected = connected;
    entry.last_error = last_error;
    entry.last_event_at_ms = Some(now_ms());
}

async fn update_last_inbound(state: &SharedState, account_id: &str) {
    let key = im_account_key(ImPlatformKind::Wecom, account_id);
    let mut accounts = state.im_accounts.lock().await;
    let entry = accounts
        .entry(key)
        .or_insert_with(|| ImAccountRuntimeState::new(ImPlatformKind::Wecom, account_id));
    let now = now_ms();
    entry.last_event_at_ms = Some(now);
    entry.last_inbound_at_ms = Some(now);
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::im::wecom::types::WecomSettings;

    fn api() -> WecomApi {
        WecomApi::new(WecomSettings {
            bot_id: "bot".into(),
            secret: "secret".into(),
            websocket_url: String::new(),
            allowed_user_ids: Vec::new(),
            allowed_chat_ids: Vec::new(),
        })
    }

    #[test]
    fn normalizes_direct_and_group_text_callbacks() {
        let direct = json!({"headers":{"req_id":"r1"},"body":{"msgid":"m1","chattype":"single","from":{"userid":"u1"},"msgtype":"text","text":{"content":"hello"}}});
        let direct = normalize_message(&api(), "a1", &direct).unwrap();
        assert_eq!(direct.platform, ImPlatformKind::Wecom);
        assert_eq!(direct.chat_id, "u1");
        assert_eq!(direct.chat_type, ChatType::Direct);

        let group = json!({"headers":{"req_id":"r2"},"body":{"msgid":"m2","chattype":"group","chatid":"g1","from":{"userid":"u2"},"msgtype":"text","text":{"content":"hi"}}});
        let group = normalize_message(&api(), "a1", &group).unwrap();
        assert_eq!(group.chat_id, "g1");
        assert_eq!(group.chat_type, ChatType::Group);
    }
}
