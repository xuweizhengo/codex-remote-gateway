use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::{Mutex, mpsc, oneshot};

use super::types::WecomSettings;

pub(crate) struct WecomSendCommand {
    pub chat_id: String,
    pub content: String,
    pub result: oneshot::Sender<Result<String>>,
}

#[derive(Clone)]
pub struct WecomApi {
    pub(crate) settings: WecomSettings,
    sender: Arc<Mutex<Option<mpsc::UnboundedSender<WecomSendCommand>>>>,
}

impl WecomApi {
    pub fn new(settings: WecomSettings) -> Self {
        Self {
            settings,
            sender: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) async fn install_sender(
        &self,
        sender: Option<mpsc::UnboundedSender<WecomSendCommand>>,
    ) {
        *self.sender.lock().await = sender;
    }

    pub async fn send_markdown(&self, chat_id: &str, content: &str) -> Result<String> {
        let sender = self
            .sender
            .lock()
            .await
            .clone()
            .context("WeCom WebSocket is not connected")?;
        let (result_tx, result_rx) = oneshot::channel();
        sender
            .send(WecomSendCommand {
                chat_id: chat_id.to_string(),
                content: content.to_string(),
                result: result_tx,
            })
            .map_err(|_| anyhow::anyhow!("WeCom WebSocket sender is closed"))?;
        tokio::time::timeout(std::time::Duration::from_secs(15), result_rx)
            .await
            .context("WeCom send acknowledgement timed out")?
            .context("WeCom send acknowledgement channel closed")?
    }
}
