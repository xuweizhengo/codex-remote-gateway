use anyhow::Result;
use async_trait::async_trait;

use crate::{app_state::SharedState, im::core::text_adapter::TextChatAdapter};

use super::api::WecomApi;

const WECOM_TEXT_CHUNK_CHARS: usize = 4000;

#[derive(Clone)]
pub struct WecomAdapter {
    api: WecomApi,
}

impl WecomAdapter {
    pub fn new(api: WecomApi) -> Self {
        Self { api }
    }

    pub async fn send_text(
        &self,
        _state: &SharedState,
        _account_id: &str,
        target: &str,
        text: &str,
    ) -> Result<String> {
        let chunks = text_chunks(text);
        let mut last_id = String::new();
        for chunk in chunks {
            last_id = self.api.send_markdown(target, &chunk).await?;
        }
        Ok(last_id)
    }
}

#[async_trait]
impl TextChatAdapter for WecomAdapter {
    async fn send_text(
        &self,
        state: &SharedState,
        account_id: &str,
        target: &str,
        text: &str,
    ) -> Result<String> {
        WecomAdapter::send_text(self, state, account_id, target, text).await
    }
}

fn text_chunks(text: &str) -> Vec<String> {
    if text.is_empty() {
        return vec![String::new()];
    }
    let chars = text.chars().collect::<Vec<_>>();
    chars
        .chunks(WECOM_TEXT_CHUNK_CHARS)
        .map(|chunk| chunk.iter().collect())
        .collect()
}
