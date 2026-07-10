use anyhow::Result;
use async_trait::async_trait;

use crate::app_state::SharedState;

#[async_trait]
pub(crate) trait TextChatAdapter: Send + Sync {
    async fn send_text(
        &self,
        state: &SharedState,
        account_id: &str,
        target: &str,
        text: &str,
    ) -> Result<String>;
}
