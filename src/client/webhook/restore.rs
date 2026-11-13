//! Restores locally delivered webhooks from storage at start-up.
use std::sync::Arc;

use super::validation::{clamp_max_connections, should_restore};
use super::worker::WorkerConfig;
use crate::client::engine::TelegramEngine;

impl TelegramEngine {
    /// Restores locally delivered webhooks from storage. Cloud webhooks are left alone.
    pub async fn restore_webhooks(self: &Arc<Self>) {
        let binlog_path = self.dir.join("webhooks_db.binlog");
        if binlog_path.exists()
            && let Ok(bytes) = tokio::fs::read(&binlog_path).await {
                for (token, url) in Self::parse_webhooks_binlog(&bytes) {
                    // Import only local webhooks, and never overwrite an existing stored record.
                    if should_restore(self.local_mode, &url) && self.storage.get_webhook(&token).await.is_none() {
                        self.storage
                            .save_webhook(&token, &url, None, self.default_max_webhook_connections, None, None)
                            .await;
                    }
                }
            }

        let saved = self.storage.get_all_webhooks().await;
        let mut count = 0;
        for (token, record) in saved {
            if !should_restore(self.local_mode, &record.url) {
                continue;
            }
            count += 1;
            self.start_webhook_worker(WorkerConfig {
                token,
                url: record.url,
                secret_token: record.secret_token,
                allowed_updates: record.allowed_updates,
                drop_pending_updates: false,
                max_connections: clamp_max_connections(Some(record.max_connections), self.default_max_webhook_connections, true),
                ip_address: record.ip_address,
            });
        }
        if count > 0 {
            tracing::info!("Restored {} local webhook configurations from SQLite WAL", count);
        }
    }

}
