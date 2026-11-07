use std::sync::Arc;
use std::time::Duration;
use reqwest::Client;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use crate::models::Update;

/// Webhook dispatch engine matching official C++ WebhookActor.h/WebhookActor.cpp
pub struct WebhookActor {
    client: Client,
    url: String,
    secret_token: Option<String>,
    connection_limit: Arc<Semaphore>,
}

impl WebhookActor {
    pub fn new(url: String, secret_token: Option<String>, max_connections: usize) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .tcp_nodelay(true)
            .build()
            .unwrap_or_default();

        Self {
            client,
            url,
            secret_token,
            connection_limit: Arc::new(Semaphore::new(max_connections.max(1))),
        }
    }

    /// Dispatches an update asynchronously with exponential backoff retries
    pub fn dispatch_update(&self, update: Update) {
        let client = self.client.clone();
        let url = self.url.clone();
        let secret = self.secret_token.clone();
        let sem = Arc::clone(&self.connection_limit);

        tokio::spawn(async move {
            let _permit = match sem.acquire().await {
                Ok(p) => p,
                Err(_) => return,
            };

            let mut retries = 0;
            let max_retries = 3;
            let mut delay = Duration::from_secs(2);

            while retries <= max_retries {
                let mut req = client.post(&url).json(&update);
                if let Some(ref sec) = secret {
                    req = req.header("X-Telegram-Bot-Api-Secret-Token", sec);
                }

                match req.send().await {
                    Ok(resp) if resp.status().is_success() => {
                        return; // Successfully delivered
                    }
                    Ok(resp) => {
                        tracing::debug!("Webhook delivery attempt {} received HTTP {}", retries + 1, resp.status());
                    }
                    Err(e) => {
                        tracing::debug!("Webhook delivery attempt {} error: {}", retries + 1, e);
                    }
                }

                retries += 1;
                if retries <= max_retries {
                    sleep(delay).await;
                    delay *= 2; // Exponential backoff
                }
            }

            tracing::warn!("Failed to deliver webhook update {} after {} retries", update.update_id, max_retries);
        });
    }
}
