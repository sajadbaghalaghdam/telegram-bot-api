use serde::{Deserialize, Serialize};
use super::Message;

/// This object represents an incoming update.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Update {
    /// The update's unique identifier.
    pub update_id: i64,
    
    /// New incoming message of any kind — text, photo, sticker, etc.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<Message>,

    /// New version of a message that is known to the bot and was edited
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edited_message: Option<Message>,

    /// New incoming channel post of any kind — text, photo, sticker, etc.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_post: Option<Message>,

    /// New version of a channel post that is known to the bot and was edited
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edited_channel_post: Option<Message>,

    /// New incoming callback query
    #[serde(skip_serializing_if = "Option::is_none")]
    pub callback_query: Option<serde_json::Value>,

    /// New incoming inline query
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inline_query: Option<serde_json::Value>,
}

/// Request parameters for the `getUpdates` method.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GetUpdatesRequest {
    /// Identifier of the first update to be returned.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<i64>,
    
    /// Limits the number of updates to be retrieved. Values between 1-100 are accepted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i32>,
    
    /// Timeout in seconds for long polling.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<i32>,
    
    /// A JSON-serialized list of the update types you want your bot to receive.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_updates: Option<Vec<String>>,
}

/// Request parameters for `setWebhook`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetWebhookRequest {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_updates: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drop_pending_updates: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_token: Option<String>,
}

/// Describes the current status of a webhook.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookInfo {
    pub url: String,
    pub has_custom_certificate: bool,
    pub pending_update_count: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_date: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_synchronization_error_date: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_updates: Option<Vec<String>>,
}
