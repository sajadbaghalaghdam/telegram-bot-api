use serde::{Deserialize, Serialize};

/// Represents a Chat object as defined by the Telegram Bot API.
/// A chat is a conversation with a bot, which can be a private chat, 
/// a group, a supergroup, or a channel.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Chat {
    /// Unique identifier for this chat.
    pub id: i64,

    /// Type of the chat: `private`, `group`, `supergroup`, or `channel`.
    #[serde(rename = "type")]
    pub chat_type: ChatType,

    /// Title of the group or channel.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,

    /// Username of the chat (if available).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,

    /// First name of the user (only for private chats).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_name: Option<String>,

    /// Last name of the user (only for private chats).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_name: Option<String>,
}

/// Strongly typed representation of the chat type to avoid string-based errors.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ChatType {
    Private,
    Group,
    Supergroup,
    Channel,
}

impl From<String> for ChatType {
    fn from(s: String) -> Self {
        match s.as_str() {
            "group" => ChatType::Group,
            "supergroup" => ChatType::Supergroup,
            "channel" => ChatType::Channel,
            _ => ChatType::Private,
        }
    }
}

impl std::fmt::Display for ChatType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ChatType::Private => "private",
            ChatType::Group => "group",
            ChatType::Supergroup => "supergroup",
            ChatType::Channel => "channel",
        };
        write!(f, "{}", s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chat_serialization() {
        let chat = Chat {
            id: 123456789,
            chat_type: ChatType::Private,
            title: None,
            username: Some("rust_expert".to_string()),
            first_name: Some("Principal".to_string()),
            last_name: Some("Engineer".to_string()),
        };

        let json = serde_json::to_string(&chat).unwrap();
        assert!(json.contains(r#""type":"private""#));
        assert!(json.contains(r#""username":"rust_expert""#));
        assert!(!json.contains(r#""title""#));
    }

    #[test]
    fn test_chat_deserialization() {
        let json = r#"{
            "id": 987654321,
            "type": "supergroup",
            "title": "Rust Systems Architecture"
        }"#;

        let chat: Chat = serde_json::from_str(json).unwrap();
        assert_eq!(chat.id, 987654321);
        assert_eq!(chat.chat_type, ChatType::Supergroup);
        assert_eq!(chat.title, Some("Rust Systems Architecture".to_string()));
        assert_eq!(chat.username, None);
    }
}