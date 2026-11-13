use serde::{Deserialize, Serialize};

/// Represents a User as defined by the Telegram Bot API.
/// 
/// A representation of a User.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct User {
    /// Unique identifier for this user.
    pub id: i64,

    /// True if the user is a bot.
    pub is_bot: bool,

    /// First name of the user.
    pub first_name: String,

    /// Last name of the user.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_name: Option<String>,

    /// User name, without the leading '@'.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,

    /// User's language code with all lower case from 'iu47' (e.g., 'en').
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language_code: Option<String>,

    /// True if the user can join groups.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub can_join_groups: Option<bool>,

    /// True if the user can read all group messages.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub can_read_all_group_messages: Option<bool>,

    /// True if the user supports inline queries.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supports_inline_queries: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_user_serialization() {
        let user = User {
            id: 12345678,
            is_bot: false,
            first_name: "John".to_string(),
            last_name: Some("Doe".to_string()),
            username: Some("johndoe".to_string()),
            language_code: Some("en".to_string()),
            can_join_groups: Some(true),
            can_read_all_group_messages: Some(false),
            supports_inline_queries: Some(true),
        };

        let serialized = serde_json::to_value(&user).unwrap();
        let expected = json!({
            "id": 12345678,
            "is_bot": false,
            "first_name": "John",
            "last_name": "Doe",
            "username": "johndoe",
            "language_code": "en",
            "can_join_groups": true,
            "can_read_all_group_messages": false,
            "supports_inline_queries": true
        });

        assert_eq!(serialized, expected);
    }

    #[test]
    fn test_user_optional_fields_skipped() {
        let user = User {
            id: 123,
            is_bot: false,
            first_name: "Minimal".to_string(),
            last_name: None,
            username: None,
            language_code: None,
            can_join_groups: None,
            can_read_all_group_messages: None,
            supports_inline_queries: None,
        };
        let serialized = serde_json::to_value(&user).unwrap();
        
        let expected = json!({
            "id": 123,
            "is_bot": false,
            "first_name": "Minimal"
        });

        assert_eq!(serialized, expected);
    }

    #[test]
    fn test_user_deserialization() {
        let data = r#"{
            "id": 987654,
            "is_bot": true,
            "first_name": "Botty",
            "username": "botty_bot"
        }"#;

        let user: User = serde_json::from_str(data).unwrap();
        assert_eq!(user.id, 987654);
        assert!(user.is_bot);
        assert_eq!(user.first_name, "Botty");
        assert_eq!(user.username, Some("botty_bot".to_string()));
        assert!(user.last_name.is_none());
    }
}