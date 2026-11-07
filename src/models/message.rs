use serde::{Deserialize, Serialize};
use std::fmt;
use super::{Chat, User};
use super::file::{Animation, Audio, Document, PhotoSize, Video, Voice};

/// Represents a Message object as defined by the official Telegram Bot API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// Unique message identifier inside this chat
    pub message_id: i32,
    
    /// Date the message was sent in Unix time
    pub date: i32,
    
    /// Conversation the message belongs to
    pub chat: Chat,
    
    /// Sender of the message; empty for messages sent to channels
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<User>,
    
    /// For text messages, the actual UTF-8 text of the message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,

    /// Message is a photo, available sizes of the photo
    #[serde(skip_serializing_if = "Option::is_none")]
    pub photo: Option<Vec<PhotoSize>>,

    /// Message is a general file, information about the file
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document: Option<Document>,

    /// Message is an audio file, information about the file
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<Audio>,

    /// Message is a video, information about the video
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video: Option<Video>,

    /// Message is an animation, information about the animation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub animation: Option<Animation>,

    /// Message is a voice message, information about the file
    #[serde(skip_serializing_if = "Option::is_none")]
    pub voice: Option<Voice>,

    /// Caption for the animation, audio, document, photo, video or voice
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caption: Option<String>,

    /// For replies, the original message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_to_message: Option<Box<Message>>,

    /// Inline keyboard attached to the message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_markup: Option<serde_json::Value>,
}

/// Telegram's supported parse modes for text formatting.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ParseMode {
    #[serde(rename = "MarkdownV2")]
    MarkdownV2,
    #[serde(rename = "Markdown")]
    Markdown,
    #[serde(rename = "HTML")]
    Html,
}

/// Telegram's flexible chat identifier, which can be an integer or a username string.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ChatId {
    Integer(i64),
    Username(String),
}

impl ChatId {
    pub fn to_i64(&self) -> Result<i64, std::num::ParseIntError> {
        match self {
            ChatId::Integer(i) => Ok(*i),
            ChatId::Username(s) => s.parse::<i64>(),
        }
    }
}

impl fmt::Display for ChatId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChatId::Integer(i) => write!(f, "{}", i),
            ChatId::Username(s) => write!(f, "{}", s),
        }
    }
}

/// Request payload for the `sendMessage` method matching all official options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendMessageRequest {
    /// Unique identifier for the target chat or username of the target channel
    pub chat_id: ChatId,

    /// Unique identifier for the target message thread (topic) of the forum
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_thread_id: Option<i32>,
    
    /// Text of the message to be sent, 1-4096 characters
    pub text: String,
    
    /// Mode for parsing entities in the message text
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parse_mode: Option<String>,

    /// A JSON-serialized list of special entities that appear in message text
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entities: Option<Vec<serde_json::Value>>,

    /// Link preview generation options for the message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link_preview_options: Option<serde_json::Value>,
    
    /// Disables link previews for links in this message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disable_web_page_preview: Option<bool>,

    /// Sends the message silently. Users will receive a notification with no sound.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disable_notification: Option<bool>,

    /// Protects the contents of the sent message from forwarding and saving
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protect_content: Option<bool>,

    /// Description of the message to reply to
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_parameters: Option<serde_json::Value>,
    
    /// If the message is a reply, ID of the original message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_to_message_id: Option<i32>,

    /// Additional interface options (InlineKeyboardMarkup, ReplyKeyboardMarkup, etc.)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_markup: Option<serde_json::Value>,
}

/// Request payload for the `editMessageText` method.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EditMessageTextRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chat_id: Option<ChatId>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub inline_message_id: Option<String>,

    pub text: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub parse_mode: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub entities: Option<Vec<serde_json::Value>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub link_preview_options: Option<serde_json::Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_markup: Option<serde_json::Value>,
}

/// Request payload for the `deleteMessage` method.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteMessageRequest {
    pub chat_id: ChatId,
    pub message_id: i32,
}
