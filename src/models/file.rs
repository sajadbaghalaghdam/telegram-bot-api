use serde::{Deserialize, Serialize};

/// Represents a file ready to be downloaded.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct File {
    pub file_id: String,
    pub file_unique_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_path: Option<String>,
}
