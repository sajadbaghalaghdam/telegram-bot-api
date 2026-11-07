use std::collections::HashMap;

/// Represents an incoming HTTP query matching official C++ Query.h/Query.cpp
#[derive(Debug, Clone)]
pub struct Query {
    pub token: String,
    pub method: String,
    pub headers: HashMap<String, String>,
    pub parameters: HashMap<String, serde_json::Value>,
}

impl Query {
    pub fn new(token: String, method: String) -> Self {
        Self {
            token,
            method,
            headers: HashMap::new(),
            parameters: HashMap::new(),
        }
    }

    pub fn clean_token(&self) -> &str {
        self.token.strip_prefix("bot").unwrap_or(&self.token)
    }
}
