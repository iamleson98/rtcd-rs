use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::JoinRequest;

/// The properties a session was started with, kept as the open map the Go service exposes over
/// its HTTP API. Absent properties are `null`, as `SessionConfig.FromMap` leaves them.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(transparent)]
pub struct SessionProps(BTreeMap<String, Value>);

impl<'de> Deserialize<'de> for SessionProps {
    /// A session without properties is `null` in Go's JSON, not an empty object.
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(Self(
            Option::<BTreeMap<String, Value>>::deserialize(d)?.unwrap_or_default(),
        ))
    }
}

impl SessionProps {
    pub fn channel_id(&self) -> &str {
        self.0
            .get("channelID")
            .and_then(Value::as_str)
            .unwrap_or_default()
    }

    pub fn av1_support(&self) -> bool {
        self.0
            .get("av1Support")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }

    pub fn dc_signaling(&self) -> bool {
        self.0
            .get("dcSignaling")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }

    /// Whether the participant is the call's host (absent from older servers: false).
    pub fn host(&self) -> bool {
        self.0.get("host").and_then(Value::as_bool).unwrap_or(false)
    }
}

/// A session, as the join request defined it and the HTTP API reports it. The field names are the
/// Go struct's, which has no JSON tags.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SessionConfig {
    #[serde(rename = "GroupID")]
    pub group_id: String,
    #[serde(rename = "CallID")]
    pub call_id: String,
    #[serde(rename = "UserID")]
    pub user_id: String,
    #[serde(rename = "SessionID")]
    pub session_id: String,
    pub props: SessionProps,
}

impl SessionConfig {
    /// The session a join request asks for. The group is the authenticated client, never taken
    /// from the request.
    pub fn from_join(group_id: &str, join: &JoinRequest) -> Self {
        let mut props = BTreeMap::new();
        props.insert(
            "channelID".to_owned(),
            Value::String(join.channel_id.clone()),
        );
        props.insert("av1Support".to_owned(), Value::Bool(join.av1_support));
        props.insert("dcSignaling".to_owned(), Value::Bool(join.dc_signaling));
        props.insert("host".to_owned(), Value::Bool(join.host));
        Self {
            group_id: group_id.to_owned(),
            call_id: join.call_id.clone(),
            user_id: join.user_id.clone(),
            session_id: join.session_id.clone(),
            props: SessionProps(props),
        }
    }

    /// Whether every identifier is present.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.group_id.is_empty() {
            return Err("GroupID should not be empty");
        }
        if self.call_id.is_empty() {
            return Err("CallID should not be empty");
        }
        if self.user_id.is_empty() {
            return Err("UserID should not be empty");
        }
        if self.session_id.is_empty() {
            return Err("SessionID should not be empty");
        }
        Ok(())
    }
}
