//! Short-lived TURN credentials in the form coturn's `use-auth-secret` mode checks (TURN REST API):
//! the username is `<expiry>:<name>` and the password the base64 HMAC-SHA1 of the username under
//! the shared secret.

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, KeyInit, Mac};
use sha1::Sha1;

use crate::ProtoError;

/// The longest a credential may be valid, in minutes (one week).
pub const MAX_EXPIRATION_MINUTES: i64 = 7 * 24 * 60;

/// A username/password pair for a TURN server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnCredentials {
    pub username: String,
    pub password: String,
}

/// Credentials for `name`, valid until `expires_at` (unix seconds), given `now` (unix seconds).
pub fn credentials(
    name: &str,
    secret: &str,
    expires_at: i64,
    now: i64,
) -> Result<TurnCredentials, ProtoError> {
    if name.is_empty() {
        return Err(ProtoError::Credentials("name should not be empty"));
    }
    if secret.is_empty() {
        return Err(ProtoError::Credentials("secret should not be empty"));
    }
    if expires_at <= 0 {
        return Err(ProtoError::Credentials(
            "expiry should be a positive number",
        ));
    }
    if expires_at > now + MAX_EXPIRATION_MINUTES * 60 {
        return Err(ProtoError::Credentials(
            "expiry cannot be more than a week away",
        ));
    }
    let username = format!("{expires_at}:{name}");
    Ok(TurnCredentials {
        password: password_for(&username, secret),
        username,
    })
}

/// The password the shared secret derives for a username.
pub fn password_for(username: &str, secret: &str) -> String {
    let mut mac = <Hmac<Sha1> as KeyInit>::new_from_slice(secret.as_bytes())
        .expect("hmac takes any key length");
    mac.update(username.as_bytes());
    STANDARD.encode(mac.finalize().into_bytes())
}
