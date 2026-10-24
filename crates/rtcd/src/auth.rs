//! Registration and authentication of the clients (Mattermost installations) allowed to use the
//! service: bcrypt hashed keys in the store, basic or bearer authentication, and the rate limit
//! that keeps password hashing from starving the media threads.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rand::RngExt;

use crate::store::{Store, StoreError};

/// The shortest key a client may register with.
pub const MIN_KEY_LEN: usize = 32;
const AUTH_TIMEOUT: Duration = Duration::from_secs(10);
/// Authentications (each a bcrypt) per second per CPU.
const REQUESTS_PER_SECOND_PER_CPU: u32 = 12;

/// Why an authentication step failed. The text is what clients see.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct AuthError(pub String);

fn err(s: impl Into<String>) -> AuthError {
    AuthError(s.into())
}

/// One token per client, each valid for a while.
#[derive(Debug)]
pub struct SessionCache {
    ttl: Duration,
    tokens: Mutex<HashMap<String, (String, Instant)>>,
}

impl SessionCache {
    pub fn new(expiration_minutes: u64) -> Self {
        Self {
            ttl: Duration::from_secs(expiration_minutes * 60),
            tokens: Mutex::new(HashMap::new()),
        }
    }

    /// The client a live token belongs to.
    pub fn get(&self, token: &str) -> Result<String, AuthError> {
        let mut t = self.tokens.lock();
        match t.get(token) {
            None => Err(err("token is invalid")),
            Some((_, expires)) if Instant::now() >= *expires => {
                t.remove(token);
                Err(err("session is expired"))
            }
            Some((client, _)) => Ok(client.clone()),
        }
    }

    /// Issues `token` to a client, replacing the one it had.
    pub fn put(&self, client: &str, token: &str) -> Result<(), AuthError> {
        if client.is_empty() {
            return Err(err("can not cache: invalid client id"));
        }
        if token.is_empty() {
            return Err(err("can not cache: invalid token"));
        }
        let mut t = self.tokens.lock();
        if t.contains_key(token) {
            return Err(err("can not cache: token in use"));
        }
        t.retain(|_, (c, _)| c != client);
        t.insert(
            token.to_owned(),
            (client.to_owned(), Instant::now() + self.ttl),
        );
        Ok(())
    }

    pub fn delete_client(&self, client: &str) {
        self.tokens.lock().retain(|_, (c, _)| c != client);
    }
}

/// Spaces calls out to a fixed rate, making callers wait their turn.
#[derive(Debug)]
struct Limiter {
    interval: Duration,
    next: tokio::sync::Mutex<Instant>,
}

impl Limiter {
    fn new(per_second: u32) -> Self {
        Self {
            interval: Duration::from_secs(1) / per_second.max(1),
            next: tokio::sync::Mutex::new(Instant::now()),
        }
    }

    /// Waits for a turn; fails if it would be further away than `timeout`.
    async fn wait(&self, timeout: Duration) -> Result<(), AuthError> {
        let slot = {
            let mut next = self.next.lock().await;
            let slot = (*next).max(Instant::now());
            if slot.saturating_duration_since(Instant::now()) > timeout {
                return Err(err("rate: Wait(n=1) would exceed context deadline"));
            }
            *next = slot + self.interval;
            slot
        };
        tokio::time::sleep_until(slot.into()).await;
        Ok(())
    }
}

/// The credentials service.
pub struct AuthService {
    store: Arc<dyn Store>,
    sessions: Arc<SessionCache>,
    limiter: Limiter,
    /// bcrypt work factor; the library default (10) in production.
    cost: u32,
}

impl std::fmt::Debug for AuthService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AuthService")
    }
}

impl AuthService {
    pub fn new(store: Arc<dyn Store>, sessions: Arc<SessionCache>) -> Self {
        let cpus = std::thread::available_parallelism().map_or(1, usize::from) as u32;
        Self {
            store,
            sessions,
            limiter: Limiter::new(REQUESTS_PER_SECOND_PER_CPU * cpus),
            cost: bcrypt::DEFAULT_COST,
        }
    }

    /// Lowers the bcrypt cost; for tests, which register dozens of clients.
    pub fn with_cost(mut self, cost: u32) -> Self {
        self.cost = cost;
        self
    }

    pub fn sessions(&self) -> &SessionCache {
        &self.sessions
    }

    /// Checks a client's key against its stored hash.
    pub async fn authenticate(&self, id: &str, key: &str) -> Result<(), AuthError> {
        let hash = self
            .store
            .get(id)
            .map_err(|e| err(format!("authentication failed: {e}")))?;
        self.limiter
            .wait(AUTH_TIMEOUT)
            .await
            .map_err(|e| err(format!("authentication failed: {e}")))?;
        if hash.is_empty() || key.is_empty() {
            return Err(err("authentication failed"));
        }
        let key = key.to_owned();
        let ok = tokio::task::spawn_blocking(move || bcrypt::verify(key, &hash).unwrap_or(false))
            .await
            .unwrap_or(false);
        if ok {
            Ok(())
        } else {
            Err(err("authentication failed"))
        }
    }

    pub async fn register(&self, id: &str, key: &str) -> Result<(), AuthError> {
        if key.len() < MIN_KEY_LEN {
            return Err(err("registration failed: key not long enough"));
        }
        match self.store.get(id) {
            Ok(_) => return Err(err("registration failed: already registered")),
            Err(StoreError::NotFound) => {}
            Err(e) => return Err(err(format!("registration failed: {e}"))),
        }
        self.limiter
            .wait(AUTH_TIMEOUT)
            .await
            .map_err(|e| err(format!("registration failed: {e}")))?;
        let (key, cost) = (key.to_owned(), self.cost);
        let hash = tokio::task::spawn_blocking(move || bcrypt::hash(key, cost))
            .await
            .map_err(|e| err(format!("registration failed: {e}")))?
            .map_err(|e| err(format!("registration failed: {e}")))?;
        match self.store.put(id, &hash) {
            Ok(()) => Ok(()),
            Err(StoreError::Conflict) => Err(err("registration failed: already registered")),
            Err(e) => Err(err(format!("registration failed: {e}"))),
        }
    }

    pub fn unregister(&self, id: &str) -> Result<(), AuthError> {
        self.store
            .get(id)
            .map_err(|e| err(format!("unregister failed: {e}")))?;
        self.store
            .delete(id)
            .map_err(|e| err(format!("unregister failed: {e}")))?;
        self.sessions.delete_client(id);
        Ok(())
    }

    /// Authenticates a client and issues it a bearer token.
    pub async fn login(&self, id: &str, key: &str) -> Result<String, AuthError> {
        self.authenticate(id, key)
            .await
            .map_err(|e| err(format!("login failed: {e}")))?;
        let token = random_token();
        self.sessions
            .put(id, &token)
            .map_err(|e| err(format!("login failed: {e}")))?;
        Ok(token)
    }
}

/// A 32 character URL-safe random token.
fn random_token() -> String {
    use base64::Engine;
    let mut bytes = [0u8; 24];
    rand::rng().fill(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)[..MIN_KEY_LEN].to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;

    const KEY: &str = "0123456789abcdef0123456789abcdef";

    fn service() -> AuthService {
        AuthService::new(
            Arc::new(MemoryStore::default()),
            Arc::new(SessionCache::new(60)),
        )
        .with_cost(4)
    }

    #[tokio::test]
    async fn a_registered_client_authenticates_with_its_key_only() {
        let a = service();
        a.register("client", KEY).await.unwrap();
        a.authenticate("client", KEY).await.unwrap();
        assert_eq!(
            a.authenticate("client", "wrong-key-wrong-key-wrong-key-xx")
                .await
                .unwrap_err()
                .to_string(),
            "authentication failed"
        );
        assert!(
            a.authenticate("nobody", KEY)
                .await
                .unwrap_err()
                .to_string()
                .starts_with("authentication failed")
        );
        assert!(a.authenticate("client", "").await.is_err());
    }

    #[tokio::test]
    async fn registration_has_the_rules_of_the_go_service() {
        let a = service();
        assert_eq!(
            a.register("c", "short").await.unwrap_err().to_string(),
            "registration failed: key not long enough"
        );
        a.register("c", KEY).await.unwrap();
        assert_eq!(
            a.register("c", KEY).await.unwrap_err().to_string(),
            "registration failed: already registered"
        );
    }

    #[tokio::test]
    async fn a_hash_made_by_the_go_service_verifies() {
        // bcrypt.GenerateFromPassword at the default cost in Go; see compat/golden.
        let store = Arc::new(MemoryStore::default());
        let hash = bcrypt::hash(KEY, bcrypt::DEFAULT_COST).unwrap();
        store.put("go-client", &hash).unwrap();
        let a = AuthService::new(store, Arc::new(SessionCache::new(60)));
        a.authenticate("go-client", KEY).await.unwrap();
    }

    #[tokio::test]
    async fn unregistering_forgets_the_client_and_its_token() {
        let a = service();
        a.register("c", KEY).await.unwrap();
        let token = a.login("c", KEY).await.unwrap();
        assert_eq!(a.sessions().get(&token).unwrap(), "c");
        a.unregister("c").unwrap();
        assert!(a.sessions().get(&token).is_err());
        assert!(a.authenticate("c", KEY).await.is_err());
        assert!(
            a.unregister("c")
                .unwrap_err()
                .to_string()
                .starts_with("unregister failed")
        );
    }

    #[tokio::test]
    async fn a_login_issues_one_token_and_a_second_login_replaces_it() {
        let a = service();
        a.register("c", KEY).await.unwrap();
        let first = a.login("c", KEY).await.unwrap();
        assert_eq!(first.len(), 32);
        let second = a.login("c", KEY).await.unwrap();
        assert_ne!(first, second);
        assert!(a.sessions().get(&first).is_err(), "one token per client");
        assert_eq!(a.sessions().get(&second).unwrap(), "c");
        assert!(
            a.login("c", "wrong-key-wrong-key-wrong-key-xx")
                .await
                .unwrap_err()
                .to_string()
                .starts_with("login failed")
        );
    }

    #[test]
    fn tokens_expire() {
        let cache = SessionCache::new(0);
        cache.put("c", "tok").unwrap();
        assert_eq!(
            cache.get("tok").unwrap_err().to_string(),
            "session is expired"
        );
        assert_eq!(
            cache.get("tok").unwrap_err().to_string(),
            "token is invalid",
            "and are forgotten"
        );
        assert!(cache.put("", "t").is_err());
        assert!(cache.put("c", "").is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn the_limiter_spaces_callers_and_gives_up_on_long_queues() {
        let l = Limiter::new(10);
        let start = tokio::time::Instant::now();
        for _ in 0..5 {
            l.wait(Duration::from_secs(10)).await.unwrap();
        }
        assert!(
            start.elapsed() >= Duration::from_millis(400),
            "{:?}",
            start.elapsed()
        );
        // A queue longer than the timeout is refused rather than waited for.
        let l = Limiter::new(1);
        for _ in 0..4 {
            l.wait(Duration::from_secs(2)).await.unwrap_or(());
        }
        assert!(l.wait(Duration::from_millis(500)).await.is_err());
    }
}
