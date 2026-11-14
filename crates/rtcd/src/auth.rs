//! Registration and authentication of the clients (Mattermost installations) allowed to use the
//! service: bcrypt hashed keys in the store, basic or bearer authentication, and the limits that
//! keep password hashing from starving the media threads: one hash at a time, logins with the
//! same key sharing one check, keys that already passed remembered for a few minutes, and a source
//! that keeps presenting wrong keys turned away without hashing.

use std::collections::HashMap;
use std::hash::Hash;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rand::RngExt;
use subtle::ConstantTimeEq;
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore};

use crate::store::{Store, StoreError};

/// The shortest key a client may register with.
pub const MIN_KEY_LEN: usize = 32;

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

/// How long a verified key is served from memory instead of being run through bcrypt again.
const VERIFIED_TTL: Duration = Duration::from_secs(5 * 60);
/// The most clients whose verified keys are remembered.
const VERIFIED_MAX: usize = 1024;

/// How many wrong keys a source may present for one client before it is turned away.
const MAX_FAILURES: u32 = 5;
/// How long a source that reached `MAX_FAILURES` is turned away, and how long a failure counts.
const LOCKOUT: Duration = Duration::from_secs(60);
/// The most (client, source) pairs whose failures are remembered.
const FAILURES_MAX: usize = 4096;

/// Makes room for one more entry in a map capped at `max`: expired entries go first, then the one
/// expiring soonest.
fn make_room<K: Clone + Eq + Hash, V>(
    entries: &mut HashMap<K, V>,
    max: usize,
    now: Instant,
    expires: impl Fn(&V) -> Instant,
) {
    if entries.len() < max {
        return;
    }
    entries.retain(|_, v| now < expires(v));
    if entries.len() >= max
        && let Some(oldest) = entries
            .iter()
            .min_by_key(|(_, v)| expires(v))
            .map(|(k, _)| k.clone())
    {
        entries.remove(&oldest);
    }
}

/// SHA-256 of a presented key.
type KeyDigest = [u8; 32];

fn key_digest(key: &str) -> KeyDigest {
    let mut out = [0u8; 32];
    out.copy_from_slice(ring::digest::digest(&ring::digest::SHA256, key.as_bytes()).as_ref());
    out
}

#[derive(Debug)]
struct Verified {
    /// The stored hash the key was verified against.
    hash: String,
    digest: KeyDigest,
    expires: Instant,
}

/// Keys that passed bcrypt, by client. A login storm of one client's valid key would otherwise
/// spend a bcrypt, hundreds of milliseconds of CPU, on every connection. Only the digest of a key
/// is kept, and only while the client's stored hash is the one it was verified against, so
/// rotating or removing a key ends it at once.
#[derive(Debug)]
struct VerifiedKeys {
    ttl: Duration,
    entries: Mutex<HashMap<String, Verified>>,
}

impl VerifiedKeys {
    fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Whether this key was verified against `hash` for the client and has not expired.
    fn contains(&self, id: &str, hash: &str, digest: &KeyDigest, now: Instant) -> bool {
        let mut entries = self.entries.lock();
        let Some(v) = entries.get(id) else {
            return false;
        };
        if now >= v.expires || v.hash != hash {
            entries.remove(id);
            return false;
        }
        bool::from(v.digest.ct_eq(digest))
    }

    fn insert(&self, id: &str, hash: &str, digest: KeyDigest, now: Instant) {
        let mut entries = self.entries.lock();
        if !entries.contains_key(id) {
            make_room(&mut entries, VERIFIED_MAX, now, |v| v.expires);
        }
        entries.insert(
            id.to_owned(),
            Verified {
                hash: hash.to_owned(),
                digest,
                expires: now + self.ttl,
            },
        );
    }

    fn remove(&self, id: &str) {
        self.entries.lock().remove(id);
    }
}

#[derive(Debug)]
struct Failing {
    count: u32,
    expires: Instant,
}

/// Wrong keys presented for a client by a source address. Every wrong key costs a bcrypt, so a
/// source that presents `MAX_FAILURES` of them in a row is refused, without hashing, until
/// `LOCKOUT` after its last failure; a right key clears the count.
#[derive(Debug, Default)]
struct Failures {
    entries: Mutex<HashMap<(String, IpAddr), Failing>>,
}

impl Failures {
    fn locked(&self, id: &str, from: IpAddr, now: Instant) -> bool {
        let mut entries = self.entries.lock();
        let key = (id.to_owned(), from);
        let Some(f) = entries.get(&key) else {
            return false;
        };
        if now >= f.expires {
            entries.remove(&key);
            return false;
        }
        f.count >= MAX_FAILURES
    }

    fn record(&self, id: &str, from: IpAddr, now: Instant) {
        let mut entries = self.entries.lock();
        let key = (id.to_owned(), from);
        if !entries.contains_key(&key) {
            make_room(&mut entries, FAILURES_MAX, now, |f| f.expires);
        }
        let f = entries.entry(key).or_insert(Failing {
            count: 0,
            expires: now + LOCKOUT,
        });
        if now >= f.expires {
            f.count = 0;
        }
        if f.count < MAX_FAILURES {
            f.count += 1;
            f.expires = now + LOCKOUT;
        }
    }

    fn clear(&self, id: &str, from: IpAddr) {
        self.entries.lock().remove(&(id.to_owned(), from));
    }
}

/// A client, the stored hash and the digest of the key being checked.
type FlightKey = (String, String, KeyDigest);
type Flights = Mutex<HashMap<FlightKey, Arc<OnceCell<bool>>>>;

/// The check of one key that concurrent logins share: whoever comes first runs it, the others
/// wait for its answer. Leaving the flight ends it, so a later login starts a new check.
struct Flight<'a> {
    flights: &'a Flights,
    key: FlightKey,
    check: Arc<OnceCell<bool>>,
}

impl<'a> Flight<'a> {
    fn join(flights: &'a Flights, key: FlightKey) -> Self {
        let check = Arc::clone(flights.lock().entry(key.clone()).or_default());
        Self {
            flights,
            key,
            check,
        }
    }
}

impl Drop for Flight<'_> {
    fn drop(&mut self) {
        let mut flights = self.flights.lock();
        if flights
            .get(&self.key)
            .is_some_and(|c| Arc::ptr_eq(c, &self.check))
        {
            flights.remove(&self.key);
        }
    }
}

/// The credentials service.
pub struct AuthService {
    store: Arc<dyn Store>,
    sessions: Arc<SessionCache>,
    /// One permit: at most one core is ever spent on password hashing.
    hashing: Arc<Semaphore>,
    verified: Arc<VerifiedKeys>,
    flights: Flights,
    failures: Failures,
    /// bcrypt work factor; the library default in production.
    cost: u32,
    /// How many bcrypt verifications ran.
    #[cfg(test)]
    verifications: std::sync::atomic::AtomicUsize,
}

impl std::fmt::Debug for AuthService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AuthService")
    }
}

impl AuthService {
    pub fn new(store: Arc<dyn Store>, sessions: Arc<SessionCache>) -> Self {
        Self {
            store,
            sessions,
            hashing: Arc::new(Semaphore::new(1)),
            verified: Arc::new(VerifiedKeys::new(VERIFIED_TTL)),
            flights: Mutex::new(HashMap::new()),
            failures: Failures::default(),
            cost: bcrypt::DEFAULT_COST,
            #[cfg(test)]
            verifications: std::sync::atomic::AtomicUsize::new(0),
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

    /// The turn to hash a password. There is no deadline: a login that waits is one whose client
    /// has not given up, and when it does the connection drops this future. The permit travels
    /// into the blocking job, so a caller that gives up does not free the core early.
    async fn hashing_turn(&self) -> Result<OwnedSemaphorePermit, AuthError> {
        Arc::clone(&self.hashing)
            .acquire_owned()
            .await
            .map_err(|e| err(e.to_string()))
    }

    /// Checks a client's key, presented from `from`, against its stored hash. A key that passed
    /// recently is not hashed again, logins presenting the same key at once share one check, and
    /// a source that keeps presenting wrong keys is refused without any.
    pub async fn authenticate(&self, id: &str, key: &str, from: IpAddr) -> Result<(), AuthError> {
        let hash = self
            .store
            .get(id)
            .map_err(|e| err(format!("authentication failed: {e}")))?;
        if hash.is_empty() || key.is_empty() {
            return Err(err("authentication failed"));
        }
        let digest = key_digest(key);
        if self.verified.contains(id, &hash, &digest, Instant::now()) {
            self.failures.clear(id, from);
            return Ok(());
        }
        if self.failures.locked(id, from, Instant::now()) {
            return Err(err("authentication failed: too many failed attempts"));
        }
        let flight = Flight::join(&self.flights, (id.to_owned(), hash.clone(), digest));
        let ok = *flight
            .check
            .get_or_init(|| self.verify(id, &hash, key, digest))
            .await;
        if ok {
            self.failures.clear(id, from);
            Ok(())
        } else {
            self.failures.record(id, from, Instant::now());
            Err(err("authentication failed"))
        }
    }

    /// Runs bcrypt for a key, in its turn.
    async fn verify(&self, id: &str, hash: &str, key: &str, digest: KeyDigest) -> bool {
        let Ok(permit) = self.hashing_turn().await else {
            return false;
        };
        // A check that finished while this one waited for its turn leaves the key remembered.
        if self.verified.contains(id, hash, &digest, Instant::now()) {
            return true;
        }
        #[cfg(test)]
        self.verifications
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (key, stored, id) = (key.to_owned(), hash.to_owned(), id.to_owned());
        let verified = Arc::clone(&self.verified);
        // The key is remembered before the turn is given up, so whoever waited behind this check
        // finds it.
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let ok = bcrypt::verify(key, &stored).unwrap_or(false);
            if ok {
                verified.insert(&id, &stored, digest, Instant::now());
            }
            ok
        })
        .await
        .unwrap_or(false)
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
        let permit = self
            .hashing_turn()
            .await
            .map_err(|e| err(format!("registration failed: {e}")))?;
        let (key, cost) = (key.to_owned(), self.cost);
        let hash = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            bcrypt::hash(key, cost)
        })
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
        self.verified.remove(id);
        self.sessions.delete_client(id);
        Ok(())
    }

    /// Authenticates a client and issues it a bearer token.
    pub async fn login(&self, id: &str, key: &str, from: IpAddr) -> Result<String, AuthError> {
        self.authenticate(id, key, from)
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
    const LOCAL: IpAddr = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
    const OTHER: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(192, 0, 2, 7));

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
        a.authenticate("client", KEY, LOCAL).await.unwrap();
        assert_eq!(
            a.authenticate("client", "wrong-key-wrong-key-wrong-key-xx", LOCAL)
                .await
                .unwrap_err()
                .to_string(),
            "authentication failed"
        );
        assert!(
            a.authenticate("nobody", KEY, LOCAL)
                .await
                .unwrap_err()
                .to_string()
                .starts_with("authentication failed")
        );
        assert!(a.authenticate("client", "", LOCAL).await.is_err());
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
        a.authenticate("go-client", KEY, LOCAL).await.unwrap();
    }

    #[tokio::test]
    async fn unregistering_forgets_the_client_and_its_token() {
        let a = service();
        a.register("c", KEY).await.unwrap();
        let token = a.login("c", KEY, LOCAL).await.unwrap();
        assert_eq!(a.sessions().get(&token).unwrap(), "c");
        a.unregister("c").unwrap();
        assert!(a.sessions().get(&token).is_err());
        assert!(a.authenticate("c", KEY, LOCAL).await.is_err());
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
        let first = a.login("c", KEY, LOCAL).await.unwrap();
        assert_eq!(first.len(), 32);
        let second = a.login("c", KEY, LOCAL).await.unwrap();
        assert_ne!(first, second);
        assert!(a.sessions().get(&first).is_err(), "one token per client");
        assert_eq!(a.sessions().get(&second).unwrap(), "c");
        assert!(
            a.login("c", "wrong-key-wrong-key-wrong-key-xx", LOCAL)
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

    fn verifications(a: &AuthService) -> usize {
        a.verifications.load(std::sync::atomic::Ordering::Relaxed)
    }

    #[tokio::test]
    async fn a_valid_key_is_hashed_once_however_often_it_is_presented() {
        let a = Arc::new(service());
        a.register("c", KEY).await.unwrap();
        let tasks: Vec<_> = (0..100)
            .map(|_| {
                let a = Arc::clone(&a);
                tokio::spawn(async move { a.authenticate("c", KEY, LOCAL).await })
            })
            .collect();
        for t in tasks {
            t.await.unwrap().unwrap();
        }
        assert_eq!(verifications(&a), 1);
    }

    #[tokio::test]
    async fn a_wrong_key_is_never_served_from_the_cache() {
        let a = service();
        a.register("c", KEY).await.unwrap();
        a.authenticate("c", KEY, LOCAL).await.unwrap();
        for _ in 0..3 {
            assert!(
                a.authenticate("c", "wrong-key-wrong-key-wrong-key-xx", LOCAL)
                    .await
                    .is_err()
            );
        }
        assert_eq!(verifications(&a), 4, "each wrong key paid for its bcrypt");
        a.authenticate("c", KEY, LOCAL).await.unwrap();
        assert_eq!(verifications(&a), 4, "and did not disturb the cached key");
    }

    #[tokio::test]
    async fn a_changed_stored_hash_ends_the_cached_key() {
        let store = Arc::new(MemoryStore::default());
        let a = AuthService::new(store.clone(), Arc::new(SessionCache::new(60))).with_cost(4);
        a.register("c", KEY).await.unwrap();
        a.authenticate("c", KEY, LOCAL).await.unwrap();
        let rotated = bcrypt::hash("another-key-another-key-another-k", 4).unwrap();
        store.delete("c").unwrap();
        store.put("c", &rotated).unwrap();
        assert!(a.authenticate("c", KEY, LOCAL).await.is_err());
        assert_eq!(verifications(&a), 2);
    }

    #[test]
    fn verified_keys_expire_and_stay_bounded() {
        let now = Instant::now();
        let keys = VerifiedKeys::new(Duration::from_secs(60));
        let digest = key_digest(KEY);
        keys.insert("c", "hash", digest, now);
        assert!(keys.contains("c", "hash", &digest, now + Duration::from_secs(59)));
        assert!(!keys.contains("c", "hash", &key_digest("other"), now));
        assert!(!keys.contains("c", "hash", &digest, now + Duration::from_secs(60)));
        assert!(
            keys.entries.lock().is_empty(),
            "an expired entry is dropped"
        );

        for i in 0..VERIFIED_MAX + 10 {
            keys.insert(
                &format!("c{i}"),
                "hash",
                digest,
                now + Duration::from_millis(i as u64),
            );
        }
        assert_eq!(keys.entries.lock().len(), VERIFIED_MAX);
        assert!(
            !keys.contains("c0", "hash", &digest, now),
            "the oldest went first"
        );
    }

    #[tokio::test]
    async fn logins_with_one_wrong_key_at_once_share_one_check() {
        let a = Arc::new(service());
        a.register("c", KEY).await.unwrap();
        let tasks: Vec<_> = (0..20)
            .map(|_| {
                let a = Arc::clone(&a);
                tokio::spawn(async move {
                    a.authenticate("c", "wrong-key-wrong-key-wrong-key-xx", OTHER)
                        .await
                })
            })
            .collect();
        for t in tasks {
            assert!(t.await.unwrap().is_err());
        }
        assert_eq!(verifications(&a), 1);
    }

    #[tokio::test]
    async fn a_source_with_five_wrong_keys_is_refused_without_hashing() {
        let a = service();
        a.register("c", KEY).await.unwrap();
        for i in 0..MAX_FAILURES {
            let wrong = format!("wrong-key-{i}-wrong-key-wrong-key-xx");
            assert_eq!(
                a.authenticate("c", &wrong, LOCAL)
                    .await
                    .unwrap_err()
                    .to_string(),
                "authentication failed"
            );
        }
        assert_eq!(verifications(&a), MAX_FAILURES as usize);
        let refused = a.authenticate("c", KEY, LOCAL).await.unwrap_err();
        assert_eq!(
            refused.to_string(),
            "authentication failed: too many failed attempts"
        );
        assert!(
            a.authenticate("c", "another-wrong-key-xx", LOCAL)
                .await
                .is_err()
        );
        assert_eq!(verifications(&a), MAX_FAILURES as usize, "none hashed");
        a.authenticate("c", KEY, OTHER).await.unwrap();
        assert_eq!(
            verifications(&a),
            MAX_FAILURES as usize + 1,
            "another source is unaffected"
        );
    }

    #[tokio::test]
    async fn a_right_key_clears_the_failures_of_its_source() {
        let a = service();
        a.register("c", KEY).await.unwrap();
        for round in 0..2 {
            for i in 0..MAX_FAILURES - 1 {
                let wrong = format!("wrong-key-{round}-{i}-wrong-key-xx");
                assert_eq!(
                    a.authenticate("c", &wrong, LOCAL)
                        .await
                        .unwrap_err()
                        .to_string(),
                    "authentication failed"
                );
            }
            a.authenticate("c", KEY, LOCAL).await.unwrap();
        }
    }

    #[test]
    fn failures_expire_and_stay_bounded() {
        let now = Instant::now();
        let f = Failures::default();
        for _ in 0..MAX_FAILURES {
            f.record("c", LOCAL, now);
        }
        assert!(f.locked("c", LOCAL, now + LOCKOUT - Duration::from_secs(1)));
        assert!(!f.locked("c", OTHER, now));
        assert!(!f.locked("d", LOCAL, now));
        assert!(!f.locked("c", LOCAL, now + LOCKOUT));
        assert!(f.entries.lock().is_empty(), "an expired entry is dropped");

        f.record("c", LOCAL, now);
        f.record("c", LOCAL, now + LOCKOUT);
        assert_eq!(f.entries.lock()[&("c".to_owned(), LOCAL)].count, 1);

        for i in 0..FAILURES_MAX + 10 {
            f.record(
                &format!("c{i}"),
                LOCAL,
                now + Duration::from_millis(i as u64),
            );
        }
        assert_eq!(f.entries.lock().len(), FAILURES_MAX);
    }
}
