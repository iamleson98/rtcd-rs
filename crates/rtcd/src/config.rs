//! Configuration, read from a TOML file and overridden by `RTCD_*` environment variables, with
//! the same keys, defaults and checks as the Go rtcd so existing config files and deployments
//! carry over unchanged.

use std::collections::BTreeMap;
use std::path::Path;

use serde::Deserialize;

/// Why the configuration could not be used.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read the config file {path}: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to decode the config file: {0}")]
    Decode(#[from] toml::de::Error),
    #[error("{var}: {message}")]
    Env { var: String, message: String },
    #[error("invalid {field} value: {message}")]
    Invalid {
        field: &'static str,
        message: String,
    },
}

fn invalid(field: &'static str, message: impl Into<String>) -> ConfigError {
    ConfigError::Invalid {
        field,
        message: message.into(),
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct Config {
    pub api: ApiConfig,
    pub rtc: RtcConfig,
    pub store: StoreConfig,
    pub record: RecordConfig,
    pub logger: LoggerConfig,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct ApiConfig {
    pub http: HttpConfig,
    pub security: SecurityConfig,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct HttpConfig {
    pub listen_address: String,
    pub tls: TlsConfig,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct TlsConfig {
    pub enable: bool,
    pub cert_file: String,
    pub cert_key: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct SecurityConfig {
    pub enable_admin: bool,
    pub admin_secret_key: String,
    pub allow_self_registration: bool,
    pub session_cache: SessionCacheConfig,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct SessionCacheConfig {
    pub expiration_minutes: i64,
}

/// A STUN or TURN server, written either as a bare URL or as a table.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum IceServerEntry {
    Url(String),
    Full {
        #[serde(default)]
        urls: Vec<String>,
        #[serde(default)]
        username: String,
        #[serde(default)]
        credential: String,
    },
}

/// A STUN or TURN server.
#[derive(Debug, Clone, PartialEq)]
pub struct IceServer {
    pub urls: Vec<String>,
    pub username: String,
    pub credential: String,
}

impl From<IceServerEntry> for IceServer {
    fn from(e: IceServerEntry) -> Self {
        match e {
            IceServerEntry::Url(u) => Self {
                urls: vec![u],
                username: String::new(),
                credential: String::new(),
            },
            IceServerEntry::Full {
                urls,
                username,
                credential,
            } => Self {
                urls,
                username,
                credential,
            },
        }
    }
}

impl IceServer {
    pub fn is_stun(&self) -> bool {
        !self.urls.is_empty()
            && self
                .urls
                .iter()
                .all(|u| u.starts_with("stun:") || u.starts_with("stuns:"))
    }

    pub fn is_turn(&self) -> bool {
        !self.urls.is_empty()
            && self
                .urls
                .iter()
                .all(|u| u.starts_with("turn:") || u.starts_with("turns:"))
    }
}

/// A port, written as a number or a string (a string may be a `local/port,...` map).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
enum PortOverride {
    Number(i64),
    Text(String),
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct TurnConfig {
    pub static_auth_secret: String,
    pub credentials_expiration_minutes: i64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct RtcConfig {
    pub ice_address_udp: String,
    pub ice_port_udp: i64,
    pub ice_address_tcp: String,
    pub ice_port_tcp: i64,
    pub ice_host_override: String,
    #[serde(deserialize_with = "deserialize_port_override")]
    pub ice_host_port_override: String,
    #[serde(deserialize_with = "deserialize_ice_servers")]
    pub ice_servers: Vec<IceServer>,
    pub turn: TurnConfig,
    pub enable_ipv6: bool,
    /// Kept for compatibility with Go rtcd config files: the Go service opened hundreds of sockets
    /// to ease lock contention, which this service has no use for.
    pub udp_sockets_count: i64,
    pub nack_buffer_size: i64,
    /// Kept for compatibility: packets are always stored by value here, there is no unsafe mode.
    pub nack_disable_copy: bool,
    /// Threads serving calls. 0 lets the SFU choose: it starts with two where there are cores for
    /// two (a class that begins at the bell is placed call by call before its participants arrive),
    /// adds another when every shard has been busy for a few seconds and the machine has most of a
    /// core idle (up to every CPU the process may use) and stops one that has been empty for two
    /// minutes. A positive number pins it.
    pub shards: i64,
    /// How many voices of a call are forwarded once more microphones than that are open: the
    /// loudest ones, as Meet and Jitsi do. A listener follows two or three speakers, and every open
    /// microphone is a stream to every listener: thirteen open microphones in a class of 130 are
    /// 1,700 packets every 20 ms, of which each listener can use three. The default is three; 0
    /// forwards all of them.
    pub audio_loudest: i64,
    /// The packet time, in milliseconds, browsers are asked to send their voice with. A longer one
    /// makes fewer, larger packets: 40 halves the packets a call's open microphones cost every
    /// listener, for 20 ms more delay (a browser sends 20 on its own). 0, the default, leaves it
    /// to the browser; otherwise a multiple of 20 (Opus's frame) up to 120.
    pub audio_ptime_ms: i64,
    /// The most this node should send, in megabits per second. Near it every receiver's cameras
    /// and screen shares fall back to their small layer, so the node slows down evenly instead of
    /// losing packets for everyone. 0 (the default) sets no budget: nothing squeezes video except
    /// `egress_adapt`, which learns a ceiling from the kernel refusing datagrams. The rate is
    /// reported on `GET /system` either way.
    pub egress_budget_mbps: i64,
    /// The most this node should send, in packets per second: the same fall back for the other
    /// ceiling. A virtual network card takes a limited number of packets however small they are,
    /// and voice packets are small: the OVH video VPS sends 150 to 200 thousand a second and then
    /// its transmit ring stays full. Set it to about 80 % of what the card was measured to take.
    /// 0 (the default) sets no budget, and nothing squeezes on packets except `egress_adapt`.
    pub egress_budget_pps: i64,
    /// Whether the node acts on the kernel refusing its datagrams (`RTCD_RTC_EGRESSADAPT`). Off,
    /// the default, it counts and reports them (`GET /system`, `/metrics`) and squeezes video
    /// only for a send budget, as the previous release did. On, it also squeezes video and cuts
    /// voices to two while refusals last, learns a ceiling from an overload that goes on for
    /// fifteen seconds, cuts calls to their loudest voice if the squeeze does not stop the
    /// refusals, and slows recording uploads while they last (see `rtcd_sfu::egress`). Those are
    /// guesses about a signal that is the whole machine's: turn it on after a run on the real card.
    pub egress_adapt: bool,
    /// The learn floor, when `egress_adapt` is on: refusals while the node sends under this many
    /// megabits a second and under `egress_learn_min_pps` packets a second never teach it a
    /// ceiling (it squeezes while they last and no longer), because a node that small is not at
    /// its card's limit. The defaults are about half of what the OVH video VPS's card was measured
    /// to take: 800 Mbit/s of large packets, 160 thousand small ones a second.
    pub egress_learn_min_mbps: i64,
    pub egress_learn_min_pps: i64,
    /// Whether a join is refused while the shard that would serve it is at its limit (a thread
    /// busy for three seconds at 95 % or more, after shedding what it can): the participants it
    /// has keep their quality, where letting more in makes every call on the shard lose packets.
    /// The default is on. A refused session is closed, and the participant joins again; a new call
    /// goes to a shard that is not at its limit when there is one. The call's host and recorder,
    /// and a participant who is in the call or left it in the last minute, are never refused.
    pub refuse_when_saturated: bool,
    /// Seconds to let running sessions end on shutdown before ending them; 0 waits for them
    /// without limit, as the Go service does (a second signal ends them at once).
    pub drain_timeout_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct StoreConfig {
    pub data_source: String,
}

/// Recording calls in rtcd itself, without a recorder participant.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct RecordConfig {
    /// Whether this node records calls when asked to. Off by default.
    pub enabled: bool,
    /// Where the files of a recording are written, in a directory of its own, until they are
    /// uploaded: a volume with room for the lessons that run at once.
    pub data_dir: String,
    /// The most one capture (a run, from a `record_start`) may write, in bytes, files already
    /// uploaded included: it counts what was written, not what is on the disk (that is bounded by
    /// `max_pending_bytes`). A capture that reaches it ends cleanly (`disk_full`) with what it
    /// has, while its class may be live, so it is not a guard to trip in a normal lesson: the
    /// default is the most the server keeps of a recording (`RecordingMaxBytes`, 16 GiB), which
    /// a four-hour lesson of ten cameras, their voices and a shared screen at the web client's
    /// highest rates comes just under.
    pub max_bytes: i64,
    /// The most bytes of closed files this node holds waiting to be uploaded, all recordings
    /// together. Above it recordings drop packets (each drop is a gap in the recording and is
    /// counted) until uploads catch up, rather than ending. Files still being written are on top
    /// of this.
    pub max_pending_bytes: i64,
    /// The rate, in megabits per second, at which this node uploads recordings, all of them
    /// together: uploads must not compete with the media the node sends, and must keep up with
    /// what is written (175 recorded lessons write ~113 Mbit/s on average), or the closed files
    /// pile up to `max_pending_bytes` and recordings drop packets.
    pub upload_mbps: i64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct LoggerConfig {
    pub enable_console: bool,
    pub console_json: bool,
    pub console_level: String,
    pub enable_file: bool,
    pub file_json: bool,
    pub file_level: String,
    pub file_location: String,
    pub enable_color: bool,
}

fn deserialize_port_override<'de, D: serde::Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    Ok(match PortOverride::deserialize(d)? {
        PortOverride::Number(n) => n.to_string(),
        PortOverride::Text(s) => s,
    })
}

fn deserialize_ice_servers<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Vec<IceServer>, D::Error> {
    Ok(Vec::<IceServerEntry>::deserialize(d)?
        .into_iter()
        .map(IceServer::from)
        .collect())
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            listen_address: ":8045".into(),
            tls: TlsConfig::default(),
        }
    }
}

impl Default for SessionCacheConfig {
    fn default() -> Self {
        Self {
            expiration_minutes: 1440,
        }
    }
}

impl Default for TurnConfig {
    fn default() -> Self {
        Self {
            static_auth_secret: String::new(),
            credentials_expiration_minutes: 1440,
        }
    }
}

impl Default for RtcConfig {
    fn default() -> Self {
        Self {
            ice_address_udp: String::new(),
            ice_port_udp: 8443,
            ice_address_tcp: String::new(),
            ice_port_tcp: 8443,
            ice_host_override: String::new(),
            ice_host_port_override: String::new(),
            ice_servers: Vec::new(),
            turn: TurnConfig::default(),
            enable_ipv6: false,
            udp_sockets_count: 0,
            nack_buffer_size: 256,
            nack_disable_copy: true,
            shards: 0,
            audio_loudest: 3,
            audio_ptime_ms: 0,
            egress_budget_mbps: 0,
            egress_budget_pps: 0,
            egress_adapt: false,
            egress_learn_min_mbps: 400,
            egress_learn_min_pps: 80_000,
            refuse_when_saturated: true,
            drain_timeout_seconds: 0,
        }
    }
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            data_source: "/tmp/rtcd_db".into(),
        }
    }
}

impl Default for RecordConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            data_dir: "/data/recordings".into(),
            max_bytes: 16 << 30,
            max_pending_bytes: 4 << 30,
            upload_mbps: 200,
        }
    }
}

impl Default for LoggerConfig {
    fn default() -> Self {
        Self {
            enable_console: true,
            console_json: false,
            console_level: "INFO".into(),
            enable_file: true,
            file_json: true,
            file_level: "DEBUG".into(),
            file_location: "rtcd.log".into(),
            enable_color: false,
        }
    }
}

const LEVELS: [&str; 5] = ["debug", "info", "warn", "error", "critical"];

impl Config {
    /// Reads `path` if it exists (a missing file means defaults), then applies the environment.
    pub fn load(path: &Path, env: &BTreeMap<String, String>) -> Result<Self, ConfigError> {
        let mut cfg = match std::fs::read_to_string(path) {
            Ok(text) => Self::from_toml(&text)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Self::default(),
            Err(source) => {
                return Err(ConfigError::Read {
                    path: path.display().to_string(),
                    source,
                });
            }
        };
        cfg.apply_env(env)?;
        Ok(cfg)
    }

    pub fn from_toml(text: &str) -> Result<Self, ConfigError> {
        Ok(toml::from_str(text)?)
    }

    /// Applies `RTCD_*` overrides. The names are the Go struct field names in upper case joined by
    /// underscores (`RTCD_RTC_ICEPORTUDP`), as the Go service's `envconfig` derives them.
    pub fn apply_env(&mut self, env: &BTreeMap<String, String>) -> Result<(), ConfigError> {
        let get = |name: &str| env.get(&format!("RTCD_{name}")).map(String::as_str);
        let var = |name: &str| format!("RTCD_{name}");
        macro_rules! text {
            ($name:literal, $field:expr) => {
                if let Some(v) = get($name) {
                    $field = v.to_owned();
                }
            };
        }
        macro_rules! boolean {
            ($name:literal, $field:expr) => {
                if let Some(v) = get($name) {
                    $field = parse_bool(v).ok_or_else(|| ConfigError::Env {
                        var: var($name),
                        message: format!("{v:?} is not a boolean"),
                    })?;
                }
            };
        }
        macro_rules! integer {
            ($name:literal, $field:expr) => {
                if let Some(v) = get($name) {
                    $field = v.trim().parse().map_err(|_| ConfigError::Env {
                        var: var($name),
                        message: format!("{v:?} is not an integer"),
                    })?;
                }
            };
        }
        text!("API_HTTP_LISTENADDRESS", self.api.http.listen_address);
        boolean!("API_HTTP_TLS_ENABLE", self.api.http.tls.enable);
        text!("API_HTTP_TLS_CERTFILE", self.api.http.tls.cert_file);
        text!("API_HTTP_TLS_CERTKEY", self.api.http.tls.cert_key);
        boolean!("API_SECURITY_ENABLEADMIN", self.api.security.enable_admin);
        text!(
            "API_SECURITY_ADMINSECRETKEY",
            self.api.security.admin_secret_key
        );
        boolean!(
            "API_SECURITY_ALLOWSELFREGISTRATION",
            self.api.security.allow_self_registration
        );
        integer!(
            "API_SECURITY_SESSIONCACHE_EXPIRATIONMINUTES",
            self.api.security.session_cache.expiration_minutes
        );
        text!("RTC_ICEADDRESSUDP", self.rtc.ice_address_udp);
        integer!("RTC_ICEPORTUDP", self.rtc.ice_port_udp);
        text!("RTC_ICEADDRESSTCP", self.rtc.ice_address_tcp);
        integer!("RTC_ICEPORTTCP", self.rtc.ice_port_tcp);
        text!("RTC_ICEHOSTOVERRIDE", self.rtc.ice_host_override);
        text!("RTC_ICEHOSTPORTOVERRIDE", self.rtc.ice_host_port_override);
        if let Some(v) = get("RTC_ICESERVERS") {
            self.rtc.ice_servers = parse_ice_servers(v).map_err(|message| ConfigError::Env {
                var: var("RTC_ICESERVERS"),
                message,
            })?;
        }
        text!(
            "RTC_TURNCONFIG_STATICAUTHSECRET",
            self.rtc.turn.static_auth_secret
        );
        integer!(
            "RTC_TURNCONFIG_CREDENTIALSEXPIRATIONMINUTES",
            self.rtc.turn.credentials_expiration_minutes
        );
        boolean!("RTC_ENABLEIPV6", self.rtc.enable_ipv6);
        integer!("RTC_UDPSOCKETSCOUNT", self.rtc.udp_sockets_count);
        integer!("RTC_NACKBUFFERSIZE", self.rtc.nack_buffer_size);
        boolean!("RTC_NACKDISABLECOPY", self.rtc.nack_disable_copy);
        integer!("RTC_SHARDS", self.rtc.shards);
        integer!("RTC_AUDIOLOUDEST", self.rtc.audio_loudest);
        integer!("RTC_AUDIOPTIMEMS", self.rtc.audio_ptime_ms);
        integer!("RTC_EGRESSBUDGETMBPS", self.rtc.egress_budget_mbps);
        integer!("RTC_EGRESSBUDGETPPS", self.rtc.egress_budget_pps);
        boolean!("RTC_EGRESSADAPT", self.rtc.egress_adapt);
        integer!("RTC_EGRESSLEARNMINMBPS", self.rtc.egress_learn_min_mbps);
        integer!("RTC_EGRESSLEARNMINPPS", self.rtc.egress_learn_min_pps);
        boolean!("RTC_REFUSEWHENSATURATED", self.rtc.refuse_when_saturated);
        integer!("RTC_DRAINTIMEOUTSECONDS", self.rtc.drain_timeout_seconds);
        text!("STORE_DATASOURCE", self.store.data_source);
        boolean!("RECORD_ENABLED", self.record.enabled);
        text!("RECORD_DATADIR", self.record.data_dir);
        integer!("RECORD_MAXBYTES", self.record.max_bytes);
        integer!("RECORD_MAXPENDINGBYTES", self.record.max_pending_bytes);
        integer!("RECORD_UPLOADMBPS", self.record.upload_mbps);
        boolean!("LOGGER_ENABLECONSOLE", self.logger.enable_console);
        boolean!("LOGGER_CONSOLEJSON", self.logger.console_json);
        text!("LOGGER_CONSOLELEVEL", self.logger.console_level);
        boolean!("LOGGER_ENABLEFILE", self.logger.enable_file);
        boolean!("LOGGER_FILEJSON", self.logger.file_json);
        text!("LOGGER_FILELEVEL", self.logger.file_level);
        text!("LOGGER_FILELOCATION", self.logger.file_location);
        boolean!("LOGGER_ENABLECOLOR", self.logger.enable_color);
        Ok(())
    }

    /// The checks of the Go service, in its order and wording.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let sec = &self.api.security;
        if sec.enable_admin && sec.admin_secret_key.is_empty() {
            return Err(invalid("AdminSecretKey", "should not be empty"));
        }
        if self.api.http.listen_address.is_empty() {
            return Err(invalid("ListenAddress", "should not be empty"));
        }
        let tls = &self.api.http.tls;
        if tls.enable {
            if tls.cert_file.is_empty() {
                return Err(invalid("CertFile", "should not be empty"));
            }
            if tls.cert_key.is_empty() {
                return Err(invalid("CertKey", "should not be empty"));
            }
        }
        if sec.session_cache.expiration_minutes <= 0 {
            return Err(invalid("ExpirationMinutes", "should be a positive number"));
        }
        let rtc = &self.rtc;
        for (field, list) in [
            ("ICEAddressUDP", &rtc.ice_address_udp),
            ("ICEAddressTCP", &rtc.ice_address_tcp),
        ] {
            for addr in split_list(list) {
                if addr.parse::<std::net::IpAddr>().is_err() {
                    return Err(invalid(field, format!("{addr} is not a valid IP address")));
                }
            }
        }
        for (field, port) in [
            ("ICEPortUDP", rtc.ice_port_udp),
            ("ICEPortTCP", rtc.ice_port_tcp),
        ] {
            if !(80..=49151).contains(&port) {
                return Err(invalid(
                    field,
                    format!("{port} is not in allowed range [80, 49151]"),
                ));
            }
        }
        for s in &rtc.ice_servers {
            if s.urls.is_empty() {
                return Err(invalid("ICEServers", "invalid empty URLs"));
            }
            if s.urls.iter().any(String::is_empty) {
                return Err(invalid("ICEServers", "invalid empty URL"));
            }
            if !s.is_stun() && !s.is_turn() {
                return Err(invalid("ICEServers", "URL is not a valid STUN/TURN server"));
            }
        }
        if !rtc.turn.static_auth_secret.is_empty() {
            let m = rtc.turn.credentials_expiration_minutes;
            if m <= 0 {
                return Err(invalid(
                    "CredentialsExpirationMinutes",
                    "should be a positive number",
                ));
            }
            if m >= rtcd_proto::turn::MAX_EXPIRATION_MINUTES {
                return Err(invalid(
                    "CredentialsExpirationMinutes",
                    "should be less than 1 week",
                ));
            }
        }
        self.host_port_override()?;
        let n = rtc.nack_buffer_size;
        if n != 0 {
            if n < 32 {
                return Err(invalid("NACKBufferSize", "should be at least 32"));
            }
            if n > 8192 {
                return Err(invalid("NACKBufferSize", "should not exceed 8192"));
            }
            if n & (n - 1) != 0 {
                return Err(invalid(
                    "NACKBufferSize",
                    "must be a power of 2 (32, 64, 128, 256, 512, 1024, 2048, 4096, 8192)",
                ));
            }
        }
        if rtc.shards < 0 || rtc.shards > 256 {
            return Err(invalid("Shards", "must be between 0 and 256"));
        }
        if !(0..=100_000).contains(&rtc.egress_budget_mbps) {
            return Err(invalid("EgressBudgetMbps", "must be between 0 and 100000"));
        }
        if !(0..=100_000_000).contains(&rtc.egress_budget_pps) {
            return Err(invalid(
                "EgressBudgetPps",
                "must be between 0 and 100000000",
            ));
        }
        if !(0..=100_000).contains(&rtc.egress_learn_min_mbps) {
            return Err(invalid(
                "EgressLearnMinMbps",
                "must be between 0 and 100000",
            ));
        }
        if !(0..=100_000_000).contains(&rtc.egress_learn_min_pps) {
            return Err(invalid(
                "EgressLearnMinPps",
                "must be between 0 and 100000000",
            ));
        }
        if !(0..=64).contains(&rtc.audio_loudest) {
            return Err(invalid("AudioLoudest", "must be between 0 and 64"));
        }
        if !(0..=120).contains(&rtc.audio_ptime_ms) || rtc.audio_ptime_ms % 20 != 0 {
            return Err(invalid(
                "AudioPtimeMs",
                "must be 0 or a multiple of 20 up to 120",
            ));
        }
        if rtc.drain_timeout_seconds < 0 {
            return Err(invalid("DrainTimeoutSeconds", "should not be negative"));
        }
        if self.store.data_source.is_empty() {
            return Err(invalid("DataSource", "should not be empty"));
        }
        if self.record.enabled {
            if self.record.data_dir.is_empty() {
                return Err(invalid("RecordDataDir", "should not be empty"));
            }
            if self.record.max_bytes <= 0 {
                return Err(invalid("RecordMaxBytes", "should be a positive number"));
            }
            if self.record.max_pending_bytes <= 0 {
                return Err(invalid(
                    "RecordMaxPendingBytes",
                    "should be a positive number",
                ));
            }
            if self.record.upload_mbps <= 0 {
                return Err(invalid("RecordUploadMbps", "should be a positive number"));
            }
        }
        let lg = &self.logger;
        if !lg.enable_console && !lg.enable_file {
            return Err(ConfigError::Invalid {
                field: "logger",
                message: "should enable at least one logging target".into(),
            });
        }
        let known = |l: &str| LEVELS.contains(&l.to_ascii_lowercase().as_str());
        if lg.enable_console && !known(&lg.console_level) {
            return Err(invalid("ConsoleLevel", format!("{:?}", lg.console_level)));
        }
        if lg.enable_file && !known(&lg.file_level) {
            return Err(invalid("FileLevel", format!("{:?}", lg.file_level)));
        }
        if lg.enable_file && lg.file_location.is_empty() {
            return Err(invalid("FileLocation", "should not be empty"));
        }
        Ok(())
    }

    /// The parsed `ice_host_port_override`: a single port, or a map of local address to port.
    pub fn host_port_override(&self) -> Result<HostPortOverride, ConfigError> {
        HostPortOverride::parse(&self.rtc.ice_host_port_override)
            .map_err(|m| invalid("ICEHostPortOverride", m))
    }
}

/// The advertised port of host candidates, when it differs from the one listened on.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum HostPortOverride {
    #[default]
    None,
    /// One port for every address.
    Single(u16),
    /// A port per local address.
    Map(BTreeMap<String, u16>),
}

impl HostPortOverride {
    fn parse(s: &str) -> Result<Self, String> {
        if s.is_empty() {
            return Ok(Self::None);
        }
        if let Ok(port) = s.parse::<u32>() {
            if !(80..=49151).contains(&port) {
                return Err(format!("{port} is not in allowed range [80, 49151]"));
            }
            return Ok(Self::Single(port as u16));
        }
        let mut map = BTreeMap::new();
        let mut ports = std::collections::BTreeSet::new();
        for pair in s.split(',') {
            let (addr, port) = pair
                .split_once('/')
                .filter(|(_, p)| !p.contains('/'))
                .ok_or("failed to parse mapping: invalid map pairing syntax")?;
            let port: u16 = port.parse().map_err(|e| {
                format!("failed to parse mapping: failed to parse port number: {e}")
            })?;
            if map.insert(addr.to_owned(), port).is_some() {
                return Err(format!(
                    "failed to parse mapping: duplicate mapping found for {addr}"
                ));
            }
            if !ports.insert(port) {
                return Err(format!(
                    "failed to parse mapping: duplicate port found for {port}"
                ));
            }
        }
        Ok(Self::Map(map))
    }

    /// The port to advertise for a local address, if overridden.
    pub fn port_for(&self, local: &std::net::IpAddr) -> Option<u16> {
        match self {
            Self::None => None,
            Self::Single(p) => Some(*p),
            Self::Map(m) => m.get(&local.to_string()).copied(),
        }
    }
}

/// Comma separated list entries, trimmed.
pub fn split_list(s: &str) -> Vec<&str> {
    if s.is_empty() {
        Vec::new()
    } else {
        s.split(',').map(str::trim).collect()
    }
}

/// Go's `strconv.ParseBool`.
fn parse_bool(s: &str) -> Option<bool> {
    match s {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

/// `RTCD_RTC_ICESERVERS`: a JSON array of URLs or of server objects.
fn parse_ice_servers(value: &str) -> Result<Vec<IceServer>, String> {
    if let Ok(urls) = serde_json::from_str::<Vec<String>>(value) {
        return Ok(vec![IceServer {
            urls,
            username: String::new(),
            credential: String::new(),
        }]);
    }
    #[derive(Deserialize)]
    struct Obj {
        #[serde(default)]
        urls: Vec<String>,
        #[serde(default)]
        username: String,
        #[serde(default)]
        credential: String,
    }
    serde_json::from_str::<Vec<Obj>>(value)
        .map(|v| {
            v.into_iter()
                .map(|o| IceServer {
                    urls: o.urls,
                    username: o.username,
                    credential: o.credential,
                })
                .collect()
        })
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn defaults_are_the_go_defaults_and_valid() {
        let c = Config::default();
        assert_eq!(c.api.http.listen_address, ":8045");
        assert_eq!((c.rtc.ice_port_udp, c.rtc.ice_port_tcp), (8443, 8443));
        assert_eq!(c.rtc.nack_buffer_size, 256);
        assert_eq!(
            c.rtc.audio_loudest, 3,
            "the three loudest voices of a call are forwarded unless asked otherwise"
        );
        assert!(
            c.rtc.refuse_when_saturated,
            "a saturated shard refuses joins unless told not to"
        );
        assert_eq!(c.store.data_source, "/tmp/rtcd_db");
        assert_eq!(c.api.security.session_cache.expiration_minutes, 1440);
        assert!(!c.record.enabled, "recording is off unless asked for");
        assert_eq!(
            (c.record.max_pending_bytes, c.record.upload_mbps),
            (4 << 30, 200)
        );
        c.validate().unwrap();
    }

    #[test]
    fn the_sample_config_of_the_go_service_parses() {
        let c = Config::from_toml(include_str!("../tests/data/config.sample.toml")).unwrap();
        assert_eq!(c.api.http.listen_address, ":8045");
        assert!(!c.api.security.allow_self_registration);
        assert_eq!(c.rtc.ice_port_udp, 8443);
        assert!(c.rtc.ice_servers.is_empty());
        c.validate().unwrap();
    }

    #[test]
    fn the_config_the_docker_entrypoint_renders_parses() {
        let c = Config::from_toml(
            r#"
[api.http]
listen_address = ":8045"

[api.security]
allow_self_registration = true
enable_admin = false

[rtc]
ice_port_udp = 8443
ice_port_tcp = 8443
ice_host_override = "203.0.113.9"
ice_servers = [{urls = ["stun:stun.l.google.com:19302"]}]

[store]
data_source = "/data/rtcd_db"

[logger]
enable_console = true
console_level = "INFO"
console_json = true
enable_file = false
"#,
        )
        .unwrap();
        c.validate().unwrap();
        assert!(c.api.security.allow_self_registration);
        assert_eq!(c.rtc.ice_host_override, "203.0.113.9");
        assert_eq!(
            c.rtc.ice_servers[0].urls,
            vec!["stun:stun.l.google.com:19302"]
        );
        assert!(c.rtc.ice_servers[0].is_stun());
        assert!(!c.logger.enable_file);
    }

    #[test]
    fn ice_servers_may_be_bare_urls_or_tables() {
        let c = Config::from_toml(
            r#"[rtc]
ice_servers = ["stun:a:3478", {urls = ["turn:b:3478"], username = "u", credential = "p"}]"#,
        )
        .unwrap();
        assert_eq!(c.rtc.ice_servers.len(), 2);
        assert!(c.rtc.ice_servers[0].is_stun());
        assert!(c.rtc.ice_servers[1].is_turn());
        assert_eq!(c.rtc.ice_servers[1].username, "u");
    }

    #[test]
    fn a_host_port_override_may_be_a_number_or_a_map() {
        let n = Config::from_toml("[rtc]\nice_host_port_override = 30443").unwrap();
        assert_eq!(
            n.host_port_override().unwrap(),
            HostPortOverride::Single(30443)
        );
        let m =
            Config::from_toml("[rtc]\nice_host_port_override = \"10.0.0.1/8443,10.0.0.2/8444\"")
                .unwrap();
        let o = m.host_port_override().unwrap();
        assert_eq!(o.port_for(&"10.0.0.2".parse().unwrap()), Some(8444));
        assert_eq!(o.port_for(&"10.0.0.3".parse().unwrap()), None);
        for bad in ["10.0.0.1/8443,10.0.0.1/8444", "a/1,b/1", "nonsense", "79"] {
            let c =
                Config::from_toml(&format!("[rtc]\nice_host_port_override = \"{bad}\"")).unwrap();
            assert!(c.host_port_override().is_err(), "{bad}");
        }
    }

    #[test]
    fn environment_overrides_use_the_go_names() {
        let mut c = Config::default();
        c.apply_env(&env(&[
            ("RTCD_API_SECURITY_ALLOWSELFREGISTRATION", "true"),
            ("RTCD_RTC_ICEPORTUDP", "30000"),
            ("RTCD_RTC_AUDIOLOUDEST", "5"),
            ("RTCD_RTC_AUDIOPTIMEMS", "40"),
            ("RTCD_RTC_EGRESSBUDGETMBPS", "250"),
            ("RTCD_RTC_EGRESSBUDGETPPS", "120000"),
            ("RTCD_RTC_EGRESSADAPT", "true"),
            ("RTCD_RTC_EGRESSLEARNMINMBPS", "300"),
            ("RTCD_RTC_EGRESSLEARNMINPPS", "60000"),
            ("RTCD_RTC_REFUSEWHENSATURATED", "false"),
            ("RTCD_STORE_DATASOURCE", "/data/db"),
            ("RTCD_LOGGER_ENABLEFILE", "false"),
            ("RTCD_LOGGER_CONSOLEJSON", "1"),
            ("RTCD_RTC_ICESERVERS", r#"["stun:x:3478"]"#),
            ("RTCD_RTC_TURNCONFIG_STATICAUTHSECRET", "s3cret"),
            ("UNRELATED", "x"),
        ]))
        .unwrap();
        assert!(c.api.security.allow_self_registration);
        assert_eq!(c.rtc.ice_port_udp, 30000);
        assert_eq!(c.rtc.audio_loudest, 5);
        assert_eq!(c.rtc.audio_ptime_ms, 40);
        assert_eq!(c.rtc.egress_budget_mbps, 250);
        assert_eq!(c.rtc.egress_budget_pps, 120_000);
        assert!(c.rtc.egress_adapt);
        assert_eq!(c.rtc.egress_learn_min_mbps, 300);
        assert_eq!(c.rtc.egress_learn_min_pps, 60_000);
        assert!(!c.rtc.refuse_when_saturated);
        assert_eq!(c.store.data_source, "/data/db");
        assert!(!c.logger.enable_file);
        assert!(c.logger.console_json);
        assert_eq!(c.rtc.ice_servers[0].urls, vec!["stun:x:3478"]);
        assert_eq!(c.rtc.turn.static_auth_secret, "s3cret");
    }

    #[test]
    fn recording_is_set_in_the_file_or_the_environment() {
        let mut c = Config::from_toml(
            "[record]\nenabled = true\ndata_dir = \"/v/rec\"\nmax_bytes = 1000\nmax_pending_bytes = 2000\nupload_mbps = 8\n",
        )
        .unwrap();
        let seen = |c: &Config| {
            (
                c.record.enabled,
                c.record.data_dir.clone(),
                c.record.max_bytes,
                c.record.max_pending_bytes,
                c.record.upload_mbps,
            )
        };
        assert_eq!(seen(&c), (true, "/v/rec".into(), 1000, 2000, 8));
        c.apply_env(&env(&[
            ("RTCD_RECORD_ENABLED", "false"),
            ("RTCD_RECORD_DATADIR", "/other"),
            ("RTCD_RECORD_MAXBYTES", "5000"),
            ("RTCD_RECORD_MAXPENDINGBYTES", "6000"),
            ("RTCD_RECORD_UPLOADMBPS", "20"),
        ]))
        .unwrap();
        assert_eq!(seen(&c), (false, "/other".into(), 5000, 6000, 20));
    }

    #[test]
    fn ice_servers_in_the_environment_may_be_objects() {
        let mut c = Config::default();
        c.apply_env(&env(&[(
            "RTCD_RTC_ICESERVERS",
            r#"[{"urls":["turn:t:3478"],"username":"a","credential":"b"}]"#,
        )]))
        .unwrap();
        assert!(c.rtc.ice_servers[0].is_turn());
        assert_eq!(c.rtc.ice_servers[0].credential, "b");
    }

    #[test]
    fn a_malformed_override_names_its_variable() {
        let mut c = Config::default();
        let err = c
            .apply_env(&env(&[("RTCD_RTC_ICEPORTUDP", "eighty")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("RTCD_RTC_ICEPORTUDP"), "{err}");
        let err = c
            .apply_env(&env(&[("RTCD_API_HTTP_TLS_ENABLE", "maybe")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("RTCD_API_HTTP_TLS_ENABLE"), "{err}");
    }

    #[test]
    fn validation_catches_what_the_go_service_catches() {
        let bad = |edit: &dyn Fn(&mut Config)| {
            let mut c = Config::default();
            edit(&mut c);
            c.validate().unwrap_err().to_string()
        };
        assert!(bad(&|c| c.api.security.enable_admin = true).contains("AdminSecretKey"));
        assert!(bad(&|c| c.api.http.listen_address.clear()).contains("ListenAddress"));
        assert!(bad(&|c| c.api.http.tls.enable = true).contains("CertFile"));
        assert!(bad(&|c| c.rtc.ice_port_udp = 79).contains("[80, 49151]"));
        assert!(bad(&|c| c.rtc.ice_port_tcp = 49152).contains("ICEPortTCP"));
        assert!(bad(&|c| c.rtc.ice_address_udp = "not-an-ip".into()).contains("ICEAddressUDP"));
        assert!(bad(&|c| c.rtc.nack_buffer_size = 100).contains("power of 2"));
        assert!(bad(&|c| c.rtc.nack_buffer_size = 16).contains("at least 32"));
        assert!(bad(&|c| c.rtc.nack_buffer_size = 16384).contains("exceed 8192"));
        assert!(bad(&|c| c.rtc.audio_loudest = -1).contains("between 0 and 64"));
        assert!(bad(&|c| c.rtc.audio_loudest = 65).contains("between 0 and 64"));
        assert!(bad(&|c| c.rtc.audio_ptime_ms = 25).contains("multiple of 20"));
        assert!(bad(&|c| c.rtc.audio_ptime_ms = 10).contains("multiple of 20"));
        assert!(bad(&|c| c.rtc.audio_ptime_ms = 30).contains("multiple of 20"));
        assert!(bad(&|c| c.rtc.audio_ptime_ms = 140).contains("multiple of 20"));
        assert!(bad(&|c| c.rtc.audio_ptime_ms = -20).contains("multiple of 20"));
        assert!(bad(&|c| c.rtc.egress_budget_mbps = -1).contains("between 0 and 100000"));
        assert!(bad(&|c| c.rtc.egress_budget_pps = -1).contains("between 0 and 100000000"));
        assert!(bad(&|c| c.rtc.egress_learn_min_mbps = -1).contains("between 0 and 100000"));
        assert!(bad(&|c| c.rtc.egress_learn_min_pps = -1).contains("between 0 and 100000000"));
        assert!(
            bad(&|c| c.rtc.ice_servers = vec![IceServer {
                urls: vec!["http://x".into()],
                username: String::new(),
                credential: String::new()
            }])
            .contains("STUN/TURN")
        );
        assert!(
            bad(&|c| {
                c.rtc.turn.static_auth_secret = "s".into();
                c.rtc.turn.credentials_expiration_minutes = 0;
            })
            .contains("CredentialsExpirationMinutes")
        );
        assert!(bad(&|c| c.logger.console_level = "loud".into()).contains("ConsoleLevel"));
        assert!(
            bad(&|c| {
                c.logger.enable_console = false;
                c.logger.enable_file = false;
            })
            .contains("at least one logging target")
        );
        assert!(bad(&|c| c.store.data_source.clear()).contains("DataSource"));
        assert!(
            bad(&|c| {
                c.record.enabled = true;
                c.record.data_dir.clear();
            })
            .contains("RecordDataDir")
        );
        assert!(
            bad(&|c| {
                c.record.enabled = true;
                c.record.max_bytes = 0;
            })
            .contains("RecordMaxBytes")
        );
        assert!(
            bad(&|c| {
                c.record.enabled = true;
                c.record.max_pending_bytes = 0;
            })
            .contains("RecordMaxPendingBytes")
        );
        assert!(
            bad(&|c| {
                c.record.enabled = true;
                c.record.upload_mbps = 0;
            })
            .contains("RecordUploadMbps")
        );
        let mut off = Config::default();
        off.record.max_bytes = 0;
        off.validate().expect("a disabled recorder is not checked");
    }

    #[test]
    fn a_missing_file_means_defaults_and_a_present_one_is_overridden_by_the_environment() {
        let dir = std::env::temp_dir().join(format!("rtcd-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let missing = Config::load(&dir.join("absent.toml"), &env(&[])).unwrap();
        assert_eq!(missing, Config::default());
        let file = dir.join("c.toml");
        std::fs::write(&file, "[rtc]\nice_port_udp = 9000\nice_port_tcp = 9001\n").unwrap();
        let c = Config::load(&file, &env(&[("RTCD_RTC_ICEPORTTCP", "9002")])).unwrap();
        assert_eq!((c.rtc.ice_port_udp, c.rtc.ice_port_tcp), (9000, 9002));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn go_parse_bool_spellings() {
        for t in ["1", "t", "T", "TRUE", "true", "True"] {
            assert_eq!(parse_bool(t), Some(true));
        }
        for f in ["0", "f", "F", "FALSE", "false", "False"] {
            assert_eq!(parse_bool(f), Some(false));
        }
        assert_eq!(parse_bool("yes"), None);
    }

    #[test]
    fn the_default_cap_of_a_capture_holds_the_longest_lesson_at_the_highest_rates() {
        // The web client's top layers (lms-web `media.ts`, `call-client.ts`): a camera 600 kbit/s,
        // a screen 2.5 Mbit/s, a voice about 40 kbit/s; the longest recording is four hours and
        // a class of ten cameras is the largest the cap is meant for. 2 GiB lasted 30 minutes.
        let bits_per_second = 10 * (600_000 + 40_000) + 2_500_000;
        let bytes = bits_per_second / 8 * 4 * 3600;
        let cap = RecordConfig::default().max_bytes;
        assert!(bytes < cap, "{bytes} bytes in four hours, cap {cap}");
        assert!(cap <= 16 << 30, "more than the server keeps of a recording");
    }
}
