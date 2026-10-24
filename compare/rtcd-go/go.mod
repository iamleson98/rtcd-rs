module github.com/mattermost/rtcd

go 1.27.0

require (
	git.mills.io/prologic/bitcask v1.0.2
	github.com/BurntSushi/toml v1.4.0
	github.com/gorilla/websocket v1.5.3
	github.com/grafana/pyroscope-go/godeltaprof v0.1.8
	github.com/iamleson98/sitename/server/public v0.0.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/mattermost/logr/v2 v2.0.23
	github.com/pborman/uuid v1.2.1
	github.com/pion/ice/v4 v4.2.0
	github.com/pion/interceptor v0.1.44
	github.com/pion/logging v0.2.4
	github.com/pion/rtcp v1.2.16
	github.com/pion/rtp v1.10.1
	github.com/pion/stun/v3 v3.1.5
	github.com/pion/webrtc/v4 v4.2.6
	github.com/prometheus/client_golang v1.23.2
	github.com/prometheus/client_model v0.6.2
	github.com/prometheus/procfs v0.16.1
	github.com/stretchr/testify v1.12.1
	github.com/vmihailenco/msgpack/v5 v5.4.1
	golang.org/x/crypto v0.58.0
	golang.org/x/sys v0.49.0
	golang.org/x/time v0.14.0
)

replace github.com/pion/interceptor v0.1.44 => github.com/bgardner8008/interceptor v0.1.44-mm-mods

require (
	github.com/abcum/lcp v0.0.0-20201209214815-7a3f3840be81 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/francoispqt/gojay v1.2.13 // indirect
	github.com/gofrs/flock v0.13.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/gopherjs/gopherjs v1.17.2 // indirect
	github.com/klauspost/compress v1.18.7 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pion/datachannel v1.6.0 // indirect
	github.com/pion/dtls/v3 v3.1.4 // indirect
	github.com/pion/mdns/v2 v2.1.0 // indirect
	github.com/pion/randutil v0.1.0 // indirect
	github.com/pion/sctp v1.9.2 // indirect
	github.com/pion/sdp/v3 v3.0.17 // indirect
	github.com/pion/srtp/v3 v3.0.10 // indirect
	github.com/pion/transport/v4 v4.0.2 // indirect
	github.com/pion/turn/v4 v4.1.4 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/plar/go-adaptive-radix-tree v1.0.4 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/sirupsen/logrus v1.9.4 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/wiggin77/merror v1.0.5 // indirect
	github.com/wiggin77/srslog v1.0.1 // indirect
	github.com/wlynxg/anet v0.0.5 // indirect
	go.yaml.in/yaml/v2 v2.4.2 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/exp v0.0.0-20250506013437-ce4c2cf36ca6 // indirect
	golang.org/x/net v0.61.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
)

replace github.com/pion/ice/v4 => github.com/bgardner8008/ice/v4 v4.2.0-role-conflict-fix-v4

// Point at the fork's public module (sibling of this vendored copy) when building rtcd standalone.
replace github.com/iamleson98/sitename/server/public => ./third_party/sitename-public

// The fork's public/model imports the main server module (password hashers),
// so building rtcd standalone needs the server module resolved locally too.
