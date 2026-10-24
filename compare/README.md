# rtcd-go vs rtcd-rust

```
rtcd-go/        the Go rtcd as the LMS runs it (its logging dependency vendored in third_party/)
harness/        what runs against either server
  interop/      Go service client + pion: lookup, voice, camera, screen, reconnect; the browser
                bridge (plays the LMS server); the classroom load generator
  browser/      the LMS web app's call client (lms/) bundled for headless Chrome, Firefox and Safari
run-suite.sh    build one server and run everything against it
report.py       compare the two servers' results
```

CI (`.github/workflows/compare.yml`) runs `run-suite.sh` for both servers on Ubuntu and macOS and then
`report.py`, which fails if the Rust service fails a check, delivers less than 99% of the media, or
uses more CPU or memory than Go at 300 participants. On Linux it also profiles each server under load
with `perf` and prints the top symbols, and it sweeps the Rust server's shard count.

## Run it locally

```bash
compare/run-suite.sh go   out/go
compare/run-suite.sh rust out/rust
mkdir results && mv out/go results/local-go && mv out/rust results/local-rust
compare/report.py results
```

Needs Go, a Rust toolchain, [bun](https://bun.sh) and Chrome. The browser test also runs in Firefox and
Safari: `harness/interop/bench.sh <binary> -browser <bundle> -browser-kind firefox|safari -synthetic`
(Safari through `safaridriver`; set `BENCH_ICE_ADDR` to the machine's LAN address for them). The load
generator and the server share the machine, so absolute numbers say little; the same run against each
binary is what compares.
