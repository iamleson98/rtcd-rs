//! Records the commit and the compiler the binary was built with, for `recorder version`.

use std::process::Command;

fn run(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_default()
}

fn main() {
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".into());
    let version = run(&rustc, &["--version"]);
    println!(
        "cargo:rustc-env=RECORDER_RUSTC={}",
        version.split_whitespace().nth(1).unwrap_or("unknown")
    );
    let revision = std::env::var("RECORDER_REVISION")
        .ok()
        .filter(|r| !r.is_empty())
        .unwrap_or_else(|| run("git", &["rev-parse", "HEAD"]));
    println!("cargo:rustc-env=RECORDER_REVISION={revision}");
    println!("cargo:rerun-if-env-changed=RECORDER_REVISION");
    println!("cargo:rerun-if-changed=../../.git/HEAD");
}
