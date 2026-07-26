use std::{env, path::Path, process::Command};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=../.git/HEAD");
    println!("cargo:rerun-if-changed=../.git/index");
    println!("cargo:rerun-if-changed=../.git/packed-refs");
    for path in [
        "../Cargo.toml",
        "../Cargo.lock",
        "../Frontend/public/js",
        "../Frontend/SystemPrompts",
    ] {
        println!("cargo:rerun-if-changed={path}");
    }

    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let repository = Path::new(&manifest_dir).join("..");
    let commit =
        git_output(&repository, &["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".to_owned());
    if let Some(reference) = git_output(&repository, &["symbolic-ref", "-q", "HEAD"]) {
        println!("cargo:rerun-if-changed=../.git/{reference}");
    }
    let dirty = Command::new("git")
        .args(["status", "--porcelain"])
        .current_dir(&repository)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| !output.stdout.is_empty());

    println!("cargo:rustc-env=KENNEDY_GIT_COMMIT={commit}");
    println!(
        "cargo:rustc-env=KENNEDY_GIT_DIRTY={}",
        dirty.map_or("unknown", |value| if value { "true" } else { "false" })
    );
}

fn git_output(repository: &Path, arguments: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(arguments)
        .current_dir(repository)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}
