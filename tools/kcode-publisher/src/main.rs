#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::error::Error;
use std::fs;
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use kcode_credential_vault::{CredentialVault, ExposeSecret, SecretString};
use kcode_rust_libs_v2::Lib;
use toml::Value;

const CRATES_IO_SECRET: &str = "cratesio-key";
const PLAN_TOKEN: &str = "plan-only-placeholder";
const DEFAULT_ROOT: &str = "data/kcode/kcode-rust-libs";
const DEFAULT_VAULT: &str = "data/kennedy-secrets.age";
const DEFAULT_REGISTRY_TIMEOUT_SECONDS: u64 = 1_500;
const REGISTRY_POLL_INTERVAL: Duration = Duration::from_secs(5);
const USER_AGENT: &str = "kennedy-kcode-publisher/0.1";

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Debug)]
struct Args {
    plan: bool,
    yes: bool,
    root: PathBuf,
    vault: PathBuf,
    registry_timeout: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Package {
    name: String,
    version: String,
    dependencies: BTreeSet<String>,
    manifest: String,
    publishable: bool,
    source: Vec<(String, String)>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RegistryState {
    Published,
    Missing,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("publish-kcode-libs: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let Some(args) = parse_args()? else {
        print_usage();
        return Ok(());
    };

    let vault = if args.plan {
        None
    } else {
        let passphrase = rpassword::prompt_password("Unlock Kennedy credential vault: ")?;
        if passphrase.is_empty() {
            return Err(failure("the credential-vault passphrase cannot be empty"));
        }
        Some(
            CredentialVault::unlock(&args.vault, SecretString::from(passphrase)).map_err(
                |error| {
                    failure(format!(
                        "could not unlock {}: {error}",
                        args.vault.display()
                    ))
                },
            )?,
        )
    };

    let packages = discover_packages(&args.root)?;
    if packages.is_empty() {
        return Err(failure(format!(
            "no managed Rust libraries found beneath {}",
            args.root.display()
        )));
    }

    println!(
        "Inspecting {} current managed-library versions on crates.io...",
        packages.len()
    );
    let mut unpublished = BTreeSet::new();
    let mut already_published = 0_usize;
    let mut not_publishable = 0_usize;
    for package in packages.values() {
        if !package.publishable {
            not_publishable += 1;
            continue;
        }
        match registry_state(&package.name, &package.version)? {
            RegistryState::Published => already_published += 1,
            RegistryState::Missing => {
                unpublished.insert(package.name.clone());
            }
        }
    }

    let order = publication_order(&packages, &unpublished)?;
    println!(
        "{} already published; {} unpublished; {} not publishable.",
        already_published,
        order.len(),
        not_publishable
    );
    if order.is_empty() {
        println!("Nothing to publish.");
        return Ok(());
    }
    println!("Publication order:");
    for (index, package) in order.iter().enumerate() {
        println!("  {:>2}. {} {}", index + 1, package.name, package.version);
    }

    if args.plan {
        return Ok(());
    }
    if !args.yes && !confirm(order.len())? {
        println!("Publication cancelled.");
        return Ok(());
    }

    let registry_token = vault
        .as_ref()
        .expect("non-plan publication unlocked the credential vault")
        .secret(CRATES_IO_SECRET)?
        .ok_or_else(|| {
            failure(format!(
                "credential vault has no {CRATES_IO_SECRET:?} secret"
            ))
        })?;

    for (index, planned) in order.iter().enumerate() {
        // A previous process may have completed this release after our initial scan.
        if registry_state(&planned.name, &planned.version)? == RegistryState::Published {
            println!(
                "[{}/{}] {} {} became available; skipping.",
                index + 1,
                order.len(),
                planned.name,
                planned.version
            );
            continue;
        }

        let (current, library) =
            open_package(&args.root, &planned.name, registry_token.expose_secret())?;
        if current != *planned {
            return Err(failure(format!(
                "{} changed after the publication plan was built; rerun the utility",
                planned.name
            )));
        }

        println!(
            "[{}/{}] Publishing {} {}...",
            index + 1,
            order.len(),
            planned.name,
            planned.version
        );
        library.publish().map_err(|error| {
            failure(format!(
                "publishing {} {} failed: {error}",
                planned.name, planned.version
            ))
        })?;
        wait_until_registry_ready(planned, args.registry_timeout)?;
        println!("         Registry ready.");
    }

    println!("All planned managed-library versions are published.");
    Ok(())
}

fn parse_args() -> Result<Option<Args>> {
    let mut parsed = Args {
        plan: false,
        yes: false,
        root: PathBuf::from(DEFAULT_ROOT),
        vault: PathBuf::from(DEFAULT_VAULT),
        registry_timeout: Duration::from_secs(DEFAULT_REGISTRY_TIMEOUT_SECONDS),
    };
    let mut arguments = env::args_os().skip(1);
    while let Some(argument) = arguments.next() {
        let argument = argument
            .into_string()
            .map_err(|_| failure("arguments must be valid UTF-8"))?;
        match argument.as_str() {
            "-h" | "--help" => return Ok(None),
            "--plan" => parsed.plan = true,
            "-y" | "--yes" => parsed.yes = true,
            "--root" => parsed.root = next_path(&mut arguments, "--root")?,
            "--vault" => parsed.vault = next_path(&mut arguments, "--vault")?,
            "--registry-timeout-seconds" => {
                let value = next_string(&mut arguments, "--registry-timeout-seconds")?;
                let seconds = value.parse::<u64>().map_err(|_| {
                    failure("--registry-timeout-seconds must be a positive integer")
                })?;
                if seconds == 0 {
                    return Err(failure(
                        "--registry-timeout-seconds must be a positive integer",
                    ));
                }
                parsed.registry_timeout = Duration::from_secs(seconds);
            }
            _ => {
                return Err(failure(format!(
                    "unknown argument {argument:?}; use --help"
                )));
            }
        }
    }
    if parsed.plan && parsed.yes {
        return Err(failure(
            "--yes has no effect with --plan; remove one of them",
        ));
    }
    Ok(Some(parsed))
}

fn next_path(
    arguments: &mut impl Iterator<Item = std::ffi::OsString>,
    flag: &str,
) -> Result<PathBuf> {
    let value = arguments
        .next()
        .ok_or_else(|| failure(format!("{flag} requires a path")))?;
    if value.is_empty() {
        return Err(failure(format!("{flag} requires a nonempty path")));
    }
    Ok(PathBuf::from(value))
}

fn next_string(
    arguments: &mut impl Iterator<Item = std::ffi::OsString>,
    flag: &str,
) -> Result<String> {
    let value = arguments
        .next()
        .ok_or_else(|| failure(format!("{flag} requires a value")))?
        .into_string()
        .map_err(|_| failure(format!("{flag} must be valid UTF-8")))?;
    if value.is_empty() {
        return Err(failure(format!("{flag} requires a nonempty value")));
    }
    Ok(value)
}

fn print_usage() {
    println!(
        "Usage: publish-kcode-libs [OPTIONS]\n\
         \n\
         Publish every unpublished current managed Rust library in dependency order.\n\
         Kennedy may remain running. Exact versions already on crates.io are skipped.\n\
         \n\
         Options:\n\
           --plan                       Show the resumable publication plan only\n\
           -y, --yes                    Publish without the confirmation prompt\n\
           --root PATH                  Managed libraries root [{DEFAULT_ROOT}]\n\
           --vault PATH                 Kennedy credential vault [{DEFAULT_VAULT}]\n\
           --registry-timeout-seconds N Registry propagation timeout [{DEFAULT_REGISTRY_TIMEOUT_SECONDS}]\n\
           -h, --help                   Show this help"
    );
}

fn discover_packages(root: &Path) -> Result<BTreeMap<String, Package>> {
    let mut names = Vec::new();
    let entries = fs::read_dir(root).map_err(|error| {
        failure(format!(
            "could not read managed libraries root {}: {error}",
            root.display()
        ))
    })?;
    for entry in entries {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if !file_type.is_dir() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if name.starts_with("kcode-") && entry.path().join("HEAD").is_file() {
            names.push(name);
        }
    }
    names.sort();

    let mut packages = BTreeMap::new();
    for name in names {
        let (package, _library) = open_package(root, &name, PLAN_TOKEN)?;
        if packages.insert(package.name.clone(), package).is_some() {
            return Err(failure(
                "two managed libraries declare the same package name",
            ));
        }
    }
    Ok(packages)
}

fn open_package(root: &Path, name: &str, token: &str) -> Result<(Package, Lib)> {
    let library = kcode_rust_libs_v2::open(root, name, token)
        .map_err(|error| failure(format!("could not open managed library {name}: {error}")))?;
    let source = library
        .files
        .iter()
        .map(|file| (file.path.clone(), file.contents.clone()))
        .collect();
    let manifest = library
        .files
        .iter()
        .find(|file| file.path == "Cargo.toml")
        .ok_or_else(|| failure(format!("managed library {name} has no Cargo.toml")))?
        .contents
        .clone();
    let mut package = parse_package(name, manifest)?;
    package.source = source;
    Ok((package, library))
}

fn parse_package(expected_name: &str, manifest: String) -> Result<Package> {
    let document: Value = toml::from_str(&manifest).map_err(|error| {
        failure(format!(
            "could not parse {expected_name}'s Cargo.toml: {error}"
        ))
    })?;
    let package = document
        .get("package")
        .and_then(Value::as_table)
        .ok_or_else(|| {
            failure(format!(
                "{expected_name}'s Cargo.toml has no [package] table"
            ))
        })?;
    let name = package
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| failure(format!("{expected_name}'s package name is not a string")))?
        .to_owned();
    if name != expected_name {
        return Err(failure(format!(
            "managed library directory {expected_name:?} declares package {name:?}"
        )));
    }
    let version = package
        .get("version")
        .and_then(Value::as_str)
        .ok_or_else(|| failure(format!("{name}'s package version is not a string")))?
        .to_owned();
    let publishable = match package.get("publish") {
        Some(Value::Boolean(false)) => false,
        Some(Value::Array(registries)) => registries
            .iter()
            .any(|registry| registry.as_str().is_some_and(|value| value == "crates-io")),
        _ => true,
    };
    let dependencies = manifest_dependencies(&document, &name)?;
    Ok(Package {
        name,
        version,
        dependencies,
        manifest,
        publishable,
        source: Vec::new(),
    })
}

fn manifest_dependencies(document: &Value, package_name: &str) -> Result<BTreeSet<String>> {
    let mut dependencies = BTreeSet::new();
    let workspace_dependencies = document
        .get("workspace")
        .and_then(|workspace| workspace.get("dependencies"))
        .and_then(Value::as_table);
    collect_dependency_sections(
        document,
        workspace_dependencies,
        package_name,
        &mut dependencies,
    )?;
    if let Some(targets) = document.get("target").and_then(Value::as_table) {
        for target in targets.values() {
            collect_dependency_sections(
                target,
                workspace_dependencies,
                package_name,
                &mut dependencies,
            )?;
        }
    }
    Ok(dependencies)
}

fn collect_dependency_sections(
    value: &Value,
    workspace_dependencies: Option<&toml::map::Map<String, Value>>,
    package_name: &str,
    output: &mut BTreeSet<String>,
) -> Result<()> {
    for section in ["dependencies", "build-dependencies", "dev-dependencies"] {
        let Some(table) = value.get(section).and_then(Value::as_table) else {
            continue;
        };
        for (alias, declaration) in table {
            let resolved = if declaration
                .as_table()
                .and_then(|fields| fields.get("workspace"))
                .and_then(Value::as_bool)
                == Some(true)
            {
                workspace_dependencies
                    .and_then(|dependencies| dependencies.get(alias))
                    .ok_or_else(|| {
                        failure(format!(
                            "{package_name} inherits unknown workspace dependency {alias:?}"
                        ))
                    })?
            } else {
                declaration
            };
            let dependency_name = resolved
                .as_table()
                .and_then(|fields| fields.get("package"))
                .and_then(Value::as_str)
                .unwrap_or(alias);
            output.insert(dependency_name.to_owned());
        }
    }
    Ok(())
}

fn publication_order(
    packages: &BTreeMap<String, Package>,
    unpublished: &BTreeSet<String>,
) -> Result<Vec<Package>> {
    let mut indegree = unpublished
        .iter()
        .map(|name| (name.clone(), 0_usize))
        .collect::<BTreeMap<_, _>>();
    let mut dependents = BTreeMap::<String, BTreeSet<String>>::new();
    for name in unpublished {
        let package = packages
            .get(name)
            .ok_or_else(|| failure(format!("publication candidate {name:?} was not discovered")))?;
        for dependency in package
            .dependencies
            .iter()
            .filter(|dependency| unpublished.contains(*dependency))
        {
            *indegree.get_mut(name).expect("candidate has indegree") += 1;
            dependents
                .entry(dependency.clone())
                .or_default()
                .insert(name.clone());
        }
    }

    let mut ready = indegree
        .iter()
        .filter_map(|(name, degree)| (*degree == 0).then_some(name.clone()))
        .collect::<BTreeSet<_>>();
    let mut order = Vec::with_capacity(unpublished.len());
    while let Some(name) = ready.pop_first() {
        order.push(packages[&name].clone());
        if let Some(children) = dependents.get(&name) {
            for child in children {
                let degree = indegree.get_mut(child).expect("dependent has indegree");
                *degree -= 1;
                if *degree == 0 {
                    ready.insert(child.clone());
                }
            }
        }
    }

    if order.len() != unpublished.len() {
        let cycle = indegree
            .into_iter()
            .filter_map(|(name, degree)| (degree > 0).then_some(name))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(failure(format!(
            "unpublished managed libraries contain a dependency cycle: {cycle}"
        )));
    }
    Ok(order)
}

fn registry_state(name: &str, version: &str) -> Result<RegistryState> {
    let url = format!("https://crates.io/api/v1/crates/{name}/{version}");
    let output = Command::new("curl")
        .args([
            "--silent",
            "--show-error",
            "--connect-timeout",
            "10",
            "--max-time",
            "30",
            "--output",
            "/dev/null",
            "--write-out",
            "%{http_code}",
            "--user-agent",
            USER_AGENT,
            &url,
        ])
        .output()
        .map_err(|error| failure(format!("could not start curl for crates.io: {error}")))?;
    if !output.status.success() {
        let detail = String::from_utf8_lossy(&output.stderr);
        return Err(failure(format!(
            "crates.io lookup for {name} {version} failed: {}",
            detail.trim()
        )));
    }
    match String::from_utf8_lossy(&output.stdout).trim() {
        "200" => Ok(RegistryState::Published),
        "404" => Ok(RegistryState::Missing),
        status => Err(failure(format!(
            "crates.io lookup for {name} {version} returned HTTP {status}"
        ))),
    }
}

fn cargo_sees_version(package: &Package) -> Result<bool> {
    let status = Command::new("cargo")
        .args([
            "info",
            "--quiet",
            "--color=never",
            "--registry=crates-io",
            &format!("{}@{}", package.name, package.version),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|error| failure(format!("could not start cargo info: {error}")))?;
    Ok(status.success())
}

fn wait_until_registry_ready(package: &Package, timeout: Duration) -> Result<()> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| failure("registry propagation timeout is too large"))?;
    loop {
        let api_ready = registry_state(&package.name, &package.version)
            .is_ok_and(|state| state == RegistryState::Published);
        if api_ready && cargo_sees_version(package)? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(failure(format!(
                "{} {} was uploaded but did not become registry-visible within {} seconds; rerun to resume safely",
                package.name,
                package.version,
                timeout.as_secs()
            )));
        }
        thread::sleep(
            REGISTRY_POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())),
        );
    }
}

fn confirm(count: usize) -> Result<bool> {
    if !io::stdin().is_terminal() {
        return Err(failure(format!(
            "refusing to publish {count} crates without an interactive confirmation; pass --yes to confirm noninteractively"
        )));
    }
    loop {
        eprint!("Publish these {count} immutable crate versions? [y/n] ");
        io::stderr().flush()?;
        let mut response = String::new();
        if io::stdin().read_line(&mut response)? == 0 {
            return Err(failure(
                "confirmation input closed before y or n was entered",
            ));
        }
        match parse_confirmation(&response) {
            Some(confirmed) => return Ok(confirmed),
            None => eprintln!("Please enter y or n."),
        }
    }
}

fn parse_confirmation(response: &str) -> Option<bool> {
    match response.trim() {
        "y" | "Y" => Some(true),
        "n" | "N" => Some(false),
        _ => None,
    }
}

fn failure(message: impl Into<String>) -> Box<dyn Error> {
    io::Error::other(message.into()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn package(name: &str, dependencies: &[&str]) -> Package {
        Package {
            name: name.to_owned(),
            version: "1.0.0".to_owned(),
            dependencies: dependencies
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            manifest: name.to_owned(),
            publishable: true,
            source: Vec::new(),
        }
    }

    #[test]
    fn parses_renamed_target_and_workspace_dependencies() {
        let manifest = r#"
[package]
name = "kcode-example"
version = "1.2.3"

[dependencies]
ordinary = "1"
alias = { package = "kcode-renamed", version = "1" }
shared = { workspace = true }

[target.'cfg(unix)'.build-dependencies]
kcode-platform = "2"

[workspace.dependencies]
shared = { package = "kcode-shared", version = "3" }
"#;
        let parsed = parse_package("kcode-example", manifest.to_owned()).unwrap();
        assert_eq!(
            parsed.dependencies,
            BTreeSet::from([
                "kcode-platform".to_owned(),
                "kcode-renamed".to_owned(),
                "kcode-shared".to_owned(),
                "ordinary".to_owned(),
            ])
        );
    }

    #[test]
    fn empty_publish_registry_list_is_not_publishable() {
        let manifest = r#"
[package]
name = "kcode-private"
version = "1.0.0"
publish = []
"#;
        assert!(
            !parse_package("kcode-private", manifest.to_owned())
                .unwrap()
                .publishable
        );
    }

    #[test]
    fn orders_dependencies_before_dependents_deterministically() {
        let packages = BTreeMap::from([
            ("a".to_owned(), package("a", &["b", "c"])),
            ("b".to_owned(), package("b", &["c"])),
            ("c".to_owned(), package("c", &[])),
            ("d".to_owned(), package("d", &[])),
        ]);
        let unpublished = packages.keys().cloned().collect();
        let names = publication_order(&packages, &unpublished)
            .unwrap()
            .into_iter()
            .map(|package| package.name)
            .collect::<Vec<_>>();
        assert_eq!(names, ["c", "b", "a", "d"]);
    }

    #[test]
    fn ignores_dependencies_that_are_already_published() {
        let packages = BTreeMap::from([
            ("a".to_owned(), package("a", &["b"])),
            ("b".to_owned(), package("b", &[])),
        ]);
        let unpublished = BTreeSet::from(["a".to_owned()]);
        let order = publication_order(&packages, &unpublished).unwrap();
        assert_eq!(order[0].name, "a");
    }

    #[test]
    fn reports_dependency_cycles() {
        let packages = BTreeMap::from([
            ("a".to_owned(), package("a", &["b"])),
            ("b".to_owned(), package("b", &["a"])),
        ]);
        let unpublished = packages.keys().cloned().collect();
        let error = publication_order(&packages, &unpublished).unwrap_err();
        assert!(error.to_string().contains("a, b"));
    }

    #[test]
    fn confirmation_requires_an_explicit_y_or_n() {
        assert_eq!(parse_confirmation(" y\n"), Some(true));
        assert_eq!(parse_confirmation("N\n"), Some(false));
        assert_eq!(parse_confirmation(""), None);
        assert_eq!(parse_confirmation("yes"), None);
        assert_eq!(parse_confirmation("no"), None);
    }
}
