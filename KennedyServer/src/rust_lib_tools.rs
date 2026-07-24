use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

use axum::http::StatusCode;
use kcode_rust_libs_v2::{Error as RustLibError, File as RustLibFile, Lib as OpenedRustLib};
use serde::Deserialize;
use serde_json::Value;
use zeroize::Zeroizing;

pub(crate) const CREATE_RUST_LIB_TOOL: &str = "kcode-rust-libs-v2/create";
pub(crate) const OPEN_RUST_LIB_TOOL: &str = "kcode-rust-libs-v2/open";
pub(crate) const DOCS_RUST_LIB_TOOL: &str = "kcode-rust-libs-v2/docs";
pub(crate) const WRITE_RUST_LIB_TOOL: &str = "kcode-rust-libs-v2/write";
pub(crate) const WRITE_FILE_FREEFORM_RUST_LIB_TOOL: &str = "kcode-rust-libs-v2/write-file-freeform";
pub(crate) const DELETE_FILE_RUST_LIB_TOOL: &str = "kcode-rust-libs-v2/delete-file";
pub(crate) const CHECK_RUST_LIB_TOOL: &str = "kcode-rust-libs-v2/check";
pub(crate) const PUBLISH_RUST_LIB_TOOL: &str = "kcode-rust-libs-v2/publish";
pub(crate) const PREVIEW_WRITE_FILE_RUST_LIB_TOOL: &str =
    "kcode-rust-libs-v2/internal-preview-write-file";
pub(crate) const RUST_LIB_TOOLS: [&str; 8] = [
    CREATE_RUST_LIB_TOOL,
    OPEN_RUST_LIB_TOOL,
    DOCS_RUST_LIB_TOOL,
    WRITE_RUST_LIB_TOOL,
    WRITE_FILE_FREEFORM_RUST_LIB_TOOL,
    DELETE_FILE_RUST_LIB_TOOL,
    CHECK_RUST_LIB_TOOL,
    PUBLISH_RUST_LIB_TOOL,
];

const SESSION_LEASE: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_SESSION_ID_BYTES: usize = 128;

#[derive(Clone)]
pub(crate) struct RustLibToolService {
    inner: Arc<ServiceInner>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LibrarySnapshot {
    pub(crate) name: String,
    pub(crate) text: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ToolExecution {
    pub(crate) text: String,
    pub(crate) snapshot: Option<LibrarySnapshot>,
}

struct ServiceInner {
    root: PathBuf,
    registry_token: Zeroizing<String>,
    registry: Arc<RegistryState>,
}

struct RegistryState {
    entries: Mutex<Registry>,
    changed: Condvar,
}

#[derive(Default)]
struct Registry {
    libraries: HashMap<HandleKey, OpenLibrary>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct HandleKey {
    session_id: String,
    library_name: String,
}

struct OpenLibrary {
    handle: Arc<Mutex<OpenedRustLib>>,
    active_operations: usize,
    closing: bool,
    last_used: Instant,
}

struct ActiveOperation {
    registry: Arc<RegistryState>,
    key: HandleKey,
}

impl Drop for ActiveOperation {
    fn drop(&mut self) {
        let Ok(mut registry) = self.registry.entries.lock() else {
            return;
        };
        let Some(entry) = registry.libraries.get_mut(&self.key) else {
            return;
        };
        if entry.active_operations == 0 {
            return;
        }
        entry.active_operations -= 1;
        self.registry.changed.notify_all();
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NameArguments {
    name: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteArguments {
    name: String,
    files: Vec<WriteFile>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteFile {
    path: String,
    contents: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SingleFileWriteArguments {
    name: String,
    path: String,
    contents: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteFileArguments {
    name: String,
    path: String,
}

#[derive(Debug)]
pub(crate) struct ToolError {
    pub(crate) status: StatusCode,
    pub(crate) code: &'static str,
    pub(crate) message: String,
}

impl ToolError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_arguments",
            message: message.into(),
        }
    }

    fn not_found(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code,
            message: message.into(),
        }
    }

    fn conflict(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code,
            message: message.into(),
        }
    }

    fn unprocessable(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            code,
            message: message.into(),
        }
    }

    fn unavailable(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "rust_lib_internal_error",
            message: message.into(),
        }
    }
}

impl RustLibToolService {
    pub(crate) fn new(
        root: impl AsRef<Path>,
        crates_io_registry_token: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let registry_token = crates_io_registry_token.into();
        let registry_token = registry_token.trim();
        anyhow::ensure!(
            !registry_token.is_empty(),
            "the crates.io registry token is empty"
        );

        let root = root.as_ref();
        std::fs::create_dir_all(root).map_err(|error| {
            anyhow::anyhow!(
                "creating managed Rust libraries root {}: {error}",
                root.display()
            )
        })?;
        let metadata = std::fs::symlink_metadata(root).map_err(|error| {
            anyhow::anyhow!(
                "inspecting managed Rust libraries root {}: {error}",
                root.display()
            )
        })?;
        anyhow::ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "managed Rust libraries root {} must be a real directory",
            root.display()
        );
        let root = std::fs::canonicalize(root).map_err(|error| {
            anyhow::anyhow!(
                "canonicalizing managed Rust libraries root {}: {error}",
                root.display()
            )
        })?;

        Ok(Self {
            inner: Arc::new(ServiceInner {
                root,
                registry_token: Zeroizing::new(registry_token.to_owned()),
                registry: Arc::new(RegistryState {
                    entries: Mutex::new(Registry::default()),
                    changed: Condvar::new(),
                }),
            }),
        })
    }

    #[cfg(test)]
    async fn execute(
        &self,
        session_id: String,
        name: String,
        arguments: Value,
    ) -> Result<String, ToolError> {
        self.execute_detailed(session_id, name, arguments)
            .await
            .map(|execution| execution.text)
    }

    pub(crate) async fn execute_detailed(
        &self,
        session_id: String,
        name: String,
        arguments: Value,
    ) -> Result<ToolExecution, ToolError> {
        validate_session_id(&session_id)?;
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.execute_blocking(&session_id, &name, arguments))
            .await
            .map_err(|error| {
                tracing::error!(error=%error, "Rust library tool worker failed");
                ToolError::internal("The Rust library worker stopped unexpectedly.")
            })?
    }

    fn execute_blocking(
        &self,
        session_id: &str,
        tool_name: &str,
        arguments: Value,
    ) -> Result<ToolExecution, ToolError> {
        self.remove_expired_handles()?;
        match tool_name {
            CREATE_RUST_LIB_TOOL => {
                let arguments: NameArguments = parse_arguments(arguments)?;
                validate_library_name(&arguments.name)?;
                self.create(session_id, &arguments.name)
            }
            OPEN_RUST_LIB_TOOL => {
                let arguments: NameArguments = parse_arguments(arguments)?;
                validate_library_name(&arguments.name)?;
                self.open(session_id, &arguments.name)
            }
            DOCS_RUST_LIB_TOOL => {
                let arguments: NameArguments = parse_arguments(arguments)?;
                validate_library_name(&arguments.name)?;
                let (version, documentation) =
                    kcode_rust_libs_v2::docs(&self.inner.root, &arguments.name)
                        .map_err(|error| self.map_library_error(error))?;
                Ok(ToolExecution::plain(format!(
                    "Rust library: {}\nVersion: {version}\n\n{documentation}",
                    arguments.name
                )))
            }
            WRITE_RUST_LIB_TOOL => {
                let arguments: WriteArguments = parse_arguments(arguments)?;
                validate_library_name(&arguments.name)?;
                let files = arguments
                    .files
                    .into_iter()
                    .map(|file| RustLibFile {
                        path: file.path,
                        contents: file.contents,
                    })
                    .collect::<Vec<_>>();
                let result_name = arguments.name.clone();
                self.with_open_library(session_id, &arguments.name, move |rust_lib| {
                    let previous = std::mem::replace(&mut rust_lib.files, files);
                    if let Err(error) = rust_lib.write() {
                        rust_lib.files = previous;
                        return Err(self.map_library_error(error));
                    }
                    let paths = rust_lib
                        .files
                        .iter()
                        .map(|file| format!("- {}", file.path))
                        .collect::<Vec<_>>()
                        .join("\n");
                    let text = format!(
                        "Wrote {} files to Rust library {result_name}.\n{paths}",
                        rust_lib.files.len()
                    );
                    Ok(ToolExecution::with_snapshot(
                        text,
                        LibrarySnapshot::new(&result_name, &rust_lib.files),
                    ))
                })
            }
            WRITE_FILE_FREEFORM_RUST_LIB_TOOL => {
                let arguments: SingleFileWriteArguments = parse_arguments(arguments)?;
                validate_library_name(&arguments.name)?;
                let result_name = arguments.name.clone();
                let result_path = arguments.path.clone();
                self.with_open_library(session_id, &arguments.name, move |rust_lib| {
                    let previous = rust_lib.files.clone();
                    upsert_file(&mut rust_lib.files, arguments.path, arguments.contents);
                    if let Err(error) = rust_lib.write() {
                        rust_lib.files = previous;
                        return Err(self.map_library_error(error));
                    }
                    Ok(ToolExecution::with_snapshot(
                        format!("Wrote file {result_path} in Rust library {result_name}."),
                        LibrarySnapshot::new(&result_name, &rust_lib.files),
                    ))
                })
            }
            PREVIEW_WRITE_FILE_RUST_LIB_TOOL => {
                let arguments: SingleFileWriteArguments = parse_arguments(arguments)?;
                validate_library_name(&arguments.name)?;
                let result_name = arguments.name.clone();
                self.with_open_library(session_id, &arguments.name, move |rust_lib| {
                    let mut files = rust_lib.files.clone();
                    upsert_file(&mut files, arguments.path, arguments.contents);
                    Ok(ToolExecution::snapshot(LibrarySnapshot::new(
                        &result_name,
                        &files,
                    )))
                })
            }
            DELETE_FILE_RUST_LIB_TOOL => {
                let arguments: DeleteFileArguments = parse_arguments(arguments)?;
                validate_library_name(&arguments.name)?;
                let result_name = arguments.name.clone();
                let result_path = arguments.path.clone();
                self.with_open_library(session_id, &arguments.name, move |rust_lib| {
                    let Some(index) = rust_lib
                        .files
                        .iter()
                        .position(|file| file.path == arguments.path)
                    else {
                        return Err(ToolError::not_found(
                            "rust_lib_file_not_found",
                            format!(
                                "File {:?} does not exist in Rust library {:?}.",
                                arguments.path, result_name
                            ),
                        ));
                    };
                    let previous = rust_lib.files.clone();
                    rust_lib.files.remove(index);
                    if let Err(error) = rust_lib.write() {
                        rust_lib.files = previous;
                        return Err(self.map_library_error(error));
                    }
                    Ok(ToolExecution::with_snapshot(
                        format!("Deleted file {result_path} from Rust library {result_name}."),
                        LibrarySnapshot::new(&result_name, &rust_lib.files),
                    ))
                })
            }
            CHECK_RUST_LIB_TOOL => {
                let arguments: NameArguments = parse_arguments(arguments)?;
                validate_library_name(&arguments.name)?;
                self.with_open_library(session_id, &arguments.name, |rust_lib| {
                    rust_lib
                        .check()
                        .map_err(|error| self.map_library_error(error))?;
                    Ok(ToolExecution::plain(format!(
                        "Rust library {} passed its checks.",
                        arguments.name
                    )))
                })
            }
            PUBLISH_RUST_LIB_TOOL => {
                let arguments: NameArguments = parse_arguments(arguments)?;
                validate_library_name(&arguments.name)?;
                self.with_open_library(session_id, &arguments.name, |rust_lib| {
                    rust_lib
                        .publish()
                        .map_err(|error| self.map_library_error(error))?;
                    Ok(ToolExecution::plain(format!(
                        "Published Rust library {}.",
                        arguments.name
                    )))
                })
            }
            _ => Err(ToolError::invalid(format!(
                "Tool {tool_name} is not a Rust library tool."
            ))),
        }
    }

    fn create(&self, session_id: &str, name: &str) -> Result<ToolExecution, ToolError> {
        let key = HandleKey::new(session_id, name);
        let mut registry = self.inner.registry.entries.lock().map_err(|_| {
            ToolError::internal("The Rust library snapshot registry is unavailable.")
        })?;
        if registry.libraries.contains_key(&key) {
            return Err(ToolError::conflict(
                "rust_lib_already_open",
                format!("Rust library {name:?} is already open in this Kennedy session."),
            ));
        }
        let rust_lib =
            kcode_rust_libs_v2::create(&self.inner.root, name, self.inner.registry_token.as_str())
                .map_err(|error| self.map_library_error(error))?;
        let snapshot = LibrarySnapshot::new(name, &rust_lib.files);
        registry.libraries.insert(key, OpenLibrary::new(rust_lib));
        Ok(ToolExecution::snapshot(snapshot))
    }

    fn open(&self, session_id: &str, name: &str) -> Result<ToolExecution, ToolError> {
        let key = HandleKey::new(session_id, name);
        let registry = self.inner.registry.entries.lock().map_err(|_| {
            ToolError::internal("The Rust library snapshot registry is unavailable.")
        })?;
        if registry.libraries.contains_key(&key) {
            drop(registry);
            return self.with_open_library(session_id, name, |rust_lib| {
                let reopened = kcode_rust_libs_v2::open(
                    &self.inner.root,
                    name,
                    self.inner.registry_token.as_str(),
                )
                .map_err(|error| self.map_library_error(error))?;
                *rust_lib = reopened;
                Ok(ToolExecution::snapshot(LibrarySnapshot::new(
                    name,
                    &rust_lib.files,
                )))
            });
        }

        let mut registry = registry;
        let rust_lib =
            kcode_rust_libs_v2::open(&self.inner.root, name, self.inner.registry_token.as_str())
                .map_err(|error| self.map_library_error(error))?;
        let snapshot = LibrarySnapshot::new(name, &rust_lib.files);
        registry.libraries.insert(key, OpenLibrary::new(rust_lib));
        Ok(ToolExecution::snapshot(snapshot))
    }

    fn with_open_library(
        &self,
        session_id: &str,
        name: &str,
        operation: impl FnOnce(&mut OpenedRustLib) -> Result<ToolExecution, ToolError>,
    ) -> Result<ToolExecution, ToolError> {
        let (handle, active_operation) = self.begin_operation(session_id, name)?;
        let mut rust_lib = handle
            .lock()
            .map_err(|_| ToolError::internal("The opened Rust library is unavailable."))?;
        let result = operation(&mut rust_lib);
        drop(rust_lib);
        drop(active_operation);
        result
    }

    fn begin_operation(
        &self,
        session_id: &str,
        name: &str,
    ) -> Result<(Arc<Mutex<OpenedRustLib>>, ActiveOperation), ToolError> {
        let key = HandleKey::new(session_id, name);
        let mut registry = self.inner.registry.entries.lock().map_err(|_| {
            ToolError::internal("The Rust library snapshot registry is unavailable.")
        })?;
        let Some(entry) = registry.libraries.get_mut(&key) else {
            return Err(ToolError::conflict(
                "rust_lib_not_open",
                format!(
                    "Rust library {name:?} is not open in this Kennedy session. Call {OPEN_RUST_LIB_TOOL} first."
                ),
            ));
        };
        if entry.closing {
            return Err(ToolError::conflict(
                "rust_lib_session_ending",
                "The Kennedy session is releasing its Rust library snapshots.",
            ));
        }
        entry.active_operations += 1;
        entry.last_used = Instant::now();
        let handle = Arc::clone(&entry.handle);
        Ok((
            handle,
            ActiveOperation {
                registry: Arc::clone(&self.inner.registry),
                key,
            },
        ))
    }

    pub(crate) async fn release(&self, session_id: String) -> Result<usize, ToolError> {
        validate_session_id(&session_id)?;
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.release_blocking(&session_id))
            .await
            .map_err(|error| {
                tracing::error!(error=%error, "Rust library session-release worker failed");
                ToolError::internal("The Rust library worker stopped unexpectedly.")
            })?
    }

    fn release_blocking(&self, session_id: &str) -> Result<usize, ToolError> {
        let mut registry = self.inner.registry.entries.lock().map_err(|_| {
            ToolError::internal("The Rust library snapshot registry is unavailable.")
        })?;
        let keys = registry
            .libraries
            .keys()
            .filter(|key| key.session_id == session_id)
            .cloned()
            .collect::<Vec<_>>();
        for key in &keys {
            if let Some(entry) = registry.libraries.get_mut(key) {
                entry.closing = true;
            }
        }
        while keys.iter().any(|key| {
            registry
                .libraries
                .get(key)
                .is_some_and(|entry| entry.active_operations > 0)
        }) {
            registry = self.inner.registry.changed.wait(registry).map_err(|_| {
                ToolError::internal("The Rust library snapshot registry is unavailable.")
            })?;
        }
        for key in &keys {
            registry.libraries.remove(key);
        }
        Ok(keys.len())
    }

    fn remove_expired_handles(&self) -> Result<(), ToolError> {
        let mut registry = self.inner.registry.entries.lock().map_err(|_| {
            ToolError::internal("The Rust library snapshot registry is unavailable.")
        })?;
        let now = Instant::now();
        registry.libraries.retain(|_, entry| {
            entry.active_operations > 0 || now.duration_since(entry.last_used) < SESSION_LEASE
        });
        Ok(())
    }

    fn map_library_error(&self, error: RustLibError) -> ToolError {
        let rendered = error.to_string();
        let category = rendered
            .split_once(':')
            .map_or("unknown", |(value, _)| value);
        let safe = rendered.replace(
            self.inner.root.to_string_lossy().as_ref(),
            "<managed-rust-libraries>",
        );
        match category {
            "invalid_name" | "unsafe_path" | "invalid_source" | "invalid_metadata" => {
                ToolError::invalid(safe)
            }
            "already_exists" => ToolError::conflict("rust_lib_exists", safe),
            "not_found" => ToolError::not_found("rust_lib_not_found", safe),
            "stale_snapshot" => ToolError::conflict("rust_lib_stale_snapshot", safe),
            "invalid_repository" | "unsafe_source" => {
                ToolError::unprocessable("rust_lib_invalid_repository", safe)
            }
            "migration" => ToolError::unavailable("rust_lib_migration_failure", safe),
            "invalid_token" => ToolError::unavailable(
                "registry_token_unavailable",
                "Rust library publication is unavailable because the operator-provisioned crates.io token is missing or invalid.",
            ),
            value if value.starts_with("check.") => {
                ToolError::unprocessable("rust_lib_check_failed", safe)
            }
            "publish" => ToolError::unavailable("rust_lib_publish_failed", safe),
            value if value.starts_with("sandbox.") || value == "io" => {
                tracing::error!(error=%rendered, "Rust library infrastructure operation failed");
                ToolError::unavailable(
                    "rust_lib_infrastructure_failure",
                    "The Rust library operation failed in local validation or publication infrastructure.",
                )
            }
            _ => {
                tracing::error!(error=%rendered, "Rust library operation failed");
                ToolError::unavailable(
                    "rust_lib_infrastructure_failure",
                    "The Rust library operation failed unexpectedly.",
                )
            }
        }
    }
}

impl LibrarySnapshot {
    fn new(name: &str, files: &[RustLibFile]) -> Self {
        Self {
            name: name.to_owned(),
            text: library_snapshot_text(name, files),
        }
    }
}

impl ToolExecution {
    fn plain(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            snapshot: None,
        }
    }

    fn snapshot(snapshot: LibrarySnapshot) -> Self {
        Self {
            text: snapshot.text.clone(),
            snapshot: Some(snapshot),
        }
    }

    fn with_snapshot(text: impl Into<String>, snapshot: LibrarySnapshot) -> Self {
        Self {
            text: text.into(),
            snapshot: Some(snapshot),
        }
    }
}

impl HandleKey {
    fn new(session_id: &str, library_name: &str) -> Self {
        Self {
            session_id: session_id.to_owned(),
            library_name: library_name.to_owned(),
        }
    }
}

impl OpenLibrary {
    fn new(rust_lib: OpenedRustLib) -> Self {
        Self {
            handle: Arc::new(Mutex::new(rust_lib)),
            active_operations: 0,
            closing: false,
            last_used: Instant::now(),
        }
    }
}

fn parse_arguments<T: for<'de> Deserialize<'de>>(arguments: Value) -> Result<T, ToolError> {
    serde_json::from_value(arguments).map_err(|error| {
        ToolError::invalid(format!("Invalid Rust library tool arguments: {error}"))
    })
}

fn validate_session_id(session_id: &str) -> Result<(), ToolError> {
    if session_id.is_empty()
        || session_id.len() > MAX_SESSION_ID_BYTES
        || !session_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':'))
    {
        return Err(ToolError::invalid(
            "The hidden Kennedy tool session ID is invalid.",
        ));
    }
    Ok(())
}

fn validate_library_name(name: &str) -> Result<(), ToolError> {
    let mut characters = name.chars();
    if name.len() > 255
        || !characters
            .next()
            .is_some_and(|character| character.is_ascii_alphanumeric())
        || !characters
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err(ToolError::invalid(
            "name must begin with an ASCII letter or digit and contain only ASCII letters, digits, '-' or '_'.",
        ));
    }
    Ok(())
}

fn upsert_file(files: &mut Vec<RustLibFile>, path: String, contents: String) {
    if let Some(file) = files.iter_mut().find(|file| file.path == path) {
        file.contents = contents;
    } else {
        files.push(RustLibFile { path, contents });
    }
}

pub(crate) fn proposed_write_snapshot(arguments: &Value) -> Option<LibrarySnapshot> {
    let arguments: WriteArguments = serde_json::from_value(arguments.clone()).ok()?;
    validate_library_name(&arguments.name).ok()?;
    let mut files = arguments
        .files
        .into_iter()
        .map(|file| RustLibFile {
            path: file.path,
            contents: file.contents,
        })
        .collect::<Vec<_>>();
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Some(LibrarySnapshot::new(&arguments.name, &files))
}

fn library_snapshot_text(name: &str, files: &[RustLibFile]) -> String {
    let mut text = format!("Rust library: {name}\nFiles: {}", files.len());
    for file in files {
        text.push_str("\n\nFile: ");
        text.push_str(&file.path);
        text.push('\n');
        text.push_str(&file.contents);
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use uuid::Uuid;

    fn temporary_root() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("kennedy-rust-lib-tools-test-{}", Uuid::new_v4()))
    }

    fn service(root: &Path) -> RustLibToolService {
        RustLibToolService::new(root, "test-crates-io-key").unwrap()
    }

    fn complete_files(name: &str, documentation: &str, source: &str) -> Value {
        json!([
            {
                "path":"Cargo.toml",
                "contents":format!("[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2024\"\n")
            },
            {"path":"Documentation.md","contents":documentation},
            {"path":"src/lib.rs","contents":source},
        ])
    }

    #[tokio::test]
    async fn concurrent_sessions_use_stale_snapshot_detection_instead_of_exclusive_ownership() {
        let root = temporary_root();
        let service = service(&root);
        service
            .execute(
                "conversation:first".into(),
                CREATE_RUST_LIB_TOOL.into(),
                json!({"name":"example-lib"}),
            )
            .await
            .unwrap();
        service
            .execute(
                "conversation:second".into(),
                OPEN_RUST_LIB_TOOL.into(),
                json!({"name":"example-lib"}),
            )
            .await
            .unwrap();

        service
            .execute(
                "conversation:first".into(),
                WRITE_RUST_LIB_TOOL.into(),
                json!({
                    "name":"example-lib",
                    "files":complete_files("example-lib", "first\n", "pub fn first() {}\n"),
                }),
            )
            .await
            .unwrap();
        let stale = service
            .execute(
                "conversation:second".into(),
                WRITE_RUST_LIB_TOOL.into(),
                json!({
                    "name":"example-lib",
                    "files":complete_files("example-lib", "second\n", "pub fn second() {}\n"),
                }),
            )
            .await
            .unwrap_err();
        assert_eq!(stale.code, "rust_lib_stale_snapshot");

        service
            .execute(
                "conversation:second".into(),
                OPEN_RUST_LIB_TOOL.into(),
                json!({"name":"example-lib"}),
            )
            .await
            .unwrap();
        service
            .execute(
                "conversation:second".into(),
                WRITE_RUST_LIB_TOOL.into(),
                json!({
                    "name":"example-lib",
                    "files":complete_files("example-lib", "second\n", "pub fn second() {}\n"),
                }),
            )
            .await
            .unwrap();
        let docs = service
            .execute(
                "conversation:third".into(),
                DOCS_RUST_LIB_TOOL.into(),
                json!({"name":"example-lib"}),
            )
            .await
            .unwrap();
        assert_eq!(
            docs,
            "Rust library: example-lib\nVersion: 0.1.0\n\nsecond\n"
        );
        assert_eq!(
            service.release("conversation:first".into()).await.unwrap(),
            1
        );
        assert_eq!(
            service.release("conversation:second".into()).await.unwrap(),
            1
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn write_replaces_the_complete_file_set() {
        let root = temporary_root();
        let service = service(&root);
        service
            .execute(
                "conversation:test".into(),
                CREATE_RUST_LIB_TOOL.into(),
                json!({"name":"complete-lib"}),
            )
            .await
            .unwrap();
        let mut first = complete_files("complete-lib", "docs\n", "pub fn answer() -> u8 { 42 }\n")
            .as_array()
            .unwrap()
            .clone();
        first.push(json!({"path":"tests/temporary.rs","contents":"#[test]\nfn temporary() {}\n"}));
        service
            .execute(
                "conversation:test".into(),
                WRITE_RUST_LIB_TOOL.into(),
                json!({"name":"complete-lib","files":first}),
            )
            .await
            .unwrap();
        let written = service
            .execute(
                "conversation:test".into(),
                WRITE_RUST_LIB_TOOL.into(),
                json!({
                    "name":"complete-lib",
                    "files":complete_files("complete-lib", "updated\n", "pub fn answer() -> u8 { 43 }\n"),
                }),
            )
            .await
            .unwrap();
        assert_eq!(
            written,
            "Wrote 3 files to Rust library complete-lib.\n- Cargo.toml\n- Documentation.md\n- src/lib.rs"
        );

        let opened = service
            .execute(
                "conversation:test".into(),
                OPEN_RUST_LIB_TOOL.into(),
                json!({"name":"complete-lib"}),
            )
            .await
            .unwrap();
        assert!(!opened.contains("File: tests/temporary.rs"));
        assert!(opened.contains("File: src/lib.rs\npub fn answer() -> u8 { 43 }\n"));
        service.release("conversation:test".into()).await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn single_file_write_previews_creates_and_overwrites_without_replacing_other_files() {
        let root = temporary_root();
        let service = service(&root);
        service
            .execute(
                "conversation:test".into(),
                CREATE_RUST_LIB_TOOL.into(),
                json!({"name":"single-file-lib"}),
            )
            .await
            .unwrap();

        let preview = service
            .execute_detailed(
                "conversation:test".into(),
                PREVIEW_WRITE_FILE_RUST_LIB_TOOL.into(),
                json!({
                    "name":"single-file-lib",
                    "path":"src/preview.rs",
                    "contents":"pub fn preview_only() {}\n",
                }),
            )
            .await
            .unwrap()
            .snapshot
            .unwrap();
        assert!(
            preview
                .text
                .contains("File: src/preview.rs\npub fn preview_only() {}\n")
        );
        let unchanged = service
            .execute_detailed(
                "conversation:test".into(),
                OPEN_RUST_LIB_TOOL.into(),
                json!({"name":"single-file-lib"}),
            )
            .await
            .unwrap()
            .snapshot
            .unwrap();
        assert!(!unchanged.text.contains("src/preview.rs"));

        let created = service
            .execute_detailed(
                "conversation:test".into(),
                WRITE_FILE_FREEFORM_RUST_LIB_TOOL.into(),
                json!({
                    "name":"single-file-lib",
                    "path":"src/additional.rs",
                    "contents":"pub fn additional() -> u8 { 1 }\n",
                }),
            )
            .await
            .unwrap();
        assert_eq!(
            created.text,
            "Wrote file src/additional.rs in Rust library single-file-lib."
        );
        let created_snapshot = created.snapshot.unwrap();
        assert!(
            created_snapshot
                .text
                .contains("File: src/additional.rs\npub fn additional() -> u8 { 1 }\n")
        );
        assert!(created_snapshot.text.contains("File: Cargo.toml\n"));
        assert!(created_snapshot.text.contains("File: Documentation.md\n"));

        let overwritten = service
            .execute_detailed(
                "conversation:test".into(),
                WRITE_FILE_FREEFORM_RUST_LIB_TOOL.into(),
                json!({
                    "name":"single-file-lib",
                    "path":"src/additional.rs",
                    "contents":"pub fn additional() -> u8 { 2 }\n",
                }),
            )
            .await
            .unwrap()
            .snapshot
            .unwrap();
        assert!(
            overwritten
                .text
                .contains("File: src/additional.rs\npub fn additional() -> u8 { 2 }\n")
        );
        assert!(!overwritten.text.contains("additional() -> u8 { 1 }"));

        service.release("conversation:test".into()).await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn delete_file_removes_one_file_and_rolls_back_failed_deletions() {
        let root = temporary_root();
        let service = service(&root);
        service
            .execute(
                "conversation:test".into(),
                CREATE_RUST_LIB_TOOL.into(),
                json!({"name":"delete-file-lib"}),
            )
            .await
            .unwrap();
        service
            .execute(
                "conversation:test".into(),
                WRITE_FILE_FREEFORM_RUST_LIB_TOOL.into(),
                json!({
                    "name":"delete-file-lib",
                    "path":"src/temporary.rs",
                    "contents":"pub fn temporary() {}\n",
                }),
            )
            .await
            .unwrap();

        let deleted = service
            .execute_detailed(
                "conversation:test".into(),
                DELETE_FILE_RUST_LIB_TOOL.into(),
                json!({"name":"delete-file-lib","path":"src/temporary.rs"}),
            )
            .await
            .unwrap();
        assert_eq!(
            deleted.text,
            "Deleted file src/temporary.rs from Rust library delete-file-lib."
        );
        assert!(!deleted.snapshot.unwrap().text.contains("src/temporary.rs"));

        let missing = service
            .execute(
                "conversation:test".into(),
                DELETE_FILE_RUST_LIB_TOOL.into(),
                json!({"name":"delete-file-lib","path":"src/temporary.rs"}),
            )
            .await
            .unwrap_err();
        assert_eq!(missing.code, "rust_lib_file_not_found");

        let mandatory = service
            .execute(
                "conversation:test".into(),
                DELETE_FILE_RUST_LIB_TOOL.into(),
                json!({"name":"delete-file-lib","path":"Cargo.toml"}),
            )
            .await
            .unwrap_err();
        assert_eq!(mandatory.code, "invalid_arguments");
        let reopened = service
            .execute_detailed(
                "conversation:test".into(),
                OPEN_RUST_LIB_TOOL.into(),
                json!({"name":"delete-file-lib"}),
            )
            .await
            .unwrap()
            .snapshot
            .unwrap();
        assert!(reopened.text.contains("File: Cargo.toml\n"));

        service.release("conversation:test".into()).await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn single_file_tools_require_the_named_library_to_be_open() {
        let root = temporary_root();
        let service = service(&root);
        let write = service
            .execute(
                "conversation:test".into(),
                WRITE_FILE_FREEFORM_RUST_LIB_TOOL.into(),
                json!({
                    "name":"closed-lib",
                    "path":"src/lib.rs",
                    "contents":"pub fn replacement() {}\n",
                }),
            )
            .await
            .unwrap_err();
        assert_eq!(write.code, "rust_lib_not_open");
        let delete = service
            .execute(
                "conversation:test".into(),
                DELETE_FILE_RUST_LIB_TOOL.into(),
                json!({"name":"closed-lib","path":"src/lib.rs"}),
            )
            .await
            .unwrap_err();
        assert_eq!(delete.code, "rust_lib_not_open");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn detailed_create_open_and_write_results_carry_the_current_snapshot() {
        let root = temporary_root();
        let service = service(&root);
        let created = service
            .execute_detailed(
                "conversation:test".into(),
                CREATE_RUST_LIB_TOOL.into(),
                json!({"name":"snapshot-lib"}),
            )
            .await
            .unwrap();
        assert_eq!(
            created
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.name.as_str()),
            Some("snapshot-lib")
        );
        assert_eq!(
            created.text,
            created.snapshot.as_ref().unwrap().text,
            "create returns the same source installed into Chatend"
        );

        let written = service
            .execute_detailed(
                "conversation:test".into(),
                WRITE_RUST_LIB_TOOL.into(),
                json!({
                    "name":"snapshot-lib",
                    "files":complete_files(
                        "snapshot-lib",
                        "updated docs\n",
                        "pub fn current_revision() {}\n"
                    ),
                }),
            )
            .await
            .unwrap();
        assert!(written.text.starts_with("Wrote 3 files"));
        let snapshot = written.snapshot.unwrap();
        assert!(snapshot.text.contains("updated docs\n"));
        assert!(
            snapshot
                .text
                .contains("File: src/lib.rs\npub fn current_revision() {}\n")
        );

        let reopened = service
            .execute_detailed(
                "conversation:test".into(),
                OPEN_RUST_LIB_TOOL.into(),
                json!({"name":"snapshot-lib"}),
            )
            .await
            .unwrap();
        assert_eq!(reopened.snapshot.unwrap().text, snapshot.text);
        service.release("conversation:test".into()).await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn docs_migrates_a_flat_legacy_library_without_opening_a_session_snapshot() {
        let root = temporary_root();
        let repository = root.join("legacy-lib");
        std::fs::create_dir_all(repository.join("src")).unwrap();
        std::fs::write(
            repository.join("Cargo.toml"),
            "[package]\nname = \"legacy-lib\"\nversion = \"2.3.4\"\nedition = \"2024\"\n",
        )
        .unwrap();
        std::fs::write(repository.join("Documentation.md"), "legacy docs\n").unwrap();
        std::fs::write(repository.join("src/lib.rs"), "pub fn legacy() {}\n").unwrap();
        std::fs::write(repository.join("Cargo.lock"), "version = 4\n").unwrap();

        let service = service(&root);
        let docs = service
            .execute(
                "conversation:test".into(),
                DOCS_RUST_LIB_TOOL.into(),
                json!({"name":"legacy-lib"}),
            )
            .await
            .unwrap();
        assert_eq!(
            docs,
            "Rust library: legacy-lib\nVersion: 2.3.4\n\nlegacy docs\n"
        );
        assert!(repository.join("HEAD").is_file());
        assert!(repository.join(".lock").is_file());
        assert!(repository.join("generations").is_dir());
        assert_eq!(
            service.release("conversation:test".into()).await.unwrap(),
            0
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
