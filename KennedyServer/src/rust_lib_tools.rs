use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use kcode_rust_libs::{
    CheckResult, Error as RustLibError, KcodeRustLibs, OpenedRustLib, RustLibFile, RustLibPath,
};
use serde::Deserialize;
use serde_json::{Value, json};

const SESSION_LEASE: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_SESSION_ID_BYTES: usize = 128;

#[derive(Clone)]
pub(crate) struct RustLibToolService {
    inner: Arc<ServiceInner>,
}

struct ServiceInner {
    rust_libs: KcodeRustLibs,
    registry: Arc<RegistryState>,
}

struct RegistryState {
    entries: Mutex<Registry>,
    changed: Condvar,
}

#[derive(Default)]
struct Registry {
    libraries: HashMap<String, OwnedLibrary>,
}

struct OwnedLibrary {
    session_id: String,
    handle: Arc<Mutex<OpenedRustLib>>,
    active_operations: usize,
    closing: bool,
    last_used: Instant,
}

struct ActiveOperation {
    registry: Arc<RegistryState>,
    library_name: String,
    session_id: String,
}

impl Drop for ActiveOperation {
    fn drop(&mut self) {
        let Ok(mut registry) = self.registry.entries.lock() else {
            return;
        };
        let Some(entry) = registry.libraries.get_mut(&self.library_name) else {
            return;
        };
        if entry.session_id != self.session_id || entry.active_operations == 0 {
            return;
        }
        entry.active_operations -= 1;
        self.registry.changed.notify_all();
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecuteRequest {
    session_id: String,
    name: String,
    arguments: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleaseRequest {
    session_id: String,
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

#[derive(Debug)]
struct ToolError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ToolError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_arguments",
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

impl IntoResponse for ToolError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({"error":{"code":self.code,"message":self.message}})),
        )
            .into_response()
    }
}

impl RustLibToolService {
    pub(crate) fn new(root: impl AsRef<Path>) -> kcode_rust_libs::Result<Self> {
        Ok(Self {
            inner: Arc::new(ServiceInner {
                rust_libs: KcodeRustLibs::new(root.as_ref())?,
                registry: Arc::new(RegistryState {
                    entries: Mutex::new(Registry::default()),
                    changed: Condvar::new(),
                }),
            }),
        })
    }

    async fn execute(
        &self,
        session_id: String,
        name: String,
        arguments: Value,
    ) -> Result<Value, ToolError> {
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
    ) -> Result<Value, ToolError> {
        self.remove_expired_handles()?;
        match tool_name {
            "CreateRustLib" => {
                let arguments: NameArguments = parse_arguments(arguments)?;
                validate_library_name(&arguments.name)?;
                self.create_or_open(session_id, &arguments.name, true)
            }
            "OpenRustLib" => {
                let arguments: NameArguments = parse_arguments(arguments)?;
                validate_library_name(&arguments.name)?;
                self.create_or_open(session_id, &arguments.name, false)
            }
            "WriteRustLib" => {
                let arguments: WriteArguments = parse_arguments(arguments)?;
                validate_library_name(&arguments.name)?;
                if arguments.files.is_empty() {
                    return Err(ToolError::invalid("files must contain at least one file."));
                }
                let files = arguments
                    .files
                    .into_iter()
                    .map(|file| {
                        RustLibPath::new(file.path)
                            .map(|path| RustLibFile::new(path, file.contents))
                            .map_err(map_library_error)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                self.with_open_library(session_id, &arguments.name, |rust_lib| {
                    rust_lib.write(&files).map_err(map_library_error)?;
                    Ok(json!({
                        "name":rust_lib.name(),
                        "version":rust_lib.docs().version,
                        "written_paths":files.iter().map(|file| file.path.as_str()).collect::<Vec<_>>(),
                    }))
                })
            }
            "CheckRustLib" => {
                let arguments: NameArguments = parse_arguments(arguments)?;
                validate_library_name(&arguments.name)?;
                self.with_open_library(session_id, &arguments.name, |rust_lib| {
                    let result = rust_lib.check().map_err(map_library_error)?;
                    Ok(check_result(rust_lib.name(), &result))
                })
            }
            "PublishRustLib" => {
                let arguments: NameArguments = parse_arguments(arguments)?;
                validate_library_name(&arguments.name)?;
                self.with_open_library(session_id, &arguments.name, |rust_lib| {
                    let version = rust_lib.docs().version.clone();
                    rust_lib.publish().map_err(map_library_error)?;
                    Ok(json!({"name":rust_lib.name(),"version":version,"published":true}))
                })
            }
            _ => Err(ToolError::invalid(format!(
                "Tool {tool_name} is not a Rust library tool."
            ))),
        }
    }

    fn create_or_open(
        &self,
        session_id: &str,
        name: &str,
        create: bool,
    ) -> Result<Value, ToolError> {
        let mut registry = self.inner.registry.entries.lock().map_err(|_| {
            ToolError::internal("The Rust library ownership registry is unavailable.")
        })?;
        if let Some(entry) = registry.libraries.get(name) {
            if entry.session_id != session_id {
                return Err(library_owned_error());
            }
            if create {
                return Err(ToolError::conflict(
                    "rust_lib_already_open",
                    format!("Rust library {name:?} is already open in this Kennedy session."),
                ));
            }
            drop(registry);
            return self
                .with_open_library(session_id, name, |rust_lib| Ok(library_snapshot(rust_lib)));
        }

        let rust_lib = if create {
            self.inner.rust_libs.create_rust_lib(name)
        } else {
            self.inner.rust_libs.open_rust_lib(name)
        }
        .map_err(map_library_error)?;
        let snapshot = library_snapshot(&rust_lib);
        registry.libraries.insert(
            name.to_owned(),
            OwnedLibrary {
                session_id: session_id.to_owned(),
                handle: Arc::new(Mutex::new(rust_lib)),
                active_operations: 0,
                closing: false,
                last_used: Instant::now(),
            },
        );
        Ok(snapshot)
    }

    fn with_open_library(
        &self,
        session_id: &str,
        name: &str,
        operation: impl FnOnce(&mut OpenedRustLib) -> Result<Value, ToolError>,
    ) -> Result<Value, ToolError> {
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
        let mut registry = self.inner.registry.entries.lock().map_err(|_| {
            ToolError::internal("The Rust library ownership registry is unavailable.")
        })?;
        let Some(entry) = registry.libraries.get_mut(name) else {
            return Err(ToolError::conflict(
                "rust_lib_not_open",
                format!(
                    "Rust library {name:?} is not open in this Kennedy session. Call OpenRustLib first."
                ),
            ));
        };
        if entry.session_id != session_id {
            return Err(library_owned_error());
        }
        if entry.closing {
            return Err(ToolError::conflict(
                "rust_lib_session_ending",
                "The Kennedy session is releasing its Rust libraries.",
            ));
        }
        entry.active_operations += 1;
        entry.last_used = Instant::now();
        let handle = Arc::clone(&entry.handle);
        Ok((
            handle,
            ActiveOperation {
                registry: Arc::clone(&self.inner.registry),
                library_name: name.to_owned(),
                session_id: session_id.to_owned(),
            },
        ))
    }

    async fn release(&self, session_id: String) -> Result<usize, ToolError> {
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
            ToolError::internal("The Rust library ownership registry is unavailable.")
        })?;
        let names = registry
            .libraries
            .iter()
            .filter(|(_, entry)| entry.session_id == session_id)
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        for name in &names {
            if let Some(entry) = registry.libraries.get_mut(name) {
                entry.closing = true;
            }
        }
        while names.iter().any(|name| {
            registry
                .libraries
                .get(name)
                .is_some_and(|entry| entry.active_operations > 0)
        }) {
            registry = self.inner.registry.changed.wait(registry).map_err(|_| {
                ToolError::internal("The Rust library ownership registry is unavailable.")
            })?;
        }
        for name in &names {
            registry.libraries.remove(name);
        }
        Ok(names.len())
    }

    fn remove_expired_handles(&self) -> Result<(), ToolError> {
        let mut registry = self.inner.registry.entries.lock().map_err(|_| {
            ToolError::internal("The Rust library ownership registry is unavailable.")
        })?;
        let now = Instant::now();
        registry.libraries.retain(|_, entry| {
            entry.active_operations > 0 || now.duration_since(entry.last_used) < SESSION_LEASE
        });
        Ok(())
    }
}

pub(crate) fn router(service: RustLibToolService) -> Router {
    Router::new()
        .route("/api/v1/rust-libs/execute", post(execute))
        .route("/api/v1/rust-libs/release", post(release))
        .with_state(service)
}

async fn execute(
    State(service): State<RustLibToolService>,
    Json(request): Json<ExecuteRequest>,
) -> Result<Json<Value>, ToolError> {
    let result = service
        .execute(request.session_id, request.name, request.arguments)
        .await?;
    Ok(Json(json!({"result":result})))
}

async fn release(
    State(service): State<RustLibToolService>,
    Json(request): Json<ReleaseRequest>,
) -> Result<Json<Value>, ToolError> {
    let released = service.release(request.session_id).await?;
    Ok(Json(json!({"released":released})))
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

fn library_owned_error() -> ToolError {
    ToolError::conflict(
        "rust_lib_owned",
        "That Rust library is already open in another active Kennedy session.",
    )
}

fn map_library_error(error: RustLibError) -> ToolError {
    match error {
        RustLibError::InvalidRustLibName(_)
        | RustLibError::InvalidRustLibPath { .. }
        | RustLibError::DuplicateWritePath(_)
        | RustLibError::MissingRequiredFile(_)
        | RustLibError::InvalidVersion(_)
        | RustLibError::InvalidCargoManifest(_)
        | RustLibError::VersionMismatch { .. } => ToolError::invalid(error.to_string()),
        RustLibError::RustLibAlreadyExists(_) => {
            ToolError::conflict("rust_lib_exists", error.to_string())
        }
        RustLibError::RustLibNotFound(_) => ToolError {
            status: StatusCode::NOT_FOUND,
            code: "rust_lib_not_found",
            message: error.to_string(),
        },
        RustLibError::RustLibIsNotDirectory(_)
        | RustLibError::NonUtf8Path(_)
        | RustLibError::NonUtf8File(_)
        | RustLibError::SymlinkNotAllowed(_)
        | RustLibError::UnsupportedFileType(_) => ToolError::invalid(
            "The managed Rust library contains an unsupported path, file, symlink, or entry type.",
        ),
        RustLibError::MissingRegistryToken(_) | RustLibError::InvalidRegistryToken(_) => {
            ToolError::unavailable(
                "registry_token_unavailable",
                "Rust library publication is unavailable because the operator-provisioned crates.io token is missing or invalid.",
            )
        }
        RustLibError::CheckFailed(result) => {
            let stage = result
                .failure()
                .map(|stage| stage.stage.as_str())
                .unwrap_or("validation");
            ToolError {
                status: StatusCode::UNPROCESSABLE_ENTITY,
                code: "rust_lib_check_failed",
                message: format!(
                    "Rust library publication stopped because its {stage} check failed. Run CheckRustLib for complete diagnostics."
                ),
            }
        }
        RustLibError::RootsOverlap { .. }
        | RustLibError::Io { .. }
        | RustLibError::Sandbox { .. }
        | RustLibError::Publish(_) => {
            tracing::error!(error=%error, "Rust library operation failed");
            ToolError::unavailable(
                "rust_lib_infrastructure_failure",
                "The Rust library operation failed in local validation or publication infrastructure.",
            )
        }
    }
}

fn library_snapshot(rust_lib: &OpenedRustLib) -> Value {
    json!({
        "name":rust_lib.name(),
        "version":rust_lib.docs().version,
        "documentation":rust_lib.docs().documentation,
        "files":rust_lib.files().iter().map(|file| json!({
            "path":file.path.as_str(),
            "contents":file.contents,
        })).collect::<Vec<_>>(),
    })
}

fn check_result(name: &str, result: &CheckResult) -> Value {
    json!({
        "name":name,
        "passed":result.passed(),
        "stages":result.stages.iter().map(|stage| json!({
            "stage":stage.stage.as_str(),
            "success":stage.success,
            "exit_code":stage.exit_code,
            "stdout":stage.stdout,
            "stderr":stage.stderr,
        })).collect::<Vec<_>>(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn temporary_root() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("kennedy-rust-lib-tools-test-{}", Uuid::new_v4()))
    }

    #[tokio::test]
    async fn one_session_owns_a_library_until_release() {
        let root = temporary_root();
        let service = RustLibToolService::new(&root).unwrap();
        let created = service
            .execute(
                "conversation:first".into(),
                "CreateRustLib".into(),
                json!({"name":"example-lib"}),
            )
            .await
            .unwrap();
        assert_eq!(created["name"], "example-lib");
        assert_eq!(created["version"], "0.1.0");
        assert_eq!(created["files"].as_array().unwrap().len(), 4);

        let conflict = service
            .execute(
                "conversation:second".into(),
                "OpenRustLib".into(),
                json!({"name":"example-lib"}),
            )
            .await
            .unwrap_err();
        assert_eq!(conflict.code, "rust_lib_owned");

        assert_eq!(
            service.release("conversation:first".into()).await.unwrap(),
            1
        );
        let opened = service
            .execute(
                "conversation:second".into(),
                "OpenRustLib".into(),
                json!({"name":"example-lib"}),
            )
            .await
            .unwrap();
        assert_eq!(opened["name"], "example-lib");
        service.release("conversation:second".into()).await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn write_replaces_complete_files_and_reports_canonical_version() {
        let root = temporary_root();
        let service = RustLibToolService::new(&root).unwrap();
        service
            .execute(
                "conversation:test".into(),
                "CreateRustLib".into(),
                json!({"name":"write-test"}),
            )
            .await
            .unwrap();
        let written = service
            .execute(
                "conversation:test".into(),
                "WriteRustLib".into(),
                json!({
                    "name":"write-test",
                    "files":[
                        {"path":"Version.txt","contents":"0.2.0\n"},
                        {"path":"src/lib.rs","contents":"pub fn answer() -> u8 { 42 }\n"}
                    ]
                }),
            )
            .await
            .unwrap();
        assert_eq!(written["version"], "0.2.0");
        assert_eq!(
            written["written_paths"],
            json!(["Version.txt", "src/lib.rs"])
        );

        let opened = service
            .execute(
                "conversation:test".into(),
                "OpenRustLib".into(),
                json!({"name":"write-test"}),
            )
            .await
            .unwrap();
        assert!(opened["files"].as_array().unwrap().iter().any(|file| {
            file["path"] == "src/lib.rs" && file["contents"].as_str().unwrap().contains("answer")
        }));
        service.release("conversation:test".into()).await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}
