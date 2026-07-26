use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

use axum::http::StatusCode;
use kcode_web_libs::{Error as WebLibError, File as WebLibFile, Lib as OpenedWebLib};
use serde::Deserialize;
use serde_json::Value;

use crate::rust_lib_tools::{LibrarySnapshot, ToolError, ToolExecution};

pub(crate) const CREATE_WEB_LIB_TOOL: &str = "kcode-web-libs/create";
pub(crate) const OPEN_WEB_LIB_TOOL: &str = "kcode-web-libs/open";
pub(crate) const DOCS_WEB_LIB_TOOL: &str = "kcode-web-libs/docs";
pub(crate) const WRITE_WEB_LIB_TOOL: &str = "kcode-web-libs/write";
pub(crate) const WRITE_FILE_FREEFORM_WEB_LIB_TOOL: &str = "kcode-web-libs/write-file-freeform";
pub(crate) const DELETE_FILE_WEB_LIB_TOOL: &str = "kcode-web-libs/delete-file";
pub(crate) const CHECK_WEB_LIB_TOOL: &str = "kcode-web-libs/check";
pub(crate) const PUBLISH_WEB_LIB_TOOL: &str = "kcode-web-libs/publish";
pub(crate) const PREVIEW_WRITE_FILE_WEB_LIB_TOOL: &str =
    "kcode-web-libs/internal-preview-write-file";
pub(crate) const WEB_LIB_TOOLS: [&str; 8] = [
    CREATE_WEB_LIB_TOOL,
    OPEN_WEB_LIB_TOOL,
    DOCS_WEB_LIB_TOOL,
    WRITE_WEB_LIB_TOOL,
    WRITE_FILE_FREEFORM_WEB_LIB_TOOL,
    DELETE_FILE_WEB_LIB_TOOL,
    CHECK_WEB_LIB_TOOL,
    PUBLISH_WEB_LIB_TOOL,
];

const SESSION_LEASE: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_SESSION_ID_BYTES: usize = 128;

#[derive(Clone)]
pub(crate) struct WebLibToolService {
    inner: Arc<ServiceInner>,
}

struct ServiceInner {
    root: PathBuf,
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
    handle: Arc<Mutex<OpenedWebLib>>,
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

impl WebLibToolService {
    pub(crate) fn new(root: impl AsRef<Path>) -> anyhow::Result<Self> {
        let root = root.as_ref();
        std::fs::create_dir_all(root).map_err(|error| {
            anyhow::anyhow!(
                "creating managed Web libraries root {}: {error}",
                root.display()
            )
        })?;
        let metadata = std::fs::symlink_metadata(root).map_err(|error| {
            anyhow::anyhow!(
                "inspecting managed Web libraries root {}: {error}",
                root.display()
            )
        })?;
        anyhow::ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "managed Web libraries root {} must be a real directory",
            root.display()
        );
        let root = std::fs::canonicalize(root).map_err(|error| {
            anyhow::anyhow!(
                "canonicalizing managed Web libraries root {}: {error}",
                root.display()
            )
        })?;

        Ok(Self {
            inner: Arc::new(ServiceInner {
                root,
                registry: Arc::new(RegistryState {
                    entries: Mutex::new(Registry::default()),
                    changed: Condvar::new(),
                }),
            }),
        })
    }

    pub(crate) fn root(&self) -> &Path {
        &self.inner.root
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
                tracing::error!(error=%error, "Web library tool worker failed");
                internal_error("The Web library worker stopped unexpectedly.")
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
            CREATE_WEB_LIB_TOOL => {
                let arguments: NameArguments = parse_arguments(arguments)?;
                validate_library_name(&arguments.name)?;
                self.create(session_id, &arguments.name)
            }
            OPEN_WEB_LIB_TOOL => {
                let arguments: NameArguments = parse_arguments(arguments)?;
                validate_library_name(&arguments.name)?;
                self.open(session_id, &arguments.name)
            }
            DOCS_WEB_LIB_TOOL => {
                let arguments: NameArguments = parse_arguments(arguments)?;
                validate_library_name(&arguments.name)?;
                let (version, documentation) =
                    kcode_web_libs::docs(&self.inner.root, &arguments.name)
                        .map_err(|error| self.map_library_error(error))?;
                Ok(plain_execution(format!(
                    "Web library: {}\nVersion: {version}\n\n{documentation}",
                    arguments.name
                )))
            }
            WRITE_WEB_LIB_TOOL => {
                let arguments: WriteArguments = parse_arguments(arguments)?;
                validate_library_name(&arguments.name)?;
                let files = arguments
                    .files
                    .into_iter()
                    .map(|file| WebLibFile {
                        path: file.path,
                        contents: file.contents,
                    })
                    .collect::<Vec<_>>();
                let result_name = arguments.name.clone();
                self.with_open_library(session_id, &arguments.name, move |web_lib| {
                    let previous = std::mem::replace(&mut web_lib.files, files);
                    if let Err(error) = web_lib.write() {
                        web_lib.files = previous;
                        return Err(self.map_library_error(error));
                    }
                    let paths = web_lib
                        .files
                        .iter()
                        .map(|file| format!("- {}", file.path))
                        .collect::<Vec<_>>()
                        .join("\n");
                    Ok(with_snapshot_execution(
                        format!(
                            "Wrote {} files to Web library {result_name}.\n{paths}",
                            web_lib.files.len()
                        ),
                        library_snapshot(&result_name, &web_lib.files),
                    ))
                })
            }
            WRITE_FILE_FREEFORM_WEB_LIB_TOOL => {
                let arguments: SingleFileWriteArguments = parse_arguments(arguments)?;
                validate_library_name(&arguments.name)?;
                let result_name = arguments.name.clone();
                let result_path = arguments.path.clone();
                self.with_open_library(session_id, &arguments.name, move |web_lib| {
                    let previous = web_lib.files.clone();
                    upsert_file(&mut web_lib.files, arguments.path, arguments.contents);
                    if let Err(error) = web_lib.write() {
                        web_lib.files = previous;
                        return Err(self.map_library_error(error));
                    }
                    Ok(with_snapshot_execution(
                        format!("Wrote file {result_path} in Web library {result_name}."),
                        library_snapshot(&result_name, &web_lib.files),
                    ))
                })
            }
            PREVIEW_WRITE_FILE_WEB_LIB_TOOL => {
                let arguments: SingleFileWriteArguments = parse_arguments(arguments)?;
                validate_library_name(&arguments.name)?;
                let result_name = arguments.name.clone();
                self.with_open_library(session_id, &arguments.name, move |web_lib| {
                    let mut files = web_lib.files.clone();
                    upsert_file(&mut files, arguments.path, arguments.contents);
                    Ok(snapshot_execution(library_snapshot(&result_name, &files)))
                })
            }
            DELETE_FILE_WEB_LIB_TOOL => {
                let arguments: DeleteFileArguments = parse_arguments(arguments)?;
                validate_library_name(&arguments.name)?;
                let result_name = arguments.name.clone();
                let result_path = arguments.path.clone();
                self.with_open_library(session_id, &arguments.name, move |web_lib| {
                    let Some(index) = web_lib
                        .files
                        .iter()
                        .position(|file| file.path == arguments.path)
                    else {
                        return Err(not_found_error(
                            "web_lib_file_not_found",
                            format!(
                                "File {:?} does not exist in Web library {:?}.",
                                arguments.path, result_name
                            ),
                        ));
                    };
                    let previous = web_lib.files.clone();
                    web_lib.files.remove(index);
                    if let Err(error) = web_lib.write() {
                        web_lib.files = previous;
                        return Err(self.map_library_error(error));
                    }
                    Ok(with_snapshot_execution(
                        format!("Deleted file {result_path} from Web library {result_name}."),
                        library_snapshot(&result_name, &web_lib.files),
                    ))
                })
            }
            CHECK_WEB_LIB_TOOL => {
                let arguments: NameArguments = parse_arguments(arguments)?;
                validate_library_name(&arguments.name)?;
                self.with_open_library(session_id, &arguments.name, |web_lib| {
                    web_lib
                        .check()
                        .map_err(|error| self.map_library_error(error))?;
                    Ok(plain_execution(format!(
                        "Web library {} passed its checks.",
                        arguments.name
                    )))
                })
            }
            PUBLISH_WEB_LIB_TOOL => {
                let arguments: NameArguments = parse_arguments(arguments)?;
                validate_library_name(&arguments.name)?;
                self.with_open_library(session_id, &arguments.name, |web_lib| {
                    web_lib
                        .publish()
                        .map_err(|error| self.map_library_error(error))?;
                    Ok(plain_execution(format!(
                        "Published Web library {}.",
                        arguments.name
                    )))
                })
            }
            _ => Err(invalid_error(format!(
                "Tool {tool_name} is not a Web library tool."
            ))),
        }
    }

    fn create(&self, session_id: &str, name: &str) -> Result<ToolExecution, ToolError> {
        let key = HandleKey::new(session_id, name);
        let mut registry = self
            .inner
            .registry
            .entries
            .lock()
            .map_err(|_| internal_error("The Web library snapshot registry is unavailable."))?;
        if registry.libraries.contains_key(&key) {
            return Err(conflict_error(
                "web_lib_already_open",
                format!("Web library {name:?} is already open in this Kennedy session."),
            ));
        }
        let web_lib = kcode_web_libs::create(&self.inner.root, name)
            .map_err(|error| self.map_library_error(error))?;
        let snapshot = library_snapshot(name, &web_lib.files);
        registry.libraries.insert(key, OpenLibrary::new(web_lib));
        Ok(snapshot_execution(snapshot))
    }

    fn open(&self, session_id: &str, name: &str) -> Result<ToolExecution, ToolError> {
        let key = HandleKey::new(session_id, name);
        let registry = self
            .inner
            .registry
            .entries
            .lock()
            .map_err(|_| internal_error("The Web library snapshot registry is unavailable."))?;
        if registry.libraries.contains_key(&key) {
            drop(registry);
            return self.with_open_library(session_id, name, |web_lib| {
                let reopened = kcode_web_libs::open(&self.inner.root, name)
                    .map_err(|error| self.map_library_error(error))?;
                *web_lib = reopened;
                Ok(snapshot_execution(library_snapshot(name, &web_lib.files)))
            });
        }

        let mut registry = registry;
        let web_lib = kcode_web_libs::open(&self.inner.root, name)
            .map_err(|error| self.map_library_error(error))?;
        let snapshot = library_snapshot(name, &web_lib.files);
        registry.libraries.insert(key, OpenLibrary::new(web_lib));
        Ok(snapshot_execution(snapshot))
    }

    fn with_open_library(
        &self,
        session_id: &str,
        name: &str,
        operation: impl FnOnce(&mut OpenedWebLib) -> Result<ToolExecution, ToolError>,
    ) -> Result<ToolExecution, ToolError> {
        let (handle, active_operation) = self.begin_operation(session_id, name)?;
        let mut web_lib = handle
            .lock()
            .map_err(|_| internal_error("The opened Web library is unavailable."))?;
        let result = operation(&mut web_lib);
        drop(web_lib);
        drop(active_operation);
        result
    }

    fn begin_operation(
        &self,
        session_id: &str,
        name: &str,
    ) -> Result<(Arc<Mutex<OpenedWebLib>>, ActiveOperation), ToolError> {
        let key = HandleKey::new(session_id, name);
        let mut registry = self
            .inner
            .registry
            .entries
            .lock()
            .map_err(|_| internal_error("The Web library snapshot registry is unavailable."))?;
        let Some(entry) = registry.libraries.get_mut(&key) else {
            return Err(conflict_error(
                "web_lib_not_open",
                format!(
                    "Web library {name:?} is not open in this Kennedy session. Call {OPEN_WEB_LIB_TOOL} first."
                ),
            ));
        };
        if entry.closing {
            return Err(conflict_error(
                "web_lib_session_ending",
                "The Kennedy session is releasing its Web library snapshots.",
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
                tracing::error!(error=%error, "Web library session-release worker failed");
                internal_error("The Web library worker stopped unexpectedly.")
            })?
    }

    fn release_blocking(&self, session_id: &str) -> Result<usize, ToolError> {
        let mut registry = self
            .inner
            .registry
            .entries
            .lock()
            .map_err(|_| internal_error("The Web library snapshot registry is unavailable."))?;
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
            registry =
                self.inner.registry.changed.wait(registry).map_err(|_| {
                    internal_error("The Web library snapshot registry is unavailable.")
                })?;
        }
        for key in &keys {
            registry.libraries.remove(key);
        }
        Ok(keys.len())
    }

    fn remove_expired_handles(&self) -> Result<(), ToolError> {
        let mut registry = self
            .inner
            .registry
            .entries
            .lock()
            .map_err(|_| internal_error("The Web library snapshot registry is unavailable."))?;
        let now = Instant::now();
        registry.libraries.retain(|_, entry| {
            entry.active_operations > 0 || now.duration_since(entry.last_used) < SESSION_LEASE
        });
        Ok(())
    }

    fn map_library_error(&self, error: WebLibError) -> ToolError {
        let rendered = error.to_string();
        let category = rendered
            .split_once(':')
            .map_or("unknown", |(value, _)| value);
        let safe = rendered.replace(
            self.inner.root.to_string_lossy().as_ref(),
            "<managed-web-libraries>",
        );
        match category {
            "invalid_name" | "unsafe_path" | "invalid_source" => invalid_error(safe),
            "already_exists" => conflict_error("web_lib_exists", safe),
            "already_published" => conflict_error("web_lib_already_published", safe),
            "not_found" => not_found_error("web_lib_not_found", safe),
            "stale_snapshot" => conflict_error("web_lib_stale_snapshot", safe),
            "invalid_repository"
            | "unsafe_source"
            | "invalid_publication_storage"
            | "source_commit_uncertain"
            | "source_generation_commit_uncertain" => {
                unprocessable_error("web_lib_invalid_repository", safe)
            }
            value if value.starts_with("check.") => {
                unprocessable_error("web_lib_check_failed", safe)
            }
            value if value.starts_with("publish.") => {
                unavailable_error("web_lib_publish_failed", safe)
            }
            "io" => {
                tracing::error!(error=%rendered, "Web library infrastructure operation failed");
                unavailable_error(
                    "web_lib_infrastructure_failure",
                    "The Web library operation failed in local validation or publication infrastructure.",
                )
            }
            _ => {
                tracing::error!(error=%rendered, "Web library operation failed");
                unavailable_error(
                    "web_lib_infrastructure_failure",
                    "The Web library operation failed unexpectedly.",
                )
            }
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
    fn new(web_lib: OpenedWebLib) -> Self {
        Self {
            handle: Arc::new(Mutex::new(web_lib)),
            active_operations: 0,
            closing: false,
            last_used: Instant::now(),
        }
    }
}

fn parse_arguments<T: for<'de> Deserialize<'de>>(arguments: Value) -> Result<T, ToolError> {
    serde_json::from_value(arguments)
        .map_err(|error| invalid_error(format!("Invalid Web library tool arguments: {error}")))
}

fn validate_session_id(session_id: &str) -> Result<(), ToolError> {
    if session_id.is_empty()
        || session_id.len() > MAX_SESSION_ID_BYTES
        || !session_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':'))
    {
        return Err(invalid_error(
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
        return Err(invalid_error(
            "name must begin with an ASCII letter or digit and contain only ASCII letters, digits, '-' or '_'.",
        ));
    }
    Ok(())
}

fn upsert_file(files: &mut Vec<WebLibFile>, path: String, contents: String) {
    if let Some(file) = files.iter_mut().find(|file| file.path == path) {
        file.contents = contents;
    } else {
        files.push(WebLibFile { path, contents });
    }
}

pub(crate) fn proposed_write_snapshot(arguments: &Value) -> Option<LibrarySnapshot> {
    let arguments: WriteArguments = serde_json::from_value(arguments.clone()).ok()?;
    validate_library_name(&arguments.name).ok()?;
    let mut files = arguments
        .files
        .into_iter()
        .map(|file| WebLibFile {
            path: file.path,
            contents: file.contents,
        })
        .collect::<Vec<_>>();
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Some(library_snapshot(&arguments.name, &files))
}

fn library_snapshot(name: &str, files: &[WebLibFile]) -> LibrarySnapshot {
    let mut text = format!("Web library: {name}\nFiles: {}", files.len());
    for file in files {
        text.push_str("\n\nFile: ");
        text.push_str(&file.path);
        text.push('\n');
        text.push_str(&file.contents);
    }
    LibrarySnapshot {
        name: name.to_owned(),
        text,
    }
}

fn plain_execution(text: impl Into<String>) -> ToolExecution {
    ToolExecution {
        text: text.into(),
        snapshot: None,
    }
}

fn snapshot_execution(snapshot: LibrarySnapshot) -> ToolExecution {
    ToolExecution {
        text: snapshot.text.clone(),
        snapshot: Some(snapshot),
    }
}

fn with_snapshot_execution(text: impl Into<String>, snapshot: LibrarySnapshot) -> ToolExecution {
    ToolExecution {
        text: text.into(),
        snapshot: Some(snapshot),
    }
}

fn invalid_error(message: impl Into<String>) -> ToolError {
    ToolError {
        status: StatusCode::BAD_REQUEST,
        code: "invalid_arguments",
        message: message.into(),
    }
}

fn not_found_error(code: &'static str, message: impl Into<String>) -> ToolError {
    ToolError {
        status: StatusCode::NOT_FOUND,
        code,
        message: message.into(),
    }
}

fn conflict_error(code: &'static str, message: impl Into<String>) -> ToolError {
    ToolError {
        status: StatusCode::CONFLICT,
        code,
        message: message.into(),
    }
}

fn unprocessable_error(code: &'static str, message: impl Into<String>) -> ToolError {
    ToolError {
        status: StatusCode::UNPROCESSABLE_ENTITY,
        code,
        message: message.into(),
    }
}

fn unavailable_error(code: &'static str, message: impl Into<String>) -> ToolError {
    ToolError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        code,
        message: message.into(),
    }
}

fn internal_error(message: impl Into<String>) -> ToolError {
    ToolError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        code: "web_lib_internal_error",
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use uuid::Uuid;

    fn temporary_root() -> PathBuf {
        std::env::temp_dir().join(format!("kennedy-web-lib-tools-test-{}", Uuid::new_v4()))
    }

    #[tokio::test]
    async fn create_write_open_and_delete_use_complete_session_snapshots() {
        let root = temporary_root();
        let service = WebLibToolService::new(&root).unwrap();
        let created = service
            .execute_detailed(
                "conversation:test".into(),
                CREATE_WEB_LIB_TOOL.into(),
                json!({"name":"example-ui"}),
            )
            .await
            .unwrap();
        assert!(created.text.contains("File: kcode-web.json\n"));
        assert!(created.text.contains("File: index.js\n"));

        service
            .execute(
                "conversation:test".into(),
                WRITE_FILE_FREEFORM_WEB_LIB_TOOL.into(),
                json!({
                    "name":"example-ui",
                    "path":"components/button.js",
                    "contents":"export const button = true;\n",
                }),
            )
            .await
            .unwrap();
        let deleted = service
            .execute_detailed(
                "conversation:test".into(),
                DELETE_FILE_WEB_LIB_TOOL.into(),
                json!({"name":"example-ui","path":"components/button.js"}),
            )
            .await
            .unwrap();
        assert!(
            !deleted
                .snapshot
                .unwrap()
                .text
                .contains("components/button.js")
        );

        let reopened = service
            .execute(
                "conversation:test".into(),
                OPEN_WEB_LIB_TOOL.into(),
                json!({"name":"example-ui"}),
            )
            .await
            .unwrap();
        assert!(!reopened.contains("components/button.js"));
        assert_eq!(
            service.release("conversation:test".into()).await.unwrap(),
            1
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn concurrent_sessions_receive_stale_snapshot_errors() {
        let root = temporary_root();
        let service = WebLibToolService::new(&root).unwrap();
        service
            .execute(
                "conversation:first".into(),
                CREATE_WEB_LIB_TOOL.into(),
                json!({"name":"shared-ui"}),
            )
            .await
            .unwrap();
        service
            .execute(
                "conversation:second".into(),
                OPEN_WEB_LIB_TOOL.into(),
                json!({"name":"shared-ui"}),
            )
            .await
            .unwrap();
        service
            .execute(
                "conversation:first".into(),
                WRITE_FILE_FREEFORM_WEB_LIB_TOOL.into(),
                json!({
                    "name":"shared-ui",
                    "path":"first.js",
                    "contents":"export {};\n",
                }),
            )
            .await
            .unwrap();
        let stale = service
            .execute(
                "conversation:second".into(),
                WRITE_FILE_FREEFORM_WEB_LIB_TOOL.into(),
                json!({
                    "name":"shared-ui",
                    "path":"second.js",
                    "contents":"export {};\n",
                }),
            )
            .await
            .unwrap_err();
        assert_eq!(stale.code, "web_lib_stale_snapshot");
        std::fs::remove_dir_all(root).unwrap();
    }
}
