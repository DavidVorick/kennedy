use std::{
    fs,
    path::{Path as FilePath, PathBuf},
};

use axum::{
    Router,
    body::Body,
    extract::{Path, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use semver::{Version, VersionReq};
use serde::Deserialize;

const IMMUTABLE_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";
const FLOATING_CACHE_CONTROL: &str = "no-store, max-age=0";

#[derive(Clone)]
struct PublishedLibraries {
    root: PathBuf,
}

#[derive(Debug)]
struct RouteError {
    status: StatusCode,
    message: String,
}

#[derive(Debug)]
struct Resolution {
    version: Version,
    exact_requested: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishedManifest {
    name: String,
    version: String,
    entry: String,
    tests: String,
}

enum PreparedResponse {
    Redirect {
        location: String,
        immutable: bool,
    },
    File {
        bytes: Vec<u8>,
        content_type: &'static str,
    },
}

pub(crate) fn router(root: impl AsRef<FilePath>) -> Router {
    Router::new()
        .route("/lib/{name}/{selector}", get(lib_entry))
        .route("/lib/{name}/{selector}/{*file}", get(lib_file))
        // kcode-web-libs checks exact cross-library dependencies under
        // /module. Keep that convention available in production while /lib
        // remains Kennedy's public floating-version API.
        .route("/module/{name}/{selector}", get(module_entry))
        .route("/module/{name}/{selector}/{*file}", get(module_file))
        .with_state(PublishedLibraries {
            root: root.as_ref().to_path_buf(),
        })
}

async fn lib_entry(
    State(state): State<PublishedLibraries>,
    Path((name, selector)): Path<(String, String)>,
) -> Response {
    entry_response(state, "lib", name, selector).await
}

async fn module_entry(
    State(state): State<PublishedLibraries>,
    Path((name, selector)): Path<(String, String)>,
) -> Response {
    entry_response(state, "module", name, selector).await
}

async fn lib_file(
    State(state): State<PublishedLibraries>,
    Path((name, selector, file)): Path<(String, String, String)>,
) -> Response {
    file_response(state, "lib", name, selector, file).await
}

async fn module_file(
    State(state): State<PublishedLibraries>,
    Path((name, selector, file)): Path<(String, String, String)>,
) -> Response {
    file_response(state, "module", name, selector, file).await
}

async fn entry_response(
    state: PublishedLibraries,
    namespace: &'static str,
    name: String,
    selector: String,
) -> Response {
    blocking_response(move || prepare_entry(&state.root, namespace, &name, &selector)).await
}

async fn file_response(
    state: PublishedLibraries,
    namespace: &'static str,
    name: String,
    selector: String,
    file: String,
) -> Response {
    blocking_response(move || prepare_file(&state.root, namespace, &name, &selector, &file)).await
}

async fn blocking_response(
    operation: impl FnOnce() -> Result<PreparedResponse, RouteError> + Send + 'static,
) -> Response {
    match tokio::task::spawn_blocking(operation).await {
        Ok(Ok(prepared)) => prepared.into_response(),
        Ok(Err(error)) => error.into_response(),
        Err(error) => {
            tracing::error!(error=%error, "published Web library route worker failed");
            RouteError::internal("The published Web library service is unavailable.")
                .into_response()
        }
    }
}

fn prepare_entry(
    root: &FilePath,
    namespace: &str,
    name: &str,
    selector: &str,
) -> Result<PreparedResponse, RouteError> {
    validate_module_name(name)?;
    let resolution = resolve_version(root, name, selector)?;
    let version_root = published_version_root(root, name, &resolution.version)?;
    let bytes = read_regular_descendant(&version_root, "kcode-web.json")?;
    let manifest: PublishedManifest = serde_json::from_slice(&bytes)
        .map_err(|_| RouteError::internal("The published Web library manifest is invalid."))?;
    if manifest.name != name
        || manifest.version != resolution.version.to_string()
        || manifest.tests.is_empty()
    {
        return Err(RouteError::internal(
            "The published Web library manifest does not match its route.",
        ));
    }
    validate_file_path(&manifest.entry)?;
    Ok(PreparedResponse::Redirect {
        location: format!(
            "/{namespace}/{name}/v{}/{}",
            resolution.version,
            encode_url_path(&manifest.entry)
        ),
        immutable: resolution.exact_requested,
    })
}

fn prepare_file(
    root: &FilePath,
    namespace: &str,
    name: &str,
    selector: &str,
    file: &str,
) -> Result<PreparedResponse, RouteError> {
    validate_module_name(name)?;
    validate_file_path(file)?;
    let resolution = resolve_version(root, name, selector)?;
    if !resolution.exact_requested {
        return Ok(PreparedResponse::Redirect {
            location: format!(
                "/{namespace}/{name}/v{}/{}",
                resolution.version,
                encode_url_path(file)
            ),
            immutable: false,
        });
    }
    let version_root = published_version_root(root, name, &resolution.version)?;
    let bytes = read_regular_descendant(&version_root, file)?;
    Ok(PreparedResponse::File {
        bytes,
        content_type: content_type(file),
    })
}

fn resolve_version(root: &FilePath, name: &str, selector: &str) -> Result<Resolution, RouteError> {
    validate_module_name(name)?;
    if selector.is_empty() || selector.len() > 512 || selector.contains(['/', '\0']) {
        return Err(RouteError::bad_request(
            "Invalid Web library version selector.",
        ));
    }

    let exact = selector
        .strip_prefix('v')
        .and_then(|value| Version::parse(value).ok())
        .filter(|version| {
            version.to_string() == selector.trim_start_matches('v')
                && version.pre.is_empty()
                && version.build.is_empty()
        });
    let requirement = if exact.is_none() {
        let value = selector.strip_prefix('v').unwrap_or(selector);
        Some(VersionReq::parse(value).map_err(|_| {
            RouteError::bad_request("Invalid Cargo-compatible Web library version requirement.")
        })?)
    } else {
        None
    };

    let module_root = published_module_root(root, name)?;
    let entries = fs::read_dir(&module_root)
        .map_err(|_| RouteError::internal("The published Web library cannot be listed."))?;
    let mut selected = None;
    for entry in entries {
        let entry = entry
            .map_err(|_| RouteError::internal("The published Web library cannot be listed."))?;
        let metadata = fs::symlink_metadata(entry.path())
            .map_err(|_| RouteError::internal("The published Web library cannot be inspected."))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(version_text) = name.strip_prefix('v') else {
            continue;
        };
        let Ok(version) = Version::parse(version_text) else {
            continue;
        };
        if version.to_string() != version_text
            || !version.pre.is_empty()
            || !version.build.is_empty()
        {
            continue;
        }
        let matches = exact.as_ref().is_some_and(|exact| exact == &version)
            || requirement
                .as_ref()
                .is_some_and(|requirement| requirement.matches(&version));
        if matches && selected.as_ref().is_none_or(|current| &version > current) {
            selected = Some(version);
        }
    }

    selected
        .map(|version| Resolution {
            version,
            exact_requested: exact.is_some(),
        })
        .ok_or_else(|| RouteError::not_found("No published Web library matches that version."))
}

fn published_module_root(root: &FilePath, name: &str) -> Result<PathBuf, RouteError> {
    let published = real_directory(root, "publication root")?;
    let modules = real_directory(&published.join("module"), "module publication root")?;
    real_directory(&modules.join(name), "published Web library")
}

fn published_version_root(
    root: &FilePath,
    name: &str,
    version: &Version,
) -> Result<PathBuf, RouteError> {
    let module = published_module_root(root, name)?;
    real_directory(
        &module.join(format!("v{version}")),
        "published Web library version",
    )
}

fn real_directory(path: &FilePath, label: &str) -> Result<PathBuf, RouteError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
            Ok(path.to_path_buf())
        }
        Ok(_) => Err(RouteError::internal(format!(
            "The {label} is not a real directory."
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Err(RouteError::not_found(
            "The published Web library was not found.",
        )),
        Err(_) => Err(RouteError::internal(format!(
            "The {label} cannot be inspected."
        ))),
    }
}

fn read_regular_descendant(root: &FilePath, relative: &str) -> Result<Vec<u8>, RouteError> {
    validate_file_path(relative)?;
    let mut current = root.to_path_buf();
    let components = relative.split('/').collect::<Vec<_>>();
    for (index, component) in components.iter().enumerate() {
        current.push(component);
        let metadata = fs::symlink_metadata(&current).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                RouteError::not_found("The published Web library file was not found.")
            } else {
                RouteError::internal("The published Web library file cannot be inspected.")
            }
        })?;
        if metadata.file_type().is_symlink() {
            return Err(RouteError::internal(
                "The published Web library contains an unsafe path.",
            ));
        }
        let final_component = index + 1 == components.len();
        if (!final_component && !metadata.is_dir()) || (final_component && !metadata.is_file()) {
            return Err(RouteError::not_found(
                "The published Web library file was not found.",
            ));
        }
    }
    fs::read(current)
        .map_err(|_| RouteError::internal("The published Web library file cannot be read."))
}

fn validate_module_name(name: &str) -> Result<(), RouteError> {
    let bytes = name.as_bytes();
    if bytes.is_empty()
        || bytes.len() > 255
        || !bytes[0].is_ascii_alphanumeric()
        || !bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(RouteError::bad_request("Invalid Web library name."));
    }
    Ok(())
}

fn validate_file_path(path: &str) -> Result<(), RouteError> {
    if path.is_empty()
        || path.len() > 4_096
        || path.starts_with('/')
        || path.ends_with('/')
        || path.contains(['\\', ':', '\0'])
        || path
            .split('/')
            .any(|component| component.is_empty() || component == "." || component == "..")
    {
        return Err(RouteError::bad_request(
            "Invalid published Web library file path.",
        ));
    }
    Ok(())
}

fn encode_url_path(path: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(path.len());
    for byte in path.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'/') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[(byte >> 4) as usize]));
            encoded.push(char::from(HEX[(byte & 0x0f) as usize]));
        }
    }
    encoded
}

fn content_type(path: &str) -> &'static str {
    match path.rsplit_once('.').map(|(_, extension)| extension) {
        Some("js" | "mjs") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("html" | "htm") => "text/html; charset=utf-8",
        Some("json") => "application/json; charset=utf-8",
        Some("md" | "txt") => "text/plain; charset=utf-8",
        Some("svg") => "image/svg+xml",
        _ => "application/octet-stream",
    }
}

impl PreparedResponse {
    fn into_response(self) -> Response {
        match self {
            Self::Redirect {
                location,
                immutable,
            } => Response::builder()
                .status(StatusCode::TEMPORARY_REDIRECT)
                .header(header::LOCATION, location)
                .header(
                    header::CACHE_CONTROL,
                    if immutable {
                        IMMUTABLE_CACHE_CONTROL
                    } else {
                        FLOATING_CACHE_CONTROL
                    },
                )
                .body(Body::empty())
                .expect("fixed redirect response is valid"),
            Self::File {
                bytes,
                content_type,
            } => Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, content_type)
                .header(header::CACHE_CONTROL, IMMUTABLE_CACHE_CONTROL)
                .header("x-content-type-options", "nosniff")
                .body(Body::from(bytes))
                .expect("fixed file response is valid"),
        }
    }
}

impl RouteError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }
}

impl IntoResponse for RouteError {
    fn into_response(self) -> Response {
        let mut response = (self.status, self.message).into_response();
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static(FLOATING_CACHE_CONTROL),
        );
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn published_root() -> PathBuf {
        let root =
            std::env::temp_dir().join(format!("kennedy-web-lib-http-test-{}", Uuid::new_v4()));
        for version in ["1.2.3", "1.8.0", "2.0.0"] {
            let version_root = root.join(format!("module/demo/v{version}"));
            fs::create_dir_all(&version_root).unwrap();
            fs::write(
                version_root.join("kcode-web.json"),
                format!(
                    r#"{{"name":"demo","version":"{version}","entry":"src/index.js","tests":"tests.js"}}"#
                ),
            )
            .unwrap();
            fs::create_dir(version_root.join("src")).unwrap();
            fs::write(
                version_root.join("src/index.js"),
                format!("export const version = \"{version}\";\n"),
            )
            .unwrap();
            fs::write(
                version_root.join("tests.js"),
                "export function runTests() {}\n",
            )
            .unwrap();
            fs::write(
                version_root.join("index.html"),
                format!("<!doctype html><title>demo {version}</title>\n"),
            )
            .unwrap();
        }
        root
    }

    #[test]
    fn resolves_exact_and_cargo_compatible_floating_selectors() {
        let root = published_root();
        let _app = router(&root);
        let exact = resolve_version(&root, "demo", "v1.2.3").unwrap();
        assert_eq!(exact.version, Version::new(1, 2, 3));
        assert!(exact.exact_requested);

        let major = resolve_version(&root, "demo", "v1").unwrap();
        assert_eq!(major.version, Version::new(1, 8, 0));
        assert!(!major.exact_requested);

        let cargo_bare = resolve_version(&root, "demo", "1.2.3").unwrap();
        assert_eq!(cargo_bare.version, Version::new(1, 8, 0));
        assert!(!cargo_bare.exact_requested);

        let range = resolve_version(&root, "demo", ">=1.2,<2").unwrap();
        assert_eq!(range.version, Version::new(1, 8, 0));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn entry_and_floating_files_redirect_to_an_exact_immutable_tree() {
        let root = published_root();
        let entry = prepare_entry(&root, "lib", "demo", "v1").unwrap();
        assert!(matches!(
            entry,
            PreparedResponse::Redirect {
                location,
                immutable: false
            } if location == "/lib/demo/v1.8.0/src/index.js"
        ));
        let exact_entry = prepare_entry(&root, "lib", "demo", "v1.2.3").unwrap();
        assert!(matches!(
            exact_entry,
            PreparedResponse::Redirect {
                location,
                immutable: true
            } if location == "/lib/demo/v1.2.3/src/index.js"
        ));

        let file = prepare_file(&root, "lib", "demo", "^1", "tests.js").unwrap();
        assert!(matches!(
            file,
            PreparedResponse::Redirect {
                location,
                immutable: false
            } if location == "/lib/demo/v1.8.0/tests.js"
        ));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn exact_files_are_served_with_module_content_types() {
        let root = published_root();
        let file = prepare_file(&root, "lib", "demo", "v1.2.3", "src/index.js").unwrap();
        assert!(matches!(
            file,
            PreparedResponse::File {
                bytes,
                content_type: "text/javascript; charset=utf-8"
            } if std::str::from_utf8(&bytes).unwrap().contains("1.2.3")
        ));
        let page = prepare_file(&root, "lib", "demo", "v1.2.3", "index.html").unwrap();
        assert!(matches!(
            page,
            PreparedResponse::File {
                bytes,
                content_type: "text/html; charset=utf-8"
            } if std::str::from_utf8(&bytes).unwrap().contains("demo 1.2.3")
        ));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn axum_routes_redirect_floating_entries_and_serve_exact_modules() {
        let root = published_root();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_root = root.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, router(server_root)).await.unwrap();
        });
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();

        let floating = client
            .get(format!("http://{address}/lib/demo/v1"))
            .send()
            .await
            .unwrap();
        assert_eq!(floating.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            floating.headers()[header::LOCATION],
            "/lib/demo/v1.8.0/src/index.js"
        );
        assert_eq!(
            floating.headers()[header::CACHE_CONTROL],
            FLOATING_CACHE_CONTROL
        );

        let exact = client
            .get(format!("http://{address}/lib/demo/v1.2.3/src/index.js"))
            .send()
            .await
            .unwrap();
        assert_eq!(exact.status(), StatusCode::OK);
        assert_eq!(
            exact.headers()[header::CONTENT_TYPE],
            "text/javascript; charset=utf-8"
        );
        assert_eq!(
            exact.headers()[header::CACHE_CONTROL],
            IMMUTABLE_CACHE_CONTROL
        );
        assert!(exact.text().await.unwrap().contains("1.2.3"));

        let floating_page = client
            .get(format!("http://{address}/lib/demo/v1/index.html"))
            .send()
            .await
            .unwrap();
        assert_eq!(floating_page.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            floating_page.headers()[header::LOCATION],
            "/lib/demo/v1.8.0/index.html"
        );
        assert_eq!(
            floating_page.headers()[header::CACHE_CONTROL],
            FLOATING_CACHE_CONTROL
        );

        server.abort();
        let _ = server.await;
        std::fs::remove_dir_all(root).unwrap();
    }
}
