use actix_web::{App as ActixApp, HttpResponse, HttpServer, web};
use ctrl::App;
use include_dir::{Dir, include_dir};
use leptos_actix::render_app_to_stream_with_context;
use leptos_config::LeptosOptions;
use leptos_router::Method;
use shared::DynError;
use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tracing::info;
use tracing_subscriber::EnvFilter;

const DL_DIR: Dir = include_dir!("$DL_DIR");

struct LauncherArtifact {
    slug: &'static str,
    version: &'static str,
    extension: &'static str,
}

const WINDOWS_X64_VERSION: &str = "0.1";
const WINDOWS_ARM64_VERSION: &str = "0.1";
const MACOS_ARM64_VERSION: &str = "0.1";

const LAUNCHER_ARTIFACTS: &[LauncherArtifact] = &[
    LauncherArtifact {
        slug: "windows-x64",
        version: WINDOWS_X64_VERSION,
        extension: "exe",
    },
    LauncherArtifact {
        slug: "windows-arm64",
        version: WINDOWS_ARM64_VERSION,
        extension: "exe",
    },
    LauncherArtifact {
        slug: "macos-arm64",
        version: MACOS_ARM64_VERSION,
        extension: "",
    },
];

async fn dl_handler(tail: web::Path<String>) -> HttpResponse {
    if let Some(file) = DL_DIR.get_file(&*tail) {
        HttpResponse::Ok()
            .content_type(
                mime_guess::from_path(tail.as_str())
                    .first_or_octet_stream()
                    .as_ref(),
            )
            .body(file.contents())
    } else {
        HttpResponse::NotFound().finish()
    }
}

const DEFAULT_API_BASE: &str = "https://proxied.eu";

async fn launcher_manifest_handler(slug: web::Path<String>) -> HttpResponse {
    let Some(artifact) = LAUNCHER_ARTIFACTS
        .iter()
        .find(|entry| entry.slug == slug.as_str())
    else {
        return HttpResponse::NotFound().finish();
    };

    let base_url = manifest_base_url();
    let file_name = if artifact.extension.is_empty() {
        format!("proxied-{}", artifact.version)
    } else {
        format!("proxied-{}.{}", artifact.version, artifact.extension)
    };
    let body = format!("{}/dl/{}/{}\n", base_url, artifact.slug, file_name);

    HttpResponse::Ok()
        .content_type("text/plain; charset=utf-8")
        .body(body)
}

fn manifest_base_url() -> String {
    env::var("API")
        .unwrap_or_else(|_| DEFAULT_API_BASE.to_string())
        .trim_end_matches('/')
        .to_string()
}

#[actix_web::main]
async fn main() -> Result<(), DynError> {
    init_tracing();

    let addr = bind_socket_from_env();
    let leptos_options = LeptosOptions::builder().site_addr(addr).build();

    info!(bind = %addr, "ctrl starting");

    HttpServer::new(move || {
        let options = leptos_options.clone();
        let leptos_handler = render_app_to_stream_with_context(options, || {}, App, Method::Get);

        ActixApp::new()
            .route("dl/{tail:.*}", web::get().to(dl_handler))
            .route("launcher/{slug}", web::get().to(launcher_manifest_handler))
            .service(web::resource("/{tail:.*}").route(leptos_handler))
    })
    .bind(addr)?
    .run()
    .await?;

    Ok(())
}

fn bind_socket_from_env() -> SocketAddr {
    let bind_addr = env::var("ADDR")
        .ok()
        .and_then(|value| value.parse::<IpAddr>().ok())
        .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

    let bind_port = env::var("PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(8080);

    SocketAddr::from((bind_addr, bind_port))
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}
