use leptos::*;
use leptos_meta::*;
use leptos_router::*;

#[cfg(any(feature = "csr", feature = "hydrate"))]
use gloo_net::http::Request;
#[cfg(any(feature = "csr", feature = "hydrate"))]
use gloo_timers::future::sleep;
#[cfg(any(feature = "csr", feature = "hydrate"))]
use leptos::leptos_dom::helpers::window;
#[cfg(any(feature = "csr", feature = "hydrate"))]
use std::time::Duration;

#[derive(Clone, Copy)]
struct DownloadInfo {
    href: &'static str,
    label: &'static str,
}

const WINDOWS_X86_64_DOWNLOAD: DownloadInfo = DownloadInfo {
    href: "/dl/windows-x64/proxied-launcher.exe",
    label: "Windows Intel/AMD",
};

#[cfg_attr(not(any(feature = "csr", feature = "hydrate")), allow(dead_code))]
const WINDOWS_ARM64_DOWNLOAD: DownloadInfo = DownloadInfo {
    href: "/dl/windows-arm64/proxied-launcher.exe",
    label: "Windows ARM64",
};

#[cfg_attr(not(any(feature = "csr", feature = "hydrate")), allow(dead_code))]
const MACOS_ARM64_DOWNLOAD: DownloadInfo = DownloadInfo {
    href: "/dl/macos-arm64/proxied-launcher",
    label: "macOS Apple Silicon",
};

#[cfg(any(feature = "csr", feature = "hydrate"))]
fn pick_download_info(user_agent: &str, platform: &str) -> DownloadInfo {
    let ua = user_agent.to_ascii_lowercase();
    let platform = platform.to_ascii_lowercase();

    if ua.contains("mac os x") || ua.contains("macintosh") || platform.contains("mac") {
        return MACOS_ARM64_DOWNLOAD;
    }

    if ua.contains("windows") || platform.contains("win") {
        if ua.contains("arm64") || ua.contains("aarch64") || platform.contains("arm64") {
            return WINDOWS_ARM64_DOWNLOAD;
        }
        return WINDOWS_X86_64_DOWNLOAD;
    }

    WINDOWS_X86_64_DOWNLOAD
}

#[component]
pub fn App() -> impl IntoView {
    provide_meta_context();

    view! {
        <Style>
            {r#"
            :root {
            color-scheme: dark light;
            background-color: light-dark(white, black);
            font-family: system-ui, sans-serif;
            }
            body {
            max-width: 48rem;
            margin: 2em auto;
            }
            "#}
        </Style>
        <Title text="proxied" />
        <Router>
            <Routes>
                <Route path="/" view=HomePage />
            </Routes>
        </Router>
    }
}

#[component]
fn HomePage() -> impl IntoView {
    let (download_info, set_download_info) = create_signal(WINDOWS_X86_64_DOWNLOAD);
    let (server_up, set_server_up) = create_signal(false);

    #[cfg(not(any(feature = "csr", feature = "hydrate")))]
    let _ = (&set_download_info, &set_server_up);

    #[cfg(any(feature = "csr", feature = "hydrate"))]
    {
        let set_download_info = set_download_info.clone();
        on_mount(move |_| {
            if let Some(window) = window() {
                let navigator = window.navigator();
                let user_agent = navigator.user_agent().unwrap_or_default();
                let platform = navigator.platform();
                let info = pick_download_info(&user_agent, &platform);
                set_download_info.set(info);
            }
        });

        let set_server_up = set_server_up.clone();
        spawn_local(async move {
            loop {
                let is_up = Request::get("http://127.0.0.1:32123")
                    .send()
                    .await
                    .map(|response| response.ok())
                    .unwrap_or(false);
                set_server_up.set(is_up);
                sleep(Duration::from_secs(1)).await;
            }
        });
    }

    view! {
        <Show
            when=move || server_up.get()
            fallback=move || view! { <Landing download_info=download_info /> }
        >
            <AdminPage />
        </Show>
    }
}

#[component]
fn Landing(download_info: ReadSignal<DownloadInfo>) -> impl IntoView {
    view! {
        <main>
            <section>
                <h1>"proxied"</h1>
                <p>
                    "🛜 Share your Minecraft or Factorio instance on the Internet. No hosting, your own computer!"
                </p>
                <p>"🔒 Your privacy is protected: players cannot detect your IP."</p>
                <p>
                    "🚅 No complicated configuration required. Run a small program and set it up here."
                </p>
                <p>"😁 Capped free trial, then 3€/month or 30€/year."</p>
                <p>
                    "Download and run "
                    <a href=move || {
                        download_info.get().href
                    }>{move || format!("our launcher ({})", download_info.get().label)}</a> " and we'll continue here."
                </p>
            </section>
        </main>
    }
}

#[component]
fn AdminPage() -> impl IntoView {
    view! {
        <main>
            <section>
                <h1>"Admin"</h1>
                <p>"Launcher connected. Configure your session from here soon."</p>
            </section>
        </main>
    }
}
