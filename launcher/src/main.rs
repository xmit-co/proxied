use std::collections::hash_map::DefaultHasher;
use std::env;
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::thread::sleep;
use std::time::Duration;

type DynError = Box<dyn std::error::Error + Send + Sync>;

const USER_AGENT: &str = "proxied-launcher/0";

fn main() {
    if let Err(err) = run() {
        eprintln!("fatal: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), DynError> {
    let config = Config::from_env()?;
    let args: Vec<String> = env::args().skip(1).collect();

    loop {
        let download_url = match fetch_download_url(&config.manifest_url) {
            Ok(url) => url,
            Err(err) => {
                eprintln!("failed to fetch manifest: {err}");
                sleep(config.retry_delay);
                continue;
            }
        };

        let binary_path = match ensure_binary(&download_url, &config.cache_dir) {
            Ok(path) => path,
            Err(err) => {
                eprintln!("failed to prepare binary: {err}");
                sleep(config.retry_delay);
                continue;
            }
        };

        match launch_binary(&binary_path, &args) {
            Ok(status) if status.success() => return Ok(()),
            Ok(status) => {
                eprintln!("binary exited with failure ({status}) - retrying");
            }
            Err(err) => {
                eprintln!("failed to start binary: {err}");
            }
        }

        sleep(config.retry_delay);
    }
}

struct Config {
    manifest_url: String,
    cache_dir: PathBuf,
    retry_delay: Duration,
}

impl Config {
    fn from_env() -> Result<Self, DynError> {
        let manifest_url = match env::var("MANIFEST_URL") {
            Ok(value) => value,
            Err(_) => default_manifest_url()?,
        };

        let cache_dir = env::var("CACHE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| default_cache_dir());

        let retry_delay = env::var("RETRY_DELAY_SECONDS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(1));

        Ok(Self {
            manifest_url,
            cache_dir,
            retry_delay,
        })
    }
}

fn default_manifest_url() -> Result<String, DynError> {
    let api = env::var("API").unwrap_or_else(|_| "https://proxied.eu".to_string());
    let base = api.trim_end_matches('/');
    let suffix = platform_manifest_suffix()?;
    Ok(format!("{}/launcher/{}", base, suffix))
}

fn platform_manifest_suffix() -> Result<&'static str, DynError> {
    if cfg!(all(target_os = "windows", target_arch = "aarch64")) {
        Ok("windows-arm64")
    } else if cfg!(all(target_os = "windows", target_arch = "x86_64")) {
        Ok("windows-x64")
    } else if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
        Ok("macos-arm64")
    } else {
        Err("unsupported platform: expected Windows (arm64 or x64) or macOS (arm64)".into())
    }
}

fn fetch_download_url(url: &str) -> Result<String, DynError> {
    let response = http_get(url)?;
    let body = response.as_str().map_err(|err| Box::new(err) as DynError)?;

    if let Some(url) = body.lines().map(str::trim).find(|line| !line.is_empty()) {
        Ok(url.to_owned())
    } else {
        Err("server returned empty download URL".into())
    }
}

fn ensure_binary(url: &str, cache_dir: &Path) -> Result<PathBuf, DynError> {
    fs::create_dir_all(cache_dir)?;
    let file_path = cache_dir.join(cache_file_name(url));
    if !file_path.exists() {
        download_binary(url, &file_path)?;
    }
    ensure_executable(&file_path)?;
    Ok(file_path)
}

fn download_binary(url: &str, dest: &Path) -> Result<(), DynError> {
    let tmp_path = dest.with_extension("download");

    let result: Result<(), DynError> = (|| {
        let response = http_get(url)?;
        let bytes = response.into_bytes();
        let mut file = File::create(&tmp_path)?;
        file.write_all(&bytes)?;
        file.flush()?;
        drop(file);
        fs::rename(&tmp_path, dest)?;
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }

    result
}

fn http_get(url: &str) -> Result<minreq::Response, DynError> {
    let response = minreq::get(url)
        .with_header("User-Agent", USER_AGENT)
        .send()
        .map_err(|err| Box::new(err) as DynError)?;

    if !(200..300).contains(&response.status_code) {
        return Err(io::Error::other(format!(
            "request to {url} failed with status {}",
            response.status_code
        ))
        .into());
    }

    Ok(response)
}

fn ensure_executable(#[allow(unused_variables)] path: &Path) -> Result<(), DynError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let metadata = fs::metadata(path)?;
        let mut permissions = metadata.permissions();
        let mode = metadata.mode();
        if mode & 0o111 == 0 {
            permissions.set_mode(mode | 0o755);
            fs::set_permissions(path, permissions)?;
        }
    }

    Ok(())
}

fn launch_binary(path: &Path, args: &[String]) -> Result<ExitStatus, DynError> {
    let status = Command::new(path)
        .args(args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()?;
    Ok(status)
}

fn cache_file_name(url: &str) -> String {
    let mut hasher = DefaultHasher::new();
    url.hash(&mut hasher);
    let mut name = format!("{:016x}", hasher.finish());

    if let Some(ext) =
        extension_from_url(url).or_else(|| platform_default_extension().map(|s| s.to_owned()))
    {
        name.push('.');
        name.push_str(&ext);
    }

    name
}

fn extension_from_url(url: &str) -> Option<String> {
    let base = url.split('?').next()?.trim_end_matches('/');
    let segment = base.rsplit('/').next()?;
    if segment.is_empty() || !segment.contains('.') {
        return None;
    }
    let ext = segment.rsplit('.').next()?;
    if ext.is_empty() {
        None
    } else {
        Some(ext.to_owned())
    }
}

fn platform_default_extension() -> Option<&'static str> {
    #[cfg(target_os = "windows")]
    {
        Some("exe")
    }

    #[cfg(not(target_os = "windows"))]
    {
        None
    }
}

#[cfg(target_os = "windows")]
fn default_cache_dir() -> PathBuf {
    let base = env::var_os("LOCALAPPDATA")
        .or_else(|| env::var_os("APPDATA"))
        .map(PathBuf::from)
        .unwrap_or_else(env::temp_dir);
    base.join("proxied").join("launcher")
}

#[cfg(not(target_os = "windows"))]
fn default_cache_dir() -> PathBuf {
    env::temp_dir().join("proxied").join("launcher")
}
