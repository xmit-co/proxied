$version = $args[0]
cargo build --release -p proxied --target aarch64-pc-windows-msvc
cargo build --release -p proxied --target x86_64-pc-windows-msvc
cp target/x86_64-pc-windows-msvc/release/proxied.exe dl\windows-x64\proxied-$version.exe
cp target/aarch64-pc-windows-msvc/release/proxied.exe dl\windows-arm64\proxied-$version.exe
