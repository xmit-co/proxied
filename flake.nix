{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    process-compose-flake.url = "github:Platonic-Systems/process-compose-flake";
    services-flake.url = "github:juspay/services-flake";
    crane.url = "github:ipetkov/crane";
    self.lfs = true;
  };

  outputs =
    {
      flake-utils,
      nixpkgs,
      process-compose-flake,
      services-flake,
      crane,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
        craneLib = crane.mkLib pkgs;
        workspaceRoot = ./.;
        workspaceSrc = craneLib.cleanCargoSource workspaceRoot;
        commonCraneArgs = {
          src = workspaceSrc;
          cargoToml = ./Cargo.toml;
          cargoLock = ./Cargo.lock;
        };
        mkCranePackage =
          {
            directory,
            crateName ? directory,
            extraArgs ? { },
          }:
          let
            toml = pkgs.lib.importTOML (workspaceRoot + "/${directory}/Cargo.toml");
          in
          craneLib.buildPackage (
            commonCraneArgs
            // {
              pname = crateName;
              version = toml.package.version;
              cargoExtraArgs = "--package ${crateName}";
            }
            // extraArgs
          );
        processComposeLib = import process-compose-flake.lib { inherit pkgs; };
        admin = mkCranePackage { directory = "admin"; };
        ctrl = (mkCranePackage { directory = "ctrl"; }).overrideAttrs (_: {
          DL_DIR = ./dl;
        });
        dns = mkCranePackage { directory = "dns"; };
        launcher = mkCranePackage {
          directory = "launcher";
          crateName = "proxied-launcher";
        };
        proxied = mkCranePackage { directory = "proxied"; };
        proxying = mkCranePackage { directory = "proxying"; };
        stack = processComposeLib.makeProcessCompose {
          name = "proxied-stack";
          modules = [
            services-flake.processComposeModules.default
            {
              services.redis.coord.enable = true;
              settings.processes = {
                ctrl = {
                  command = "${ctrl}/bin/ctrl";
                  depends_on.coord.condition = "process_healthy";
                };
                dns = {
                  command = "${dns}/bin/dns";
                  depends_on.coord.condition = "process_healthy";
                  environment.DOMAIN = "proxied.local";
                };
                proxying = {
                  command = "${proxying}/bin/proxying";
                  depends_on.coord.condition = "process_healthy";
                  environment.PORT = "8081";
                };
              };
            }
          ];
        };
      in
      {
        devShell = pkgs.mkShell {
          packages = with pkgs; [
            cargo
            cargo-feature
            cargo-leptos
            cargo-sort
            clippy
            leptosfmt
            nil
            openssl
            pkg-config
            rustc
            rust-analyzer
            rustfmt
          ];
        };
        packages = {
          inherit
            admin
            ctrl
            dns
            launcher
            proxied
            proxying
            stack
            ;
        };
      }
    );
}
