{
  description = "Cal-ITP Data Infrastructure Dev Environment";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          system = system;
          config.allowUnfree = true;
        };
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            # Shell integration caching tool
            nix-direnv

            # Core Python & Packaging
            uv

            # Standalone Tools
            nixfmt

            # Portfolio Dependencies
            nodejs_22
            chromium
            chromedriver

            # System / Ops Utilities
            google-cloud-sdk
            gdal
            gh
            git
            gnumake
            rsync
            curl
            # Kerberos-enabled build: Debian's /etc/ssh/ssh_config sets
            # GSSAPIAuthentication, which plain `openssh` rejects with a warning
            opensshWithKerberos
            zlib
          ];
          # Set UV to use the local cache
          UV_CACHE_DIR="$/workspaces/data-analyses/.uv-cache";
          # Node 17+ defaults to "verbatim" DNS ordering, which resolves
          # localhost to ::1 (IPv6) first in this container. That makes
          # myst's local build server (binds to the first resolved address)
          # and its own page-export fetches (which hit 127.0.0.1) land on
          # different loopback interfaces, causing ECONNREFUSED. Forcing
          # IPv4-first keeps both sides consistent.
          NODE_OPTIONS = "--dns-result-order=ipv4first";
        };
      }
    );
}
