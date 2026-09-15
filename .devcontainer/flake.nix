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
        };
      }
    );
}
