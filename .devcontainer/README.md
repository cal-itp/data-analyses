# Devcontainer Setup (Caltrans Network)

This devcontainer gives you a consistent Nix-based dev environment for this repo,
and includes workarounds needed to run on the Caltrans (CT) network, which does
SSL inspection (MITM) on outbound traffic. Without the setup below, tools will fail with SSL/certificate errors for Caltrans users.

## Prerequisites

* Docker Desktop (not available to Caltrans users) or Rancher Desktop
  * For Caltrans users, follow [instructions](https://gist.github.com/mrtopsyt/1325d021a8fd6cdd9e7239dc7b7df348) for an IT-assisted install of Rancher Desktop
* [VS Code](https://code.visualstudio.com/) with the
  [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) installed.

## Caltrans Network Set Up

These steps are only relevant for users on the Caltrans network

### 1. Obtain Caltrans root and issuing certificates from Onramp:

* `CTROOTCA01`: _link TBD_
* `CTISSUINGCA01`: _link TBD_
* `CTISSUINGCA02`: _link TBD_


### 2. Place the certs on your host machine

TODO - set up separate that work starting from windows and from wsl
TODO - can we make this step optional for non CT users (so they don't need to creatae devcontainer_ssl if they don't want)
TODO - caltrans incorrectly makes these .crt files - do users need to rename them or is it fine?

On first boot, the devcontainer mounts `~/devcontainer_ssl` from the host into the
container at `/home/ssl_certs`.

Create the folder and copy the three `.pem` files in, named **exactly** as below:

```bash
mkdir -p ~/devcontainer_ssl
cp /path/to/CTROOTCA01.pem ~/devcontainer_ssl/
cp /path/to/CTISSUINGCA01.pem ~/devcontainer_ssl/
cp /path/to/CTISSUINGCA02.pem ~/devcontainer_ssl/
```

File names must match `CTROOTCA01.pem`, `CTISSUINGCA01.pem`, and
`CTISSUINGCA02.pem` exactly. Otherwise they will not be copied properly

## Instructions for All Users

### 1. Open the repo in the devcontainer

From VS Code, open the repo folder and run **Dev Containers: Reopen in
Container** from the Command Palette (`ctrl+shift+p` by default to open the Command Palette → `Dev Containers: Reopen in Container`). The
first build will:

1. Install the three CT certs into the container's system trust store
   (`update-ca-certificates`), and point common SSL env vars
   (`SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE`, `NIX_SSL_CERT_FILE`,
   `GRPC_DEFAULT_SSL_ROOTS_FILE_PATH`) at it.

TODO - is setting the certs necessary if we are running update-ca-certificates

2. Enable Nix flakes and set up `direnv`.
3. Drop you into a shell with the project's Nix dev shell (`uv`, `gcloud`,
   `gh`, `gdal`, `git`, `claude-code`, etc. — see
   [`flake.nix`](./flake.nix)) auto-loaded via `direnv`.

If this is your first time running the container, you may need to use a non-Caltrans network (no VPN, no wifi) so the operating system the container runs on can download without the SSL certs being present. Once the first build runs, this won't occur again.

### 2. Authenticate to Google Cloud with `glogin`

Run `glogin` to authenticate and create an application-default certificate. The certificate will last 8 hours, after that you will need to re-run.

This runs `gcloud auth application-default login` against the login config at
[`iac/login.json`](../iac/login.json).

### 3. Enjoy Developing!

You can now work in VS Code - see below for some common tasks you can do

#### Running Python

You can set up the Python environment by running `uv sync` from the repo root and run a python interpreter with `uv run python`. After running `uv sync`, you can also run notebooks in the VS Code window by selecting the local cal-itp-data-analyses kernel.

If you have worked outside of a devcontainer on this system, you may have issues with the virtual environment - fix it by deleting the venv (`rm -rf .venv`).

#### Using git and GitHub

Both Git and the GitHub CLI are installed in the devcontainer, but you will have to authenticate to GitHub within the container. You can authenticate interactively by running `gh auth login` and following the prompts. Note that if you ever rebuild the container, you will need to repeat this process.

#### Installing Software in the Container

New shells should install with all the software in `flake.nix`, and you can add more by adding to that file. The`flake.nix` file works similarly to `pyproject.toml` by defining a list of packages to install, but instead of covering Python packages, it covers the software available in the container. You can choose from the [vast collection of packages](https://search.nixos.org/packages?channel=unstable) in the `nixpkgs` directory. You can also temporarily install software for a single shell session with `nix-shell -p <package_name>` - for example:

```bash
nix-shell -p cowsay
cowsay i like buses *and* trains
```


## Skipping the CT certs

If you're not on the Caltrans network, you can still use this devcontainer:
create an empty `~/devcontainer_ssl` folder (or empty placeholder `.pem`
files) so the mount and `onCreateCommand` don't fail, or adjust
`onCreateCommand` in [`devcontainer.json`](./devcontainer.json) locally to
skip the cert install step.

## Using in other repos

If you want to set up Python development using a similar container, copy this entire folder to that repo and make sure it is set up with a uv `pyproject.toml`. Then, open in VS Code and follow the instructions for all users to build the container. You may also want to change the packages installed in `flake.nix` - some of them are specific to running the Cal-ITP portfolio and may be unnecessary for your project.