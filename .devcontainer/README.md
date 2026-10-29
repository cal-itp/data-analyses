# Devcontainer Setup (Caltrans Network)

This devcontainer gives you a consistent Nix-based dev environment for this repo,
and includes workarounds needed to run on the Caltrans (CT) network, which does
SSL inspection (MITM) on outbound traffic. Without the setup below, tools will fail with SSL/certificate errors for Caltrans users.

## Prerequisites

* Docker Desktop (not available to Caltrans users) or Rancher Desktop
  * For Caltrans users, follow [instructions](https://gist.github.com/mrtopsyt/1325d021a8fd6cdd9e7239dc7b7df348) for an IT-assisted install of Rancher Desktop
* [VS Code](https://code.visualstudio.com/) with the
  [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) installed.

While these instructions should work on either Windows or Linux, they are only tested from a Linux (Ubuntu on WSL2) environment. For this reason, Windows users are recommended to clone the repo to the WSL file system and open the directory in VS Code by cc-ing into the repo folder and running `code .`.

## Caltrans Network Setup

This only needs to be run the first time. For non-Caltrans users, you can skip these steps, and just run the following:

```bash
mkdir -p ~/devcontainer_ssl
```

### 1. Obtain Caltrans root and issuing certificates from Onramp

Download all three certificates from [Onramp](https://itsecurity.onramp.dot.ca.gov/caltrans-proxy-certificate-information) to your local system.

### 2. Place the certs on your host machine

On first boot, the devcontainer mounts `~/devcontainer_ssl` from the host into the
container at `/home/ssl_certs`.

Create the folder and copy the three certificate files.

```bash
mkdir -p ~/devcontainer_ssl
cp /path/to/CTROOTCA01.cer ~/devcontainer_ssl/
cp /path/to/CTISSUINGCA01.cer ~/devcontainer_ssl/
cp /path/to/CTISSUINGCA02.cer ~/devcontainer_ssl/
```

If you are on a Windows system you will need to copy the files from your Windows filesystem to the WSL filesystem. Caltrans users should replace `<username>` with their S-number. (i.e. `/mnt/c/Users/s999999/Downloads/...`)

```bash
mkdir -p ~/devcontainer_ssl
cp /mnt/c/Users/<username>/Downloads/CTROOTCA01.cer ~/devcontainer_ssl/
cp /mnt/c/Users/<username>/Downloads/CTISSUINGCA01.cer ~/devcontainer_ssl/
cp /mnt/c/Users/<username>/Downloads/CTISSUINGCA02.cer ~/devcontainer_ssl/
```

File names must match either `CTROOTCA01.cer`, `CTISSUINGCA01.cer`, and
`CTISSUINGCA02.cer` or `CTROOTCA01.pem`, `CTISSUINGCA01.pem`, and
`CTISSUINGCA02.pem`  exactly. Otherwise they will not be copied properly.

## Instructions for All Users

### 1. Open the repo in the devcontainer

From VS Code, open the repo folder and run **Dev Containers: Reopen in
Container** from the Command Palette (`ctrl+shift+p` by default to open the Command Palette → `Dev Containers: Reopen in Container`). The first build will download the container's Linux distribution and install packages, so it will take longer than subsequent runs.

If this is your first time running the container, you may need to use a non-Caltrans network (no VPN, no wifi) so the operating system the container runs on can download without the SSL certs being present. Once the first build runs, this won't occur again.

### 2. Authenticate to Google Cloud with `glogin`

Run `glogin` to authenticate and create an application-default certificate. The certificate will last 8 hours, after that you will need to re-run.

This runs `gcloud auth application-default login` against the login config at
[`iac/login.json`](../iac/login.json).

### 3. Enjoy Developing

You can now work in VS Code - see below for some common tasks you can do:

#### Running Python

You can set up the Python environment with `uv sync` from the repo root and run a Python interpreter with `uv run python`. After running `uv sync`, you can also run notebooks in the VS Code window by selecting the local cal-itp-data-analyses kernel.

If you have worked outside of a devcontainer on this system, you may have issues with the virtual environment - fix it by deleting the venv (`rm -rf .venv`).

#### Using git and GitHub

Both Git and the GitHub CLI are installed in the devcontainer, but you will have to authenticate to GitHub within the container. You can authenticate interactively by running `gh auth login` and following the prompts. Note that if you ever rebuild the container, you will need to repeat this process.

#### Installing Software in the Container

The devcontainer has access to the programs defined in  `.devcontainer/flake.nix`. The `flake.nix` file works similarly to `pyproject.toml` by defining a list of packages to install, but instead of covering Python packages, it covers the software available in the container. You can choose from the [vast set of packages](https://search.nixos.org/packages?channel=unstable) in the `nixpkgs` collection.

You can also temporarily install software for a single shell session with `nix-shell -p <package_name>` - for example:

```bash
nix-shell -p cowsay
cowsay i like buses *and* trains
```

## Using in other repos

If you want to set up Python development using a similar container, copy this entire folder to that repo and make sure it is set up with a uv `pyproject.toml`. Then, open in VS Code and follow the instructions for all users to build the container. You may also want to change the packages installed in `flake.nix` - some of them are specific to running the Cal-ITP portfolio and may be unnecessary for your project.
