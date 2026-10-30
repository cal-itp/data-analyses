# Devcontainer Setup (Caltrans Network)

This devcontainer gives you a consistent Nix-based dev environment for this repo,
and includes workarounds needed to run on the Caltrans (CT) network, which does
SSL inspection (MITM) on outbound traffic.

## Prerequisites

* Docker Desktop (not available to Caltrans users) or Rancher Desktop
  * For Caltrans users, follow [instructions](https://gist.github.com/mrtopsyt/1325d021a8fd6cdd9e7239dc7b7df348) for an IT-assisted install of Rancher Desktop
* [VS Code](https://code.visualstudio.com/) with the
  [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) installed.

While these instructions should work on either Windows or Linux, they are only tested from a Linux (Ubuntu on WSL2) environment. For this reason, Windows users are recommended to clone the repo to the WSL file system and open the directory in VS Code by cc-ing into the repo folder and running `code .`.

## Setup Instructions

### 1. Configure WSL to communicate through the Caltrans network (WSL2 on CT laptop only only)

To communicate on the Caltrans Network, WSL's "Networking Mode" setting must be set to "Mirrored".

1. Open WSL Settings from the Start menu
2. Select "Networking" from the left panel
3. Set "Networking Mode" to "Mirrored". It should be the first option on the page.


### 2. Start and Configure Docker Desktop or Rancher Desktop

Open Docker Desktop or Rancher Desktop, and wait for it to initialize.

On WSL, you will need to make Docker available to your WSL distro. On Rancher Desktop, open settings (ctrl + ,), select WSL, then check the box next to the distribution you're using. Then click Apply, and restart Rancher Desktop if prompted to.

### 2.5 Optional: Connect to the Caltrans Network

If you intend to develop on the Caltrans network, you will need to be connected before you build the devcontainer. You can do this either by connecting to the dotsecure network in a physical Caltrans facility, or by connecting to the VPN. A network connection is needed so the devcontainer can download the SSL certificates it needs from Onramp.n 

If you are not connected, or do not have access, the devcontainer will still build, but will not function correctly if you connect to the Caltrans network later. You can fix this by connecting to the network, then running **Dev Containers: Rebuild and Reopen in Container**.

### 3. Open the repo in the devcontainer

From VS Code, open the repo folder and run **Dev Containers: Reopen in
Container** from the Command Palette (`ctrl+shift+p` by default to open the Command Palette → `Dev Containers: Reopen in Container`). The first build will download the container's Linux distribution and install packages, so it will take longer than subsequent runs.

### 4. Authenticate to Google Cloud with `glogin`

Run `glogin` to authenticate and create an application-default certificate. The certificate will last 8 hours, after that you will need to re-run.

This runs `gcloud auth application-default login` against the login config at
[`iac/login.json`](../iac/login.json).

### 5. Enjoy Developing

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
