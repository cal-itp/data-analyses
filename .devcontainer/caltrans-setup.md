# Devcontainer Setup (Caltrans Network)
  
This devcontainer gives you a consistent Nix-based dev environment for this repo,
and includes workarounds needed to run on the Caltrans (CT) network, which does
SSL inspection (MITM) on outbound traffic.

## Prerequisites

For Caltrans users, follow [instructions](https://gist.github.com/mrtopsyt/1325d021a8fd6cdd9e7239dc7b7df348) for an IT-assisted install of WSL and Rancher Desktop.
Then, follow the "WSL Instructions"

* WSL running Ubuntu (recommended)
  * Windows Subsystem for Linux (WSL) is a tool that allows you to run Linux (an operating system) on your Windows laptop
  * Ubuntu is one of the most popular distributions ("flavor") of Linux. We assume you already have Ubuntu installed for this guide, but it isn't strictly required for the container.
* Rancher Desktop
* [VS Code](https://code.visualstudio.com/) with the
  [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) installed.

## Setup Instructions

### 0. Clone the Repository in WSL

1. Open WSL by going to the Start Menu and run "Ubuntu".
2. Clone the repo with

```bash
git clone git@github.com:cal-itp/data-analyses.git
```

3. If you get an authentication error, you may need to authenticate to Git first. Run the below commands one-by-one and follow the prompts. You will be prompted for the password you provided when you first set up WSL. Then try to clone the repo again.

```bash
sudo apt-get install gh
gh auth login
```

### 1. Configure WSL to communicate through the Caltrans network

To communicate on the Caltrans Network, WSL's "Networking Mode" setting must be set to "Mirrored".

1. Open WSL Settings from the Start menu
2. Select "Networking" from the left panel
3. Set "Networking Mode" to "Mirrored". It should be the first option on the page.

![A screenshot showing the Networking tab of WSL Settings with "Networking Mode" set to "Mirrored"](<img/Screenshot 2026-11-05 192709.png>)

### 2. Start and Configure Rancher Desktop

Open Rancher Desktop, and wait for it to initialize (the blue loading bar in the bottom right corner should be gone).

On WSL, you will need to make the software available to your WSL distro. In Rancher Desktop, open settings (ctrl + ,), select WSL, then check the box next to the distribution you're using. Then click Apply, and restart Rancher Desktop if prompted to.

![A screenshot of Rancher Desktop](<img/Screenshot 2026-11-05 192755.png>)

### 2.5 Optional: Connect to the Caltrans Network

If you intend to develop on the Caltrans network, you will need to be connected before you build the devcontainer. You can do this either by connecting to the dotsecure network in a physical Caltrans facility, or by connecting to the VPN. Being on the Caltrans network is needed so the devcontainer can download the SSL certificates it needs from Onramp.

*Troubleshooting Note*: If you are not connected, or do not have access, the devcontainer will still build, but will not function correctly if you connect to the Caltrans network later. You can fix this by connecting to the network, then running **Dev Containers: Rebuild and Reopen in Container**.

### 3. Open the repo in the devcontainer

1. Make sure Rancher Desktop is opened and has finished loading.
2. Open the repo in VS Code by running the following from your Ubuntu shell

    ```bash
    code ./path/to/data-analyses
    ```

3. Once you have the folder open and with Rancher Desktop running, run **Dev Containers: Reopen in Container** from the Command Palette (`ctrl+shift+p` by default to open the Command Palette → `Dev Containers: Reopen in Container`). The first build will download the container's Linux distribution and the package manager, so it will take longer than subsequent runs.

*Troubleshooting Note*: If you do not see any "Dev Containers" options in the command palette, you may not have the Dev Containers extension installed. Open the Extensions menu from the left sidebar in VS Code, search "Dev Containers", and install the extension from Microsoft - it should have a checkmark next to it. Then repeat these steps.

*Troubleshooting Note*: If this step fails by stating that the "Docker Daemon is not available", check that you have Rancher Desktop running and that the blue status bar is gone. If you do and it continues to fail, you may need to restart your computer.

### 4. Open a Terminal, and Confirm Packages Work

1. After the container is built, open a terminal window with Terminal -> New Terminal (or ctrl + shift + \`) from the top bar.  
   * This should open a terminal window in VS Code and automatically install the rest of the software needed to develop in this repository.
   * It will also take a long time to load on the first run, and be faster the next time it is run.
2. Confirm that software installed by running `uv` and expect to see a list of potential commands

*Troubleshooting Note*: If you get an error before loading the shell or after running `uv`, you may need to run `direnv allow`, then retry.

![A screenshot of the Devcontainer running in VS Code with the shell open](<img/Screenshot 2026-11-05 193752.png>)

### 5. Authenticate to Google Cloud with `glogin`

From the VS Code terminal (open with Terminal -> New Terminal), run `glogin` to authenticate and create an application-default certificate. The certificate will last 8 hours, after that you will need to re-authenticate.

### 6. Enjoy Developing

You can now work in VS Code - see below for some common tasks you can do:

#### Running Python

You can set up the Python environment with `uv sync` from the repo root and run a Python interpreter with `uv run python`. After running `uv sync`, you can also run notebooks in the VS Code window by selecting the local `cal-itp-data-analyses` kernel.

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

If you want to set up Python development using a similar container:

1. Copy this entire folder to that repo and make sure it is set up with a uv `pyproject.toml`.  
2. Change the variable `repo_path` in `.devcontainer/flake.nix` to `/workspaces/[repo_name]`
3. Open in VS Code and follow the instructions for all users to build the container. 
4. Optionally, change the packages installed in `.devcontainer/flake.nix`. Some of them are specific to building the Cal-ITP portfolio and may be unnecessary for your project. You can define them by adding them to the list under `buildInputs = with pkgs;`

## Running after set up

After the first time you set up the container, you only need to take the following steps:

1. Open Rancher Desktop, and wait for it to finish loading
2. Open VS Code
   * If you have recently opened the container, you can open VS Code and start the devcontainer from "Open Recent"
   * Otherwise, open Ubuntu, navigate (with `cc`) to the repo folder and run `code .` to open VS Code. Then, run "Dev Containers: Open Workspace in Container" from the command palette
3. Open a new terminal.